package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	elastic "gopkg.in/olivere/elastic.v3"
)

var (
	command      string
	url          string
	maxRetries   int
	indexName    string
	typeName     string
	bulk         bool
	concurrency  int
	count        int
	verbose      bool
	dataFilePath string
)

func main() {
	parseFlags()

	clientOpts := make([]elastic.ClientOptionFunc, 0, 4)
	clientOpts = append(clientOpts, elastic.SetURL(url))
	clientOpts = append(clientOpts, elastic.SetMaxRetries(maxRetries))
	if verbose {
		stdOutLogger := log.New(os.Stdout, "INFO: ", log.LstdFlags|log.Llongfile)
		stdErrLogger := log.New(os.Stderr, "ERROR: ", log.LstdFlags|log.Llongfile)
		clientOpts = append(clientOpts, elastic.SetInfoLog(stdOutLogger))
		clientOpts = append(clientOpts, elastic.SetErrorLog(stdErrLogger))
	}

	client, err := elastic.NewClient(clientOpts...)
	if err != nil {
		panic(err)
	}
	showElasticsearchInfo(client)
	ensureIndexExists(client)
	updateIndexMappings(client)

	var datafile *os.File
	if command == "create" {
		datafile, err = os.OpenFile(dataFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	} else if command == "search" {
		datafile, err = os.OpenFile(dataFilePath, os.O_RDONLY, 0)
	}
	if err != nil {
		panic(err)
	}
	defer datafile.Close()

	var (
		duration  time.Duration
		succeeded int
		failed    int
	)

	if command == "create" && bulk {
		duration, succeeded, failed = createByBatch(client, datafile)
	} else if command == "create" {
		duration, succeeded, failed = createParallel(client, datafile)
	} else if command == "search" {
		panic("Not Implemented")
	}

	if failed > 0 {
		fmt.Printf("Error percent: %f %%\n", float64(failed)*100.0/float64(count))
	}
	if succeeded > 0 {
		fmt.Printf("Benchmark: %f q/s\n", float64(succeeded*10e9)/float64(duration))
	}
}

func createByBatch(client *elastic.Client, datafile *os.File) (duration time.Duration, succeeded int, failed int) {
	array := make([]map[string]string, count)
	for i := 0; i < count; i++ {
		array[i] = generateRecord()
	}
	bulk := client.Bulk().Index(indexName).Type(typeName)
	for i := 0; i < count; i++ {
		request := elastic.NewBulkIndexRequest()
		request.Id(generateUUID())
		request.Doc(array[i])
		request.OpType("create")
		bulk.Add(request)
	}
	beginTime := time.Now()
	response, err := bulk.Do()
	endTime := time.Now()
	duration = endTime.Sub(beginTime)

	if err != nil {
		fmt.Fprintf(os.Stderr, "createByBatch() Error: %s\n", err.Error())
		succeeded = 0
		failed = count
	} else if response.Errors {
		succeeded = len(response.Succeeded())
		failed = len(response.Failed())
		for i, item := range response.Items {
			result := item["index"]
			if result != nil {
				if result.Status >= 200 && result.Status <= 299 {
					writeRecord(array[i], datafile)
					succeeded += 1
				} else {
					failed += 1
				}
			}
		}
		ensureWritten(client)
	} else {
		for i := 0; i < count; i++ {
			writeRecord(array[i], datafile)
		}
		ensureWritten(client)
		succeeded = count
		failed = 0
	}
	return
}

func createParallel(client *elastic.Client, datafile *os.File) (duration time.Duration, succeeded int, failed int) {
	array := make([]map[string]string, count)
	for i := 0; i < count; i++ {
		array[i] = generateRecord()
	}
	inputs, outputs, errors := prepareChannels()

	for i := 0; i < concurrency; i++ {
		go createAsync(client, inputs[i], outputs[i], errors[i], datafile)
	}
	for i := 0; i < count; i++ {
		inputs[i%concurrency] <- array[i]
	}
	for i := 0; i < concurrency; i++ {
		close(inputs[i])
	}
	duration, succeeded, failed = waitForCases(outputs, errors)
	ensureWritten(client)
	return
}

func createAsync(client *elastic.Client, inputs <-chan map[string]string, outputs chan<- time.Duration, errors chan<- string, datafile *os.File) {
	defer close(outputs)
	defer close(errors)

	for {
		record, moreJob := <-inputs
		if moreJob {
			request := client.Index()
			request.Id(generateUUID())
			request.Index(indexName)
			request.Type(typeName)
			request.BodyJson(record)
			request.OpType("create")
			request.Timeout("60s")
			beginTime := time.Now()
			_, err := request.Do()
			endTime := time.Now()
			if err != nil {
				errors <- err.Error()
			} else {
				writeRecord(record, datafile)
				outputs <- endTime.Sub(beginTime)
			}
		} else {
			break
		}
	}
}

func prepareChannels() (inputs []chan map[string]string, outputs []chan time.Duration, errors []chan string) {
	tasksPerChannel := 1 + (count-1)/concurrency

	inputs = make([]chan map[string]string, concurrency)
	outputs = make([]chan time.Duration, concurrency)
	errors = make([]chan string, concurrency)
	for i := 0; i < concurrency; i++ {
		inputs[i] = make(chan map[string]string, tasksPerChannel)
		outputs[i] = make(chan time.Duration, concurrency)
		errors[i] = make(chan string, concurrency)
	}
	return
}

func waitForCases(outputs []chan time.Duration, errors []chan string) (durationSum time.Duration, succeed int, failed int) {
	casesCount := concurrency + concurrency
	cases := make([]reflect.SelectCase, casesCount)
	for i := 0; i < concurrency; i++ {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(outputs[i])}
	}
	for i := 0; i < concurrency; i++ {
		cases[i+concurrency] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(errors[i])}
	}

	middle := concurrency
	durationSum = 0
	succeed = 0
	failed = 0

	for len(cases) > 0 {
		chosen, value, ok := reflect.Select(cases)
		if ok {
			if chosen < middle {
				durationSum += time.Duration(value.Int())
				succeed += 1
			} else {
				fmt.Fprintf(os.Stderr, "%d: %s\n", chosen-concurrency, value.String())
				failed += 1
			}
		} else {
			cases = append(cases[:chosen], cases[chosen+1:]...)
			if chosen < middle {
				middle -= 1
			}
		}
	}
	return
}

func parseFlags() {
	flag.StringVar(&url, "url", "http://localhost:9200", "Elasticsearch Url")
	flag.StringVar(&indexName, "index", "", "Elasticsearch Index")
	flag.StringVar(&typeName, "type", "", "Elasticsearch Type")
	flag.StringVar(&command, "command", "", "Command ('create' or 'search')")
	flag.BoolVar(&bulk, "bulk", false, "Use Bulk API")
	flag.IntVar(&count, "count", 1, "Count")
	flag.IntVar(&concurrency, "concurrency", 1, "Concurrency")
	flag.IntVar(&maxRetries, "max-retries", 10, "Elasticsearch Max Retries")
	flag.StringVar(&dataFilePath, "datafile", "elasticrecords.txt", "Datafile")
	flag.BoolVar(&verbose, "v", false, "Show Logs")
	flag.Parse()

	if len(flag.Args()) > 0 {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	if command != "create" && command != "query" {
		fmt.Fprintf(os.Stderr, "Usage: -command must be `create` or `search`\n")
		os.Exit(1)
	}

	if len(indexName) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: -index must be specified\n")
		os.Exit(1)
	}

	if len(typeName) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: -type must be specified\n")
		os.Exit(1)
	}

	if concurrency <= 0 {
		fmt.Fprintf(os.Stderr, "Usage: -concurrency must be greater than 0\n")
		os.Exit(1)
	}

	if count < concurrency {
		fmt.Fprintf(os.Stderr, "Usage: -count must be greater than or equal to -concurrency\n")
		os.Exit(1)
	}
}

func showElasticsearchInfo(client *elastic.Client) {
	pingResult, _, err := client.Ping(url).Do()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Name: %s, Cluster Name: %s, Version: %s\n", pingResult.Name, pingResult.ClusterName, pingResult.Version.Number)
}

func ensureWritten(client *elastic.Client) {
	_, err := client.Flush(indexName).WaitIfOngoing(true).Do()
	if err != nil {
		panic(err)
	}
}

func ensureIndexExists(client *elastic.Client) {
	indexExists, err := client.IndexExists(indexName).Do()
	if err != nil {
		panic(err)
	}

	if !indexExists {
		indexCreateResult, err := client.CreateIndex(indexName).Do()
		if err != nil {
			panic(err)
		}
		if !indexCreateResult.Acknowledged {
			fmt.Fprintf(os.Stderr, "createIndex() cannot be acknowledged\n")
			os.Exit(1)
		}
	}
}

var mappingsJson string = `
{
    "_all": {
        "enabled": false
    },
    "properties": {
        "data1": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data2": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data3": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data4": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data5": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data6": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data7": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data8": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data9": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data10": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data11": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data12": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data13": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data14": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data15": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data16": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data17": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data18": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data19": {
            "type": "string",
            "index": "not_analyzed"
        },
        "data20": {
            "type": "string",
            "index": "not_analyzed"
        }
    }
}
`

func updateIndexMappings(client *elastic.Client) {
	response, err := client.PutMapping().Index(indexName).Type(typeName).BodyString(mappingsJson).Do()
	if err != nil {
		panic(err)
	}
	if !response.Acknowledged {
		fmt.Fprintf(os.Stderr, "updateIndexMappings() cannot be acknowledged\n")
		os.Exit(1)
	}
}
