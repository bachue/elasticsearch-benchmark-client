package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	flags "github.com/jessevdk/go-flags"
	elastic "gopkg.in/olivere/elastic.v3"
)

var CommandArgs struct {
	Urls         []string `long:"url" description:"Elasticsearch URL" required:"true"`
	Index        string   `long:"index" description:"Elasticsearch Index" required:"true"`
	Type         string   `long:"type" description:"Elasticsearch Type" required:"true"`
	Command      string   `long:"command" description:"Command ('create' or 'search')" required:"true"`
	MaxRetries   int      `long:"max-retries" description:"Max Retries" default:"1"`
	Bulk         int      `long:"bulk" description:"Bulk API count" default:"0"`
	Concurrency  int      `long:"concurrency"  description:"Concurrency" default:"1"`
	Count        int      `long:"count" description:"Count" default:"1"`
	DataFilePath string   `long:"datafile" description:"Data File Path" default:"elasticrecords.txt"`
	Sniff        bool     `long:"sniff" description:"Enable Elasticsearch Sniffer"`
	Verbose      bool     `long:"verbose" description:"Show Elasticsearch API Calls"`
	Trace        bool     `long:"trace" description:"Show Elasticsearch API Calls Details"`
}

func main() {
	parseFlags()

	transport := &http.Transport{
		Proxy:               nil,
		Dial:                (&net.Dialer{Timeout: 1 * time.Minute, KeepAlive: 30 * time.Minute}).Dial,
		MaxIdleConnsPerHost: CommandArgs.Concurrency * 5,
	}
	httpClient := http.Client{Transport: transport, Timeout: time.Duration(5 * time.Minute)}

	clientOpts := make([]elastic.ClientOptionFunc, 0, 4)
	clientOpts = append(clientOpts, elastic.SetURL(CommandArgs.Urls...))
	clientOpts = append(clientOpts, elastic.SetMaxRetries(CommandArgs.MaxRetries))
	clientOpts = append(clientOpts, elastic.SetSniff(CommandArgs.Sniff))
	clientOpts = append(clientOpts, elastic.SetHttpClient(&httpClient))
	stdOutLogger := log.New(os.Stdout, "INFO: ", log.LstdFlags|log.Llongfile)
	stdErrLogger := log.New(os.Stderr, "ERROR: ", log.LstdFlags|log.Llongfile)
	if CommandArgs.Verbose {
		clientOpts = append(clientOpts, elastic.SetInfoLog(stdOutLogger))
		clientOpts = append(clientOpts, elastic.SetErrorLog(stdErrLogger))
	}
	if CommandArgs.Trace {
		clientOpts = append(clientOpts, elastic.SetTraceLog(stdOutLogger))
	}

	client, err := elastic.NewClient(clientOpts...)
	if err != nil {
		panic(err)
	}
	for _, url := range CommandArgs.Urls {
		showElasticsearchInfo(client, url)
	}
	ensureIndexExists(client)
	updateIndexMappings(client)

	var datafile *os.File
	if CommandArgs.Command == "create" {
		datafile, err = os.OpenFile(CommandArgs.DataFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	} else if CommandArgs.Command == "search" {
		datafile, err = os.OpenFile(CommandArgs.DataFilePath, os.O_RDONLY, 0)
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

	if CommandArgs.Command == "create" && CommandArgs.Bulk > 0 {
		duration, succeeded, failed = createByBatch(client, datafile)
	} else if CommandArgs.Command == "create" {
		duration, succeeded, failed = createParallel(client, datafile)
	} else if CommandArgs.Command == "search" {
		duration, succeeded, failed = searchParallel(client, datafile)
	}

	if failed > 0 {
		fmt.Printf("Error percent: %f %%\n", float64(failed)*100.0/float64(CommandArgs.Count))
	}
	if succeeded > 0 {
		fmt.Printf("Benchmark: %f q/s\n", float64(succeeded*1e9)/float64(duration))
	}
}

func createByBatch(client *elastic.Client, datafile *os.File) (duration time.Duration, succeeded int, failed int) {
	var (
		succeededIdsLists []string
		failedMessages    []string
	)
	records := make([]map[string]string, CommandArgs.Count)
	for i := 0; i < CommandArgs.Count; i++ {
		records[i] = generateRecord()
	}

	ids := make([]string, CommandArgs.Count)
	for i := 0; i < CommandArgs.Count; i++ {
		ids[i] = generateUUID()
	}

	bulkCount := 1 + (CommandArgs.Count-1)/CommandArgs.Bulk
	bulks := make([]*elastic.BulkService, bulkCount)
	for i := 0; i < bulkCount; i++ {
		bulks[i] = client.Bulk().Index(CommandArgs.Index).Type(CommandArgs.Type)
	}
	for i := 0; i < CommandArgs.Count; i++ {
		request := elastic.NewBulkIndexRequest()
		request.Id(ids[i])
		request.Doc(records[i])
		request.OpType("create")
		bulks[i%bulkCount].Add(request)
	}
	inputs, outputs, errors := prepareChannels()
	cases := prepareCases(outputs, errors)

	for i := 0; i < CommandArgs.Concurrency; i++ {
		go bulkCreateAsync(inputs, outputs[i], errors[i])
	}

	beginTime := time.Now()
	go func() {
		for _, bulk := range bulks {
			inputs <- bulk
		}
		close(inputs)
	}()
	_, succeededIdsLists, failed, failedMessages = waitForCases(cases, outputs, errors)
	endTime := time.Now()
	duration = endTime.Sub(beginTime)

	succeeded = 0
	for _, succeededIdsList := range succeededIdsLists {
		succeededIds := strings.Split(succeededIdsList, ",")
		for _, succeededId := range succeededIds {
			for i, id := range ids {
				if id == succeededId {
					succeeded += 1
					writeRecord(records[i], datafile)
					break
				}
			}
		}
	}
	for _, message := range failedMessages {
		fmt.Fprintf(os.Stderr, "Error: %s\n", message)
	}

	return
}

func createParallel(client *elastic.Client, datafile *os.File) (duration time.Duration, succeeded int, failed int) {
	var (
		succeededIds   []string
		failedMessages []string
	)
	ids := make([]string, CommandArgs.Count)
	for i := 0; i < CommandArgs.Count; i++ {
		ids[i] = generateUUID()
	}
	records := make([]map[string]string, CommandArgs.Count)
	for i := 0; i < CommandArgs.Count; i++ {
		records[i] = generateRecord()
	}

	requests := make([]*elastic.IndexService, CommandArgs.Count)
	for i := 0; i < CommandArgs.Count; i++ {
		request := client.Index()
		request.Index(CommandArgs.Index)
		request.Type(CommandArgs.Type)
		request.Id(ids[i])
		request.BodyJson(records[i])
		request.OpType("create")
		request.Timeout("60s")
		requests[i] = request
	}
	inputs, outputs, errors := prepareChannels()
	cases := prepareCases(outputs, errors)

	for i := 0; i < CommandArgs.Concurrency; i++ {
		go createAsync(inputs, outputs[i], errors[i])
	}

	beginTime := time.Now()
	go func() {
		for _, request := range requests {
			inputs <- request
		}
		close(inputs)
	}()
	succeeded, succeededIds, failed, failedMessages = waitForCases(cases, outputs, errors)
	endTime := time.Now()
	duration = endTime.Sub(beginTime)

	for _, succeededId := range succeededIds {
		for i, id := range ids {
			if id == succeededId {
				writeRecord(records[i], datafile)
				break
			}
		}
	}
	for _, message := range failedMessages {
		fmt.Fprintf(os.Stderr, "Error: %s\n", message)
	}

	ensureWritten(client)
	return
}

func searchParallel(client *elastic.Client, datafile *os.File) (duration time.Duration, succeeded int, failed int) {
	var (
		succeededHits  []string
		failedMessages []string
	)
	requests := make([]*elastic.SearchService, CommandArgs.Count)
	for i := 0; i < CommandArgs.Count; i++ {
		key, value, err := getLine(datafile)
		if err != nil {
			panic(err)
		}

		request := client.Search()
		request.Index(CommandArgs.Index)
		request.Type(CommandArgs.Type)
		request.Query(elastic.NewTermQuery(key, value))
		requests[i] = request
	}
	inputs, outputs, errors := prepareChannels()
	cases := prepareCases(outputs, errors)

	for i := 0; i < CommandArgs.Concurrency; i++ {
		go searchAsync(inputs, outputs[i], errors[i])
	}

	beginTime := time.Now()
	go func() {
		for _, request := range requests {
			inputs <- request
		}
		close(inputs)
	}()
	succeeded, succeededHits, failed, failedMessages = waitForCases(cases, outputs, errors)
	endTime := time.Now()
	duration = endTime.Sub(beginTime)

	for _, succeededHit := range succeededHits {
		hit, err := strconv.ParseInt(succeededHit, 10, 64)
		if err != nil {
			panic(err)
		}
		if hit != 1 {
			fmt.Fprintf(os.Stderr, "WARN: Expected Hit is 1, but %d\n", hit)
		}
	}

	for _, message := range failedMessages {
		fmt.Fprintf(os.Stderr, "Error: %s\n", message)
	}
	return
}

func bulkCreateAsync(inputs <-chan interface{}, outputs chan<- string, errors chan<- string) {
	defer close(outputs)
	defer close(errors)

	for {
		job, moreJob := <-inputs
		if moreJob {
			request, ok := job.(*elastic.BulkService)
			if ok {
				response, err := request.Do()
				if err != nil {
					errors <- err.Error()
				} else {
					succeededIds := make([]string, 0, response.Took)
					for _, responseItem := range response.Succeeded() {
						succeededIds = append(succeededIds, responseItem.Id)
					}
					outputs <- strings.Join(succeededIds, ",")
				}
			} else {
				panic(fmt.Sprintf("Expected Type is *elastic.BulkService, but %T\n", request))
			}
		} else {
			break
		}
	}
}

func createAsync(inputs <-chan interface{}, outputs chan<- string, errors chan<- string) {
	defer close(outputs)
	defer close(errors)

	for {
		job, moreJob := <-inputs
		if moreJob {
			request, ok := job.(*elastic.IndexService)
			if ok {
				response, err := request.Do()
				if err != nil {
					errors <- err.Error()
				} else {
					outputs <- response.Id
				}
			} else {
				panic(fmt.Sprintf("Expected Type is *elastic.IndexService, but %T\n", request))
			}
		} else {
			break
		}
	}
}

func searchAsync(inputs <-chan interface{}, outputs chan<- string, errors chan<- string) {
	defer close(outputs)
	defer close(errors)

	for {
		job, moreJob := <-inputs
		if moreJob {
			request, ok := job.(*elastic.SearchService)
			if ok {
				response, err := request.Do()
				if err != nil {
					errors <- err.Error()
				} else {
					outputs <- strconv.FormatInt(response.Hits.TotalHits, 10)
				}
			} else {
				panic(fmt.Sprintf("Expected Type is *elastic.SearchService, but %T\n", request))
			}
		} else {
			break
		}
	}
}

func prepareChannels() (inputs chan interface{}, outputs []chan string, errors []chan string) {
	inputs = make(chan interface{}, CommandArgs.Concurrency)
	outputs = make([]chan string, CommandArgs.Concurrency)
	errors = make([]chan string, CommandArgs.Concurrency)
	for i := 0; i < CommandArgs.Concurrency; i++ {
		outputs[i] = make(chan string, CommandArgs.Concurrency)
		errors[i] = make(chan string, CommandArgs.Concurrency)
	}
	return
}

func prepareCases(outputs []chan string, errors []chan string) []reflect.SelectCase {
	casesCount := CommandArgs.Concurrency + CommandArgs.Concurrency
	cases := make([]reflect.SelectCase, casesCount)
	for i := 0; i < CommandArgs.Concurrency; i++ {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(outputs[i])}
	}
	for i := 0; i < CommandArgs.Concurrency; i++ {
		cases[i+CommandArgs.Concurrency] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(errors[i])}
	}
	return cases
}

func waitForCases(cases []reflect.SelectCase, outputs []chan string, errors []chan string) (succeeded int, succeededMessages []string, failed int, failedMessages []string) {
	middle := CommandArgs.Concurrency
	succeeded = 0
	succeededMessages = make([]string, 0, CommandArgs.Count)
	failed = 0
	failedMessages = make([]string, 0)

	for len(cases) > 0 {
		chosen, value, ok := reflect.Select(cases)
		if ok {
			if chosen < middle {
				succeededMessages = append(succeededMessages, value.String())
				succeeded += 1
			} else {
				failedMessages = append(failedMessages, value.String())
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
	_, err := flags.Parse(&CommandArgs)
	if err != nil {
		os.Exit(1)
	}

	if CommandArgs.Command != "create" && CommandArgs.Command != "search" {
		fmt.Fprintf(os.Stderr, "Usage: -command must be `create` or `search`\n")
		os.Exit(1)
	}

	if CommandArgs.Concurrency <= 0 {
		fmt.Fprintf(os.Stderr, "Usage: -concurrency must be greater than 0\n")
		os.Exit(1)
	}

	if CommandArgs.Count < CommandArgs.Concurrency {
		fmt.Fprintf(os.Stderr, "Usage: -count must be greater than or equal to -concurrency\n")
		os.Exit(1)
	}
}

func showElasticsearchInfo(client *elastic.Client, url string) {
	pingResult, _, err := client.Ping(url).Do()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Name: %s, Cluster Name: %s, Version: %s\n", pingResult.Name, pingResult.ClusterName, pingResult.Version.Number)
}

func ensureWritten(client *elastic.Client) {
	_, err := client.Flush(CommandArgs.Index).WaitIfOngoing(true).Do()
	if err != nil {
		panic(err)
	}
}

func ensureIndexExists(client *elastic.Client) {
	indexExists, err := client.IndexExists(CommandArgs.Index).Do()
	if err != nil {
		panic(err)
	}

	if !indexExists {
		indexCreateResult, err := client.CreateIndex(CommandArgs.Index).Do()
		if err != nil {
			panic(err)
		}
		if !indexCreateResult.Acknowledged {
			fmt.Fprintf(os.Stderr, "createIndex() cannot be acknowledged\n")
			os.Exit(1)
		}
	}
}

func getLine(datafile *os.File) (string, string, error) {
	var (
		stat    os.FileInfo
		size    int64
		buf     []byte = make([]byte, 300)
		lineBuf []byte
		line    string
		results []string
		err     error
	)
	stat, err = datafile.Stat()
	if err != nil {
		return "", "", err
	}
	size = stat.Size()
	for {
		_, err = datafile.ReadAt(buf, rand.Int63n(size))
		if err != nil {
			return "", "", err
		}
		bytesReader := bytes.NewReader(buf)
		bufReader := bufio.NewReader(bytesReader)
		lineBuf, _, err = bufReader.ReadLine()
		if err != nil {
			continue
		}
		line = string(lineBuf[:])
		results = strings.SplitN(line, ":", 3)
		if len(results) != 3 || len(results[2]) != 128 {
			lineBuf, _, err = bufReader.ReadLine()
			if err != nil {
				continue
			}
			line = string(lineBuf[:])
			results = strings.SplitN(line, ":", 3)
			if len(results) != 3 || len(results[2]) != 128 {
				continue
			}
		}
		return "data" + results[1], results[2], err
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
	response, err := client.PutMapping().Index(CommandArgs.Index).Type(CommandArgs.Type).BodyString(mappingsJson).Do()
	if err != nil {
		panic(err)
	}
	if !response.Acknowledged {
		fmt.Fprintf(os.Stderr, "updateIndexMappings() cannot be acknowledged\n")
		os.Exit(1)
	}
}
