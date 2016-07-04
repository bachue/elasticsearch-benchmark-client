package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	elastic "gopkg.in/olivere/elastic.v3"
)

var (
	command     string
	url         string
	maxRetries  int
	indexName   string
	typeName    string
	bulk        bool
	concurrency int
	count       int
	verbose     bool
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

	var (
		duration  time.Duration
		succeeded int
		failed    int
	)

	if command == "create" && bulk {
		duration, succeeded, failed = createByBatch(client)
	} else if command == "create" {
		panic("Not Implemented")
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

func createByBatch(client *elastic.Client) (duration time.Duration, succeeded int, failed int) {
	bulk := client.Bulk().Index(indexName).Type(typeName)
	for i := 0; i < count; i++ {
		request := elastic.NewBulkIndexRequest()
		request.Id(generateUUID())
		request.Doc(generateRecord())
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
	} else {
		succeeded = count
		failed = 0
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
	fmt.Printf("Name: %s, Cluster Name: %s Version: %s\n", pingResult.Name, pingResult.ClusterName, pingResult.Version.Number)
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
