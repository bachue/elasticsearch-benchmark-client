package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	flags "github.com/jessevdk/go-flags"
	"github.com/pborman/uuid"
	"github.com/spaolacci/murmur3"
	elastic "github.com/olivere/elastic"
)

var CommandArgs struct {
	Urls        []string `long:"url" description:"Elasticsearch URL" required:"true"`
	Index       string   `long:"index" description:"Elasticsearch Index" required:"true"`
	Type        string   `long:"type" description:"Elasticsearch Type" required:"true"`
	Command     string   `long:"command" description:"Command (only 'create' is acceptable)" required:"true"`
	MaxRetries  int      `long:"max-retries" description:"Max Retries" default:"1"`
	Concurrency int32    `long:"concurrency"  description:"Go Routinue Count" default:"1"`
	Count       int32    `long:"count" description:"Count" default:"1"`
	Bulk        int32    `long:"bulkcount" description:"Bulk Size" default:"100"`
	Sniff       bool     `long:"sniff" description:"Enable Elasticsearch Sniffer"`
	Frequency   int32    `long:"frequency" description:"Print Progress Frequency" default:"100000"`
	Verbose     bool     `long:"verbose" description:"Show Elasticsearch API Calls"`
	Trace       bool     `long:"trace" description:"Show Elasticsearch API Calls Details"`
}

func parseFlags() {
	_, err := flags.Parse(&CommandArgs)
	if err != nil {
		os.Exit(1)
	}

	if CommandArgs.Command != "create" {
		fmt.Fprintf(os.Stderr, "Usage: --command must be `create`\n")
		os.Exit(1)
	}

	if CommandArgs.Concurrency <= 0 {
		fmt.Fprintf(os.Stderr, "Usage: --concurrency must be greater than 0\n")
		os.Exit(1)
	}

	if CommandArgs.Count < CommandArgs.Concurrency {
		fmt.Fprintf(os.Stderr, "Usage: --count must be greater than or equal to -concurrency\n")
		os.Exit(1)
	}

	if CommandArgs.Frequency <= 0 {
		fmt.Fprintf(os.Stderr, "Usage: --frequency must be greater than 0\n")
		os.Exit(1)
	}

	if CommandArgs.Bulk <= 0 {
		fmt.Fprintf(os.Stderr, "Usage: --bulkcount must be greater than 0\n")
		os.Exit(1)
	}

	if CommandArgs.Frequency%CommandArgs.Bulk != 0 {
		fmt.Fprintf(os.Stderr, "Usage: --frequency %% --bulkcount must be 0\n")
		os.Exit(1)
	}
}

func main() {
	parseFlags()

	transport := &http.Transport{
		Proxy:               nil,
		Dial:                (&net.Dialer{Timeout: 1 * time.Minute, KeepAlive: 30 * time.Minute}).Dial,
		MaxIdleConnsPerHost: int(CommandArgs.Concurrency * 5),
	}
	httpClient := http.Client{Transport: transport, Timeout: time.Duration(5 * time.Minute)}

	clientOpts := make([]elastic.ClientOptionFunc, 0, 8)
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

	switch CommandArgs.Command {
	case "create":
		create(client)
	}
}

var last time.Time

func create(client *elastic.Client) {
	var wg sync.WaitGroup

	wg.Add(int(CommandArgs.Concurrency))

	last = time.Now()
	for i := int32(0); i < CommandArgs.Concurrency; i++ {
		go createRoutine(client, &wg)
	}
	wg.Wait()
}

func generateMurmur3() []byte {
	var bytesArray [16]byte

	lpointer := unsafe.Pointer(&bytesArray[0])
	hpointer := unsafe.Pointer(&bytesArray[8])
	*(*int64)(lpointer) = time.Now().UnixNano()

	hasher := murmur3.New128()
	hasher.Write(bytesArray[0:8])
	r1, r2 := hasher.Sum128()

	*(*uint64)(lpointer) = r1
	*(*uint64)(hpointer) = r2

	return bytesArray[:]
}

func generateRandomHexes() [20]string {
	var bytes1 []byte = generateMurmur3()
	var bytes2 []byte = generateMurmur3()
	var bytes3 []byte = generateMurmur3()
	var bytes4 []byte = generateMurmur3()
	var hex1 string = hex.EncodeToString(bytes1)
	var hex2 string = hex.EncodeToString(bytes2)
	var hex3 string = hex.EncodeToString(bytes3)
	var hex4 string = hex.EncodeToString(bytes4)
	return [20]string{
		strings.Join([]string{hex1, hex2, hex3, hex4}, ""),
		strings.Join([]string{hex1, hex2, hex4, hex3}, ""),
		strings.Join([]string{hex1, hex3, hex2, hex4}, ""),
		strings.Join([]string{hex1, hex3, hex4, hex2}, ""),
		strings.Join([]string{hex1, hex4, hex2, hex3}, ""),
		strings.Join([]string{hex1, hex4, hex3, hex2}, ""),
		strings.Join([]string{hex2, hex1, hex3, hex4}, ""),
		strings.Join([]string{hex2, hex1, hex4, hex3}, ""),
		strings.Join([]string{hex2, hex3, hex1, hex4}, ""),
		strings.Join([]string{hex2, hex3, hex4, hex1}, ""),
		strings.Join([]string{hex2, hex4, hex3, hex2}, ""),
		strings.Join([]string{hex2, hex4, hex2, hex3}, ""),
		strings.Join([]string{hex3, hex1, hex2, hex4}, ""),
		strings.Join([]string{hex3, hex1, hex4, hex2}, ""),
		strings.Join([]string{hex3, hex2, hex1, hex4}, ""),
		strings.Join([]string{hex3, hex2, hex4, hex1}, ""),
		strings.Join([]string{hex3, hex4, hex1, hex2}, ""),
		strings.Join([]string{hex3, hex4, hex2, hex1}, ""),
		strings.Join([]string{hex4, hex1, hex2, hex3}, ""),
		strings.Join([]string{hex4, hex1, hex3, hex2}, ""),
	}
}

type Doc map[string]string

func generateRandomDoc() Doc {
	hexes := generateRandomHexes()
	return Doc{
		"data0":  hexes[0],
		"data1":  hexes[1],
		"data2":  hexes[2],
		"data3":  hexes[3],
		"data4":  hexes[4],
		"data5":  hexes[5],
		"data6":  hexes[6],
		"data7":  hexes[7],
		"data8":  hexes[8],
		"data9":  hexes[9],
		"data10": hexes[10],
		"data11": hexes[11],
		"data12": hexes[12],
		"data13": hexes[13],
		"data14": hexes[14],
		"data15": hexes[15],
		"data16": hexes[16],
		"data17": hexes[17],
		"data18": hexes[18],
		"data19": hexes[19],
	}
}

var (
	totalWrite int32 = 0
)

func createRoutine(client *elastic.Client, wg *sync.WaitGroup) {
	var (
		t     int32
		count int32
	)

	for {
		if t = atomic.AddInt32(&totalWrite, CommandArgs.Bulk); t-CommandArgs.Bulk >= CommandArgs.Count {
			break
		}

		if t > CommandArgs.Count {
			count = t - CommandArgs.Count
		} else {
			count = CommandArgs.Bulk
		}

		docs := make([]Doc, 0, count)
		bulkRequest := client.Bulk().Index(CommandArgs.Index).Type(CommandArgs.Type).Timeout("60s")
		for i := int32(0); i < count; i++ {
			doc := generateRandomDoc()
			docs = append(docs, doc)
			indexRequest := elastic.NewBulkIndexRequest()
			indexRequest.Id(uuid.New())
			indexRequest.Doc(doc)
			indexRequest.OpType("create")
			bulkRequest.Add(indexRequest)
		}
		response, err := bulkRequest.Do()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Create Bulk Error: %v\n", err)
			continue
		}

		if response.Errors {
			failedItems := response.Failed()
			fmt.Fprintf(os.Stderr, "Something wrong just happened, %d items was failed to be indexed\n", len(failedItems))
			for _, failedItem := range failedItems {
				if failedItem.Error != nil {
					fmt.Fprintf(os.Stderr, "Failed: Id = %v, Status = %d, Reason = %s\n", failedItem.Id, failedItem.Status, failedItem.Error.Reason)
				} else {
					fmt.Fprintf(os.Stderr, "Failed: Id = %v, Status = %d\n", failedItem.Id, failedItem.Status)
				}
			}
		}

		if t%CommandArgs.Frequency == 0 {
			fmt.Printf("INSERT %d %f\n", t, float64(CommandArgs.Frequency)/time.Since(last).Seconds())
			last = time.Now()
		}
	}
	ensureWritten(client)

	wg.Done()
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

type Mappings map[string]interface{}

func updateIndexMappings(client *elastic.Client) {
	var mappings Mappings = Mappings{
		"_all": Mappings{
			"enabled": false,
		},
		"properties": Mappings{
			"data0":  Mappings{"type": "string", "index": "not_analyzed"},
			"data1":  Mappings{"type": "string", "index": "not_analyzed"},
			"data2":  Mappings{"type": "string", "index": "not_analyzed"},
			"data3":  Mappings{"type": "string", "index": "not_analyzed"},
			"data4":  Mappings{"type": "string", "index": "not_analyzed"},
			"data5":  Mappings{"type": "string", "index": "not_analyzed"},
			"data6":  Mappings{"type": "string", "index": "not_analyzed"},
			"data7":  Mappings{"type": "string", "index": "not_analyzed"},
			"data8":  Mappings{"type": "string", "index": "not_analyzed"},
			"data9":  Mappings{"type": "string", "index": "not_analyzed"},
			"data10": Mappings{"type": "string", "index": "not_analyzed"},
			"data11": Mappings{"type": "string", "index": "not_analyzed"},
			"data12": Mappings{"type": "string", "index": "not_analyzed"},
			"data13": Mappings{"type": "string", "index": "not_analyzed"},
			"data14": Mappings{"type": "string", "index": "not_analyzed"},
			"data15": Mappings{"type": "string", "index": "not_analyzed"},
			"data16": Mappings{"type": "string", "index": "not_analyzed"},
			"data17": Mappings{"type": "string", "index": "not_analyzed"},
			"data18": Mappings{"type": "string", "index": "not_analyzed"},
			"data19": Mappings{"type": "string", "index": "not_analyzed"},
		},
	}
	response, err := client.PutMapping().Index(CommandArgs.Index).Type(CommandArgs.Type).BodyJson(mappings).Do()
	if err != nil {
		panic(err)
	}
	if !response.Acknowledged {
		fmt.Fprintf(os.Stderr, "updateIndexMappings() cannot be acknowledged\n")
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
