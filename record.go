package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/tuvistavie/securerandom"
)

func writeRecord(record map[string]string, file *os.File) {
	fmt.Fprintf(file, ":0:%s\n", record["data0"])
	fmt.Fprintf(file, ":1:%s\n", record["data1"])
	fmt.Fprintf(file, ":2:%s\n", record["data2"])
	fmt.Fprintf(file, ":3:%s\n", record["data3"])
	fmt.Fprintf(file, ":4:%s\n", record["data4"])
	fmt.Fprintf(file, ":5:%s\n", record["data5"])
	fmt.Fprintf(file, ":6:%s\n", record["data6"])
	fmt.Fprintf(file, ":7:%s\n", record["data7"])
	fmt.Fprintf(file, ":8:%s\n", record["data8"])
	fmt.Fprintf(file, ":9:%s\n", record["data9"])
	fmt.Fprintf(file, ":10:%s\n", record["data10"])
	fmt.Fprintf(file, ":11:%s\n", record["data11"])
	fmt.Fprintf(file, ":12:%s\n", record["data12"])
	fmt.Fprintf(file, ":13:%s\n", record["data13"])
	fmt.Fprintf(file, ":14:%s\n", record["data14"])
	fmt.Fprintf(file, ":15:%s\n", record["data15"])
	fmt.Fprintf(file, ":16:%s\n", record["data16"])
	fmt.Fprintf(file, ":17:%s\n", record["data17"])
	fmt.Fprintf(file, ":18:%s\n", record["data18"])
	fmt.Fprintf(file, ":19:%s\n", record["data19"])
}

func generateUUID() string {
	uuid, err := securerandom.Uuid()
	if err != nil {
		panic(err)
	}
	return uuid
}

func generateRecordJson() string {
	json, err := json.Marshal(generateRecord())
	if err != nil {
		panic(err)
	}
	return string(json)
}

func generateRecord() map[string]string {
	return map[string]string{
		"data0":  generateRandomHex(64),
		"data1":  generateRandomHex(64),
		"data2":  generateRandomHex(64),
		"data3":  generateRandomHex(64),
		"data4":  generateRandomHex(64),
		"data5":  generateRandomHex(64),
		"data6":  generateRandomHex(64),
		"data7":  generateRandomHex(64),
		"data8":  generateRandomHex(64),
		"data9":  generateRandomHex(64),
		"data10": generateRandomHex(64),
		"data11": generateRandomHex(64),
		"data12": generateRandomHex(64),
		"data13": generateRandomHex(64),
		"data14": generateRandomHex(64),
		"data15": generateRandomHex(64),
		"data16": generateRandomHex(64),
		"data17": generateRandomHex(64),
		"data18": generateRandomHex(64),
		"data19": generateRandomHex(64),
	}
}

func generateRandomHex(n int) string {
	random, err := securerandom.Hex(n)
	if err != nil {
		panic(err)
	}
	return random
}
