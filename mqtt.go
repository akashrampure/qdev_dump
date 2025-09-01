package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fabrikiot/wsmqttrt/wsmqttrtpuller"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/snappy"
)

var (
	mu             sync.Mutex
	fileMu         sync.Mutex
	deviceHeaders  = make(map[string][]string)
	deviceData     = make(map[string][][]string)
	deviceMsgCount = make(map[string]int)
	baseFolder     string
	outputDir      = "/mnt/pssd/modhexdump"
)

func initOutputFolder(topic, jobname string) {
	dateFolder := time.Now().Format("2006/01/02")
	baseFolder = path.Join(outputDir, dateFolder, topic, jobname)

	err := os.MkdirAll(baseFolder, 0755)
	if err != nil {
		fmt.Println("Error creating folder:", err)
		os.Exit(1)
	}
	fmt.Println("Saving CSVs in folder:", baseFolder)
}

func ExtractDeviceIdsFromFile(filename string) []string {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return []string{}
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV:", err)
		return []string{}
	}

	var deviceIds []string
	for _, record := range records {
		if len(record) > 0 {
			deviceIds = append(deviceIds, strings.TrimSpace(record[0]))
		}
	}
	return deviceIds
}

func appendDataToCSV(filename string, headers []string, records [][]string) error {
	fileMu.Lock()
	defer fileMu.Unlock()

	filename = path.Join(baseFolder, filename+".csv")

	_, err := os.Stat(filename)
	isNewFile := os.IsNotExist(err)

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open csv file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if isNewFile {
		if err := writer.Write(headers); err != nil {
			return fmt.Errorf("failed to write headers: %w", err)
		}
	}

	for _, rec := range records {
		if err := writer.Write(rec); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	fmt.Println("Wrote to CSV:", filename)

	return nil
}

func flushDeviceData(deviceid string) {
	records := deviceData[deviceid]
	if len(records) == 0 {
		return
	}

	headers, ok := deviceHeaders[deviceid]
	if !ok {
		fmt.Println("No headers for device:", deviceid)
		return
	}

	if err := appendDataToCSV(deviceid, headers, records); err != nil {
		fmt.Println("Error writing to CSV:", err)
	}
	deviceData[deviceid] = nil
	deviceMsgCount[deviceid] = 0
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run filename.go <csvfile> <topic> <jobname>")
		os.Exit(1)
	}

	csvfileName := os.Args[1]
	topic := os.Args[2]
	jobname := os.Args[3]

	initOutputFolder(topic, jobname)

	deviceIds := ExtractDeviceIdsFromFile(csvfileName)

	wspulleropts := wsmqttrtpuller.NewWsMqttRtPullerOpts("qdev1.buzzenergy.in", 11884)
	wg := &sync.WaitGroup{}

	stoppedflag := uint32(0)
	var once sync.Once

	statecallback := &wsmqttrtpuller.WsMqttRtPullerStateCallback{
		Started: func() {
			fmt.Println("Puller started")
		},
		Stopped: func() {
			once.Do(func() {
				atomic.StoreUint32(&stoppedflag, 1)
				fmt.Println("Puller stopped")
			})
		},
	}

	subscribecallback := func(topic []byte, issubscribe bool, isok bool) {
		if isok {
			fmt.Printf("Subscribed to topic: %s\n", string(topic))
		} else {
			fmt.Printf("Failed to subscribe to topic: %s\n", string(topic))
		}
	}

	msgcallback := func(topic []byte, payload []byte) {
		topics := strings.Split(string(topic), "/")
		if len(topics) < 3 {
			return
		}
		deviceid := strings.TrimSpace(topics[2])

		decompressed, err := snappy.Decode(nil, payload)
		if err != nil {
			fmt.Println("Snappy decompression error:", err)
			return
		}

		var rawData map[string]interface{}
		err = json.Unmarshal(decompressed, &rawData)
		if err != nil {
			fmt.Println("Error unmarshalling payload:", err)
			return
		}

		mu.Lock()
		defer mu.Unlock()

		if _, ok := deviceHeaders[deviceid]; !ok {
			keys := make([]string, 0, len(rawData))
			for k := range rawData {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			headers := []string{"timestamp", "deviceid"}
			headers = append(headers, keys...)
			deviceHeaders[deviceid] = headers
		}

		record := []string{time.Now().Format("02-1-06 15:04:05"), deviceid}
		for _, h := range deviceHeaders[deviceid][2:] {
			val := ""
			if v, ok := rawData[h]; ok {
				switch vv := v.(type) {
				case string:
					val = vv
				case float64:
					val = strconv.FormatFloat(vv, 'f', -1, 64)
				default:
					val = fmt.Sprintf("%v", vv)
				}
			}
			record = append(record, val)
		}

		deviceData[deviceid] = append(deviceData[deviceid], record)
		deviceMsgCount[deviceid]++
		if deviceMsgCount[deviceid] >= 5 {
			headers := deviceHeaders[deviceid]
			records := deviceData[deviceid]

			if err := appendDataToCSV(deviceid, headers, records); err != nil {
				fmt.Println("Error writing to CSV:", err)
				return
			}

			deviceData[deviceid] = nil
			deviceMsgCount[deviceid] = 0
		}
	}

	wspuller := wsmqttrtpuller.NewWsMqttRtPuller(wspulleropts, statecallback, msgcallback)
	wg.Add(1)
	go func() {
		defer wg.Done()
		wspuller.Start()
		for atomic.LoadUint32(&stoppedflag) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	subscribedTopics := make(map[string]bool)
	for _, deviceid := range deviceIds {
		fullTopic := "/lafraw/" + deviceid + "/" + topic
		if !subscribedTopics[fullTopic] {
			subscribedTopics[fullTopic] = true
			wspuller.Subscribe([]byte(fullTopic), subscribecallback)
		}
	}

	ossigch := make(chan os.Signal, 1)
	signal.Notify(ossigch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	<-ossigch
	fmt.Println("Interrupt signal received, stopping...")

	wspuller.Stop()
	wg.Wait()

	for deviceid := range deviceData {
		flushDeviceData(deviceid)
	}
}
