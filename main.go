package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/Jeffail/gabs"
	badger "github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/pb"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	flow "github.com/onflow/flow-go/model/flow"

	"github.com/vmihailenco/msgpack"
)

var (
	dbMap map[string]*EventWriter = nil
)

type EventWriter struct {
	db           *sql.DB
	eventName    string
	writeChannel chan []any
}

func (ew *EventWriter) Run() {

	for {
		data, ok := <-ew.writeChannel
		if ok == false {
			fmt.Println("Channel Close ", ok)
			break
		}

		_, err := ew.db.Exec(string(data[0].(string)), data[1:]...)
		if err != nil {

			if ew.eventName != "A.4eb8a10cb9f87357.NFTStorefront.ListingCompleted" {

				fmt.Println(ew.eventName)
				fmt.Println(data[0])
				fmt.Println(data)
				log.Fatal(err)
			}
		}
	}

}

func NewEventWriter(eventName string, payload string) *EventWriter {

	parts := strings.Split(eventName[2:], ".")
	path := "db/" + strings.Join(parts[:len(parts)-1], "/")

	fullPath := path + "/" + parts[len(parts)-1] + ".db"

	os.MkdirAll(path, 0777)
	os.Remove(fullPath)

	sdb, err := sql.Open("sqlite3", fullPath)
	if err != nil {
		log.Fatal(err)
	}

	jsonParsed, err := gabs.ParseJSON([]byte(payload))
	if err != nil {
		panic(err)
	}
	s, _ := jsonParsed.Search("value", "fields").Children()

	fields := ""

	for _, child := range s {
		value := child.Data().(map[string]interface{})
		xvalue := value["value"].(map[string]interface{})
		fieldName := value["name"].(string)
		fieldType := xvalue["type"].(string)
		if fieldType == "Optional" {
			if xvalue["value"] == nil {
				fieldType = "text"
			} else {
				fieldType = xvalue["value"].(map[string]interface{})["type"].(string)
			}
		}

		if strings.Contains(fieldType, "Int") || strings.Contains(fieldType, "Fix") {
			fieldType = "integer"
		} else {
			fieldType = "text"
		}
		fields = fmt.Sprintf("%s _%s %s, ", fields, fieldName, fieldType)
	}

	fields = fmt.Sprintf("%s note text", fields)
	sqlStmt := fmt.Sprintf(`
	create table Events (id integer not null primary key, blockID text, transactionID text, transactionIndex integer, eventIndex integer, eventType text, %s);
	`, fields)

	_, err = sdb.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return nil
	}
	sdb.Exec(`PRAGMA journal_mode = OFF;
	PRAGMA synchronous = 0;
	PRAGMA cache_size = 1000000;
	PRAGMA locking_mode = EXCLUSIVE;
	PRAGMA temp_store = MEMORY;`)

	return &EventWriter{
		db:           sdb,
		eventName:    eventName,
		writeChannel: make(chan []any, 10),
	}

}

func ensureDb(contractEvent string, samplePayload string) *EventWriter {

	v, ok := dbMap[contractEvent]
	if ok {
		return v
	}

	writer := NewEventWriter(contractEvent, samplePayload)
	dbMap[contractEvent] = writer
	go writer.Run()
	return writer

}

func main() {

	dbMap = make(map[string]*EventWriter)

	//open badger database
	db, err := badger.Open(badger.DefaultOptions("/mnt/flow/mainnet16/protocol"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stream := db.NewStream()
	// db.NewStreamAt(readTs) for managed mode.

	// -- Optional settings
	stream.NumGo = 32                     // Set number of goroutines to use for iteration.
	stream.Prefix = []byte{0x66}          //events
	stream.LogPrefix = "Badger.Streaming" // For identifying stream logs. Outputs to Logger.
	stream.KeyToList = nil

	count := 0
	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.GetKv() {
			count = count + 1
			if count%10000 == 0 {
				fmt.Println(count)
			}
			//k key

			k := kv.GetKey()
			v := kv.GetValue()

			blockID := hex.EncodeToString(k[1:33])
			transactionID := hex.EncodeToString(k[33:65])
			transactionIndex := hex.EncodeToString(k[65:69])
			transactionIndexInt, err := strconv.ParseInt(transactionIndex, 16, 32)

			eventIndex := hex.EncodeToString(k[69:73])
			eventIndexInt, err := strconv.ParseInt(eventIndex, 16, 32)

			//v value
			var event flow.Event
			err = msgpack.Unmarshal(v, &event)
			if err != nil {
				return fmt.Errorf("could not decode the event: %w", err)
			}
			//fmt.Println(string(event.Payload))

			writer := ensureDb(string(event.Type), string(event.Payload))

			jsonParsed, err := gabs.ParseJSON(event.Payload)
			if err != nil {
				panic(err)
			}
			s, _ := jsonParsed.Search("value", "fields").Children()

			var vs []any = make([]any, 0)
			insertFields := ""
			valuesMarker := ""
			for _, child := range s {
				value := child.Data().(map[string]interface{})
				xvalue := value["value"].(map[string]interface{})
				fieldType := xvalue["type"].(string)
				fieldValue := ""
				fieldName := value["name"].(string)

				if fieldType == "Optional" {
					if xvalue["value"] != nil {
						fieldType = xvalue["value"].(map[string]interface{})["type"].(string)
						fieldValue = fmt.Sprintf("%s", xvalue["value"].(map[string]interface{})["value"])
					} else {
						fieldValue = "null"
					}
				} else {
					switch tt := xvalue["value"].(type) {
					case string:
						fieldValue = tt
					default:
						b, _ := json.Marshal(xvalue["value"])
						fieldValue = string(b)
					}
				}

				if strings.Contains(fieldType, "Int") {
					number, _ := strconv.ParseInt(fieldValue, 10, 0)
					vs = append(vs, number)
				} else if strings.Contains(fieldType, "Fix") {
					number, _ := strconv.ParseInt(strings.Replace(fieldValue, ".", "", 1), 10, 0)
					vs = append(vs, number)
				} else {
					vs = append(vs, fieldValue)
				}
				insertFields = fmt.Sprintf("%s _%s,", insertFields, fieldName)
				valuesMarker = fmt.Sprintf("%s ?,", valuesMarker)
			}
			stmt := fmt.Sprintf(`insert into Events(blockID, transactionID, transactionIndex, eventIndex, eventType, %s note) values(?, ?, ?, ?, ?, %s ?)`, insertFields, valuesMarker)

			var args []any = make([]any, 0)
			args = append(args, stmt)
			args = append(args, blockID)
			args = append(args, transactionID)
			args = append(args, transactionIndexInt)
			args = append(args, eventIndexInt)
			args = append(args, event.Type)
			args = append(args, vs...)
			args = append(args, "")
			writer.writeChannel <- args

		}
		return nil
	}

	if err := stream.Orchestrate(context.Background()); err != nil {
		fmt.Println(err)
	}

	for {

		time.Sleep(time.Second)
	}

}
