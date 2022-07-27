package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/Jeffail/gabs"
	badger "github.com/dgraph-io/badger/v2"
	_ "github.com/mattn/go-sqlite3"
	flow "github.com/onflow/flow-go/model/flow"

	"github.com/vmihailenco/msgpack"
)

var (
	dbMap map[string]*sql.DB = nil
)

type Envelope struct {
	Type  string      `json:type`
	Value interface{} `json:value`
}

func getInnerField(v map[string]interface{}, key string) string {
	value := v[key]

	switch vv := value.(type) {

	case map[string]interface{}:
		return getInnerField(vv, key)

	case string:
		return vv

	default:
		fmt.Println(vv)
	}

	return "DNZ"
}

func ensureDb(contractEvent string, samplePayload string) *sql.DB {

	v, ok := dbMap[contractEvent]
	if ok {
		return v
	}

	parts := strings.Split(contractEvent[2:], ".")
	path := "db/" + strings.Join(parts[:len(parts)-1], "/")

	fullPath := path + "/" + parts[len(parts)-1] + ".db"

	//lets make target sqlite
	os.MkdirAll(path, 0777)
	os.Remove(fullPath)

	sdb, err := sql.Open("sqlite3", fullPath)
	if err != nil {
		log.Fatal(err)
	}

	jsonParsed, err := gabs.ParseJSON([]byte(samplePayload))
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
			fieldType = xvalue["value"].(map[string]interface{})["type"].(string)
		}

		if strings.Contains(fieldType, "Int") || strings.Contains(fieldType, "Fix") {
			fieldType = "integer"
		} else {
			fieldType = "text"
		}
		fields = fmt.Sprintf("%s _%s %s, ", fields, fieldName, fieldType)
	}

	fields = fmt.Sprintf("%s note text", fields)
	//crrate events table
	sqlStmt := fmt.Sprintf(`
	create table Events (id integer not null primary key, blockID text, transactionID text, transactionIndex integer, eventIndex integer, eventType text, %s);
	`, fields)

	_, err = sdb.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return nil
	}

	dbMap[contractEvent] = sdb
	return sdb

}

func main() {

	dbMap = make(map[string]*sql.DB)

	//open badger database
	db, err := badger.Open(badger.DefaultOptions("/mnt/external1/mainnet1/protocol"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//query events
	db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte{0x66} //event prefix
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()

			blockID := hex.EncodeToString(k[1:33])
			transactionID := hex.EncodeToString(k[33:65])
			transactionIndex := hex.EncodeToString(k[65:69])
			transactionIndexInt, err := strconv.ParseInt(transactionIndex, 16, 32)

			eventIndex := hex.EncodeToString(k[69:73])
			eventIndexInt, err := strconv.ParseInt(eventIndex, 16, 32)

			err = item.Value(func(v []byte) error {

				var event flow.Event
				err := msgpack.Unmarshal(v, &event)
				if err != nil {
					return fmt.Errorf("could not decode the event: %w", err)
				}

				sdb := ensureDb(string(event.Type), string(event.Payload))

				jsonParsed, err := gabs.ParseJSON(event.Payload)
				if err != nil {
					panic(err)
				}
				s, _ := jsonParsed.Search("value", "fields").Children()

				fields := ""
				values := ""
				valuesMarker := ""
				var vs []any = make([]any,0)

				for _, child := range s {
					value := child.Data().(map[string]interface{})
					xvalue := value["value"].(map[string]interface{})
					fieldName := value["name"].(string)
					fieldType := xvalue["type"].(string)
					fieldValue := ""
					if fieldType == "Optional" {
						if xvalue["value"] != nil {
							fieldType = xvalue["value"].(map[string]interface{})["type"].(string)
							fieldValue = xvalue["value"].(map[string]interface{})["value"].(string)
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

					if strings.Contains(fieldType, "Int"){
						fieldType = "integer"
						number, _ := strconv.ParseInt(fieldValue, 10, 0)
						vs = append(vs, number)
					}else if strings.Contains(fieldType, "Fix") {
						fieldType = "integer"
						number, _ := strconv.ParseInt(strings.Replace(fieldValue, ".","",1),10,0)
						vs = append(vs, number)
					} else {
						fieldType = "text"
						vs = append(vs, fieldValue)
					}
					fields = fmt.Sprintf("%s _%s, ", fields, fieldName)
					valuesMarker = fmt.Sprintf("%s ?,", valuesMarker)
					values = fmt.Sprintf("%s '%s', ", values, fieldValue)
				}

				fields = fmt.Sprintf("%s note", fields)
				values = fmt.Sprintf("%s ''", values)
				valuesMarker = fmt.Sprintf("%s ?", valuesMarker)

				stmt := fmt.Sprintf(`insert into Events(blockID, transactionID, transactionIndex, eventIndex, eventType, %s) values(?, ?, ?, ?, ?, %s)`, fields, valuesMarker)

				var args []any = make([]any,0)
				args = append(args, blockID)
				args = append(args, transactionID)
				args = append(args, transactionIndexInt)
				args = append(args, eventIndexInt)
				args = append(args, event.Type)
				for _, item := range(vs){
					args = append(args, item)
				}
				args = append(args, "")
					
				fmt.Println(args)

				_, err = sdb.Exec(stmt,args...)
				//blockID, transactionID, transactionIndexInt, eventIndexInt, event.Type, string(event.Payload), vs...)
				if err != nil {
					log.Fatal(err)
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

}
