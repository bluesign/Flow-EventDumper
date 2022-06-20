package main

import (
	"encoding/hex"
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger/v2"
	flow "github.com/onflow/flow-go/model/flow"
	"github.com/vmihailenco/msgpack"
)

func main() {

	//open database

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
			eventIndex := hex.EncodeToString(k[69:73])

			fmt.Printf("BlockID\t\t : %s\nTransactionID\t : %s\nTransactionIndex : %s\nEventIndex\t : %s\n", blockID, transactionID, transactionIndex, eventIndex)

			err := item.Value(func(v []byte) error {

				var event flow.Event
				err := msgpack.Unmarshal(v, &event)
				if err != nil {
					return fmt.Errorf("could not decode the event: %w", err)
				}
				fmt.Printf("Event Type\t: %s\n", event.Type)
				fmt.Printf("Event Payload\t: %s\n\n", string(event.Payload))

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}
