package db

import (
	"fmt"
	"sync"
	"time"
)

var wmux sync.Mutex
var rmux sync.Mutex

//KVStore is the key-value store used to save and retrieve data from disk.
type KVStore struct {
	store *hashtableStore
}

//This is thrown when the looked up key is not present in the store
type keyNotFoundError struct {
}

func (e *keyNotFoundError) Error() string {
	return "Key not found"
}

//This is thrown when the looked up key is already present in the store
type keyAlreadyExistsError struct {
}

func (e *keyAlreadyExistsError) Error() string {
	return "Key already exists !!"
}

//This is thrown when the value passed exceeds the allowed limit of 64KB
type valueTooLargeError struct {
}

func (e *valueTooLargeError) Error() string {
	return "JSON object exceeds max allowed size of 64KB, this may lead to errors in Unmarshalling"
}

//NewKVStore creates new key-value store. Data file will be created in the 'filePath'.
func NewKVStore(filePath string) KVStore {

	return KVStore{store: newHashtableStore(filePath)}
}

//Create adds the passed key and value to the store.
//value is the 'marshalled JSON object'
//If expiration is greater than 0, it will be deleted from the store after those seconds.
func (db KVStore) Create(key string, value []byte, expiration time.Duration) error {

	wmux.Lock()

	err := db.validate(key, value)

	if err != nil {
		return err
	}

	key = truncateKey(key)
	node := getNewNode(key, string(value))

	//add node to list store
	db.store.addNode(node)

	//schedule deletion based on expiration if available
	if expiration > 0 {
		db.scheduleDelete(key, expiration)
	}
	fmt.Println(key, node.Pos)

	wmux.Unlock()

	return nil

}

//Get retrieves the value from the store for the passed key.
func (db KVStore) Get(key string) ([]byte, error) {

	rmux.Lock()

	key = truncateKey(key) //if at all, client passes key of more than allowed length...

	node, err := db.store.getNode(key)

	rmux.Unlock()

	if err != nil {
		return make([]byte, 0), &keyNotFoundError{}
	}

	return []byte(node.Value), nil

}

//Exists checks if the key is already present in the store..
func (db KVStore) Exists(key string) bool {

	_, err := db.Get(truncateKey(key)) //if at all, client passes key of more than allowed length...
	return err == nil
}

//Delete deletes the passed key from the store permanently..
func (db KVStore) Delete(key string) {
	key = truncateKey(key)
	wmux.Lock()
	rmux.Lock()
	db.store.deleteNode(key)
	wmux.Unlock()
	rmux.Unlock()
}

//Schedules the delete operation after passed expiration in seconds
func (db KVStore) scheduleDelete(key string, expiration time.Duration) {
	time.AfterFunc(expiration*time.Second, func() { db.Delete(key) })
}

//Close is used to close the data file help up by the store.
//Use this using 'defer Close()' always whenever instantiating the store
func (db KVStore) Close() {
	db.store.close()
}

//validates the key and val. Also validates whether storage is ful
func (db KVStore) validate(key string, val []byte) error {

	if len(val) > maxValLength {
		return &valueTooLargeError{}
	}
	if db.store.isfull() {
		return &storageFullError{}
	}

	if db.Exists(key) {
		return &keyAlreadyExistsError{}
	}

	return nil
}
