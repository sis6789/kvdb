package kvdb

// wrapper for badger key-value db
// refer https://dgraph.io/docs/badger/

import (
	"github.com/dgraph-io/badger/v3"
	"log"
)

import (
	"errors"
	"strconv"
)

type KVDB struct {
	db        *badger.DB
	dbPath    string
	updateTxn *badger.Txn
}

func (x *KVDB) DB() *badger.DB {
	return x.db
}

func (x *KVDB) Open(path string) error {

	if path == "" {
		log.Printf("no db path")
		return errors.New("no db path")
	}
	var err error
	x.dbPath = path
	x.db, err = badger.Open(badger.DefaultOptions(x.dbPath))
	if err != nil {
		log.Printf("%v", err)
		return err
	}
	log.Printf("kvdb at %v", x.dbPath)

	x.updateTxn = x.db.NewTransaction(true)

	return nil
}

func (x *KVDB) Close() error {
	if x.db == nil {
		log.Printf("no db")
		return errors.New("no db")
	}
	var err error
	if err = x.updateTxn.Commit(); err != nil {
		log.Printf("%v", err)
		return err
	}
	x.updateTxn.Discard()
	err = x.db.Close()
	if err != nil {
		log.Printf("%v", err)
		return err
	} else {
		log.Printf("kvdb close at %v", x.dbPath)
		return nil
	}
}

func (x *KVDB) Set(k string, v string) error {
	if x.updateTxn == nil {
		log.Printf("no transaction")
		return errors.New("no transaction")
	}
	kb := []byte(k)
	vb := []byte(v)
	err := x.updateTxn.Set(kb, vb)
	switch err {
	case badger.ErrTxnTooBig:
		if err = x.updateTxn.Commit(); err != nil {
			log.Printf("%v", err)
		}
		x.updateTxn.Discard()
		x.updateTxn = x.db.NewTransaction(true)
		return x.Set(k, v)
	case badger.ErrDiscardedTxn:
		x.updateTxn = x.db.NewTransaction(true)
		return x.Set(k, v)
	}
	return err
}

func (x *KVDB) SetInt(k string, v int) error {
	return x.Set(k, strconv.Itoa(v))
}

func (x *KVDB) GetInt(k string) (int, error) {
	v, err := x.Get(k)
	vi := 0
	if err == nil {
		vi, _ = strconv.Atoi(v)
	}
	return vi, err
}

func (x *KVDB) Get(k string) (string, error) {
	if x.updateTxn == nil {
		log.Printf("no transaction")
		return "", errors.New("no transaction")
	}
	kb := []byte(k)
	item, err := x.updateTxn.Get(kb)
	switch err {
	case badger.ErrKeyNotFound:
		return "", err
	case badger.ErrDiscardedTxn:
		x.updateTxn = x.db.NewTransaction(true)
		return x.Get(k)
	case nil:
		var vb []byte
		vb, err = item.ValueCopy(vb)
		if err != nil {
			log.Printf("%v", err)
		}
		return string(vb), err
	default:
		log.Printf("%v", err)
		return "", err
	}
}
