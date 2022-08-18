package kvdb

// wrapper for badger key-value db

import (
	"errors"
	"log"
	"strconv"

	"github.com/dgraph-io/badger/v3"
)

type KVDB struct {
	db     *badger.DB
	dbPath string
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

	return nil
}

func (x *KVDB) Close() error {
	if x.db == nil {
		log.Printf("no db")
		return errors.New("no db")
	}
	err := x.db.Close()
	if err != nil {
		log.Printf("%v", err)
		return err
	} else {
		log.Printf("kvdb close at %v", x.dbPath)
		return nil
	}
}

func (x *KVDB) Set(k string, v string) error {

	err := x.db.Update(func(txn *badger.Txn) error {
		kb := []byte(k)
		vb := []byte(v)
		err := txn.Set(kb, vb)
		switch err {
		case badger.ErrTxnTooBig:
			if err = txn.Commit(); err != nil {
				log.Printf("%v", err)
			}
			txn.Discard()
			txn = x.db.NewTransaction(true)
			return x.Set(k, v)
		case badger.ErrDiscardedTxn:
			txn = x.db.NewTransaction(true)
			return x.Set(k, v)
		default:
			err = txn.Commit()
			if err != nil {
				log.Printf("%v", err)
			}
		}
		return err
	})

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
	readString := ""
	err := x.db.View(func(txn *badger.Txn) error {
		kb := []byte(k)
		item, err := txn.Get(kb)
		switch err {
		case badger.ErrKeyNotFound:
			return err
		case badger.ErrDiscardedTxn:
			txn = x.db.NewTransaction(true)
			v, err := x.Get(k)
			if err == nil {
				readString = v
			}
			return err
		case nil:
			var vb []byte
			vb, err = item.ValueCopy(vb)
			if err != nil {
				log.Printf("%v", err)
			} else {
				readString = string(vb)
			}
			return err
		default:
			log.Printf("%v", err)
			return err
		}
	})
	return readString, err
}
