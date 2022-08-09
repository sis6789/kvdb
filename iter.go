package kvdb

import (
	"github.com/dgraph-io/badger/v3"
	"log"
)

// wrapper for badger key-value db

type Iter struct {
	iter   *badger.Iterator
	txn    *badger.Txn
	rw     string
	buffer []byte
}

func (x *KVDB) NewIterator() *Iter {
	var iter Iter
	iter.txn = x.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	iter.iter = iter.txn.NewIterator(opts)
	iter.buffer = make([]byte, 0, 4096)
	return &iter
}

func (x *Iter) Close() {
	x.iter.Close()
	x.txn.Discard()
}

func (x *Iter) Rewind() {
	x.iter.Rewind()
}

func (x *Iter) Valid() bool {
	return x.iter.Valid()
}

func (x *Iter) Next() {
	x.iter.Next()
}

func (x *Iter) KeyValue() (key, value []byte, err error) {
	it := x.iter.Item()
	key = it.Key()
	value, err = it.ValueCopy(x.buffer)
	if err != nil {
		log.Printf("%v", err)
	}
	return
}

func (x *Iter) Value() (value []byte, err error) {
	it := x.iter.Item()
	value, err = it.ValueCopy(x.buffer)
	if err != nil {
		log.Printf("%v", err)
	}
	return
}

func (x *Iter) Key() (key []byte) {
	it := x.iter.Item()
	key = it.Key()
	return
}

func (x *Iter) KeyValueString() (key, value string, err error) {
	kb, vb, err := x.KeyValue()
	key = string(kb)
	value = string(vb)
	return
}

func (x *Iter) ValueString() (value string, err error) {
	vb, err := x.Value()
	value = string(vb)
	return
}

func (x *Iter) KeyString() (key string) {
	kb := x.Key()
	key = string(kb)
	return
}
