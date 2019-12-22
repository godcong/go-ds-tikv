package tikv

import (
	"context"
	"errors"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logger "github.com/ipfs/go-log"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
	"sync"
	"time"
)

type Datastore struct {
	cli       *txnkv.Client
	closeLk   sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closing   chan struct{}

	gcDiscardRatio float64
	gcSleep        time.Duration
	gcInterval     time.Duration

	syncWrites bool
}

// Implements the datastore.Txn interface, enabling transaction support for
// the badger Datastore.
type txn struct {
	ds  *Datastore
	txn *txnkv.Transaction

	// Whether this transaction has been implicitly created as a result of a direct Datastore
	// method invocation.
	implicit bool
}

func (t *txn) Get(key ds.Key) (value []byte, err error) {
	return t.get(key)
}

func (t *txn) Has(key ds.Key) (exists bool, err error) {
	panic("implement me")
}

func (t *txn) GetSize(key ds.Key) (size int, err error) {
	panic("implement me")
}

func (t *txn) Query(q query.Query) (query.Results, error) {
	panic("implement me")
}

func (t *txn) Put(key ds.Key, value []byte) error {
	panic("implement me")
}

func (t *txn) Delete(key ds.Key) error {
	panic("implement me")
}

func (t *txn) Commit() error {
	panic("implement me")
}

func (t *txn) Discard() {
	panic("implement me")
}

// Options are the badger datastore options, reexported here for convenience.
type Options struct {
	config.Config
}

var ErrClosed = errors.New("datastore closed")

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.TxnDatastore = (*Datastore)(nil)

//var _ ds.TTLDatastore = (*Datastore)(nil)
var _ ds.GCDatastore = (*Datastore)(nil)

var log = logger.Logger("tikv")

func NewDatastore(addr []string, options *Options) (*Datastore, error) {
	kv, err := txnkv.NewClient(context.TODO(), addr, options.Config)
	//kv, err := rawkv.NewClient(context.TODO(), []string{"127.0.0.1:2379"}, options.Config)
	if err != nil {
		return nil, err
	}

	ds := &Datastore{
		cli:     kv,
		closing: make(chan struct{}),
	}

	return ds, nil
}

func (d *Datastore) PutWithTTL(key ds.Key, value []byte, ttl time.Duration) error {
	txn := d.newImplicitTransaction(false)
	defer txn.rollback()

	if err := txn.putWithTTL(key, value, ttl); err != nil {
		return err
	}

	return txn.commit()
}

func (d *Datastore) SetTTL(key ds.Key, ttl time.Duration) error {
	panic("ttl")
}

func (d *Datastore) GetExpiration(key ds.Key) (time.Time, error) {
	panic("ttl")
}

func (d *Datastore) NewTransaction(readOnly bool) (ds.Txn, error) {
	transaction, e := d.cli.Begin(context.TODO())
	if e != nil {
		return nil, e
	}
	return &txn{ds: d, txn: transaction, implicit: false}, nil
}

func (d *Datastore) Get(key ds.Key) (value []byte, err error) {
	panic("ds.Datastore")
}

func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	panic("ds.Datastore")
}

func (d *Datastore) GetSize(key ds.Key) (size int, err error) {
	panic("ds.Datastore")
}

func (d *Datastore) Query(q query.Query) (query.Results, error) {
	txn := d.newImplicitTransaction(true)

	return txn.query(q)
}

func (d *Datastore) Put(key ds.Key, value []byte) error {
	txn := d.newImplicitTransaction(false)
	defer txn.rollback()

	if err := txn.put(key, value); err != nil {
		return err
	}

	return txn.commit()
}

func (d *Datastore) Delete(key ds.Key) error {
	panic("ds.Datastore")
}

func (d *Datastore) Sync(prefix ds.Key) error {
	return nil
}

func (d *Datastore) Close() error {
	return nil
}

func (d *Datastore) CollectGarbage() error {
	panic("ds.Datastore")
}

// newImplicitTransaction creates a transaction marked as 'implicit'.
// Implicit transactions are created by Datastore methods performing single operations.
func (d *Datastore) newImplicitTransaction(readOnly bool) *txn {
	t, err := d.cli.Begin(context.TODO())
	if err != nil {
		return nil
	}
	return &txn{d, t, true}
}

func (t *txn) put(key ds.Key, bytes []byte) error {
	return t.txn.Set(key.Bytes(), bytes)
}

func (t *txn) commit() error {
	return t.txn.Commit(context.Background())
}

func (t *txn) rollback() {
	e := t.txn.Rollback()
	if e != nil {
		log.Error("discard error:", e)
	}
}

func (t *txn) putWithTTL(key ds.Key, bytes []byte, duration time.Duration) error {
	panic("todo")
}

func (t *txn) get(key ds.Key) ([]byte, error) {
	item, err := t.txn.Get(context.TODO(), key.Bytes())
	if err == kv.ErrNotExist {
		err = ds.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (t *txn) query(q query.Query) (query.Results, error) {
	panic("TODO")
}
