package tikv

import (
	"context"
	"errors"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logger "github.com/ipfs/go-log"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/txnkv"
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

// Options are the badger datastore options, reexported here for convenience.
type Options struct {
	config.Config
}

var ErrClosed = errors.New("datastore closed")

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.TxnDatastore = (*Datastore)(nil)
var _ ds.TTLDatastore = (*Datastore)(nil)
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
	panic("ttl")
}

func (d *Datastore) SetTTL(key ds.Key, ttl time.Duration) error {
	panic("ttl")
}

func (d *Datastore) GetExpiration(key ds.Key) (time.Time, error) {
	panic("ttl")
}

func (d *Datastore) NewTransaction(readOnly bool) (ds.Txn, error) {
	panic("txn")
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
	panic("ds.Datastore")
}

func (d *Datastore) Put(key ds.Key, value []byte) error {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	if d.closed {
		return ErrClosed
	}

	txn := d.newImplicitTransaction(false)
	defer txn.discard()

	if err := txn.put(key, value); err != nil {
		return err
	}

	return txn.commit()
}

func (d *Datastore) Delete(key ds.Key) error {
	panic("ds.Datastore")
}

func (d *Datastore) Sync(prefix ds.Key) error {
	panic("ds.Datastore")
}

func (d *Datastore) Close() error {
	panic("ds.Datastore")
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

func (t *txn) discard() {
	e := t.txn.Rollback()
	if e != nil {
		log.Error("discard error:", e)
	}

}
