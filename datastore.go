package tikv

import (
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logger "github.com/ipfs/go-log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
	"sync"
	"time"
)

type Datastore struct {
	cli       *tikv.RawKVClient
	closeLk   sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closing   chan struct{}

	gcDiscardRatio float64
	gcSleep        time.Duration
	gcInterval     time.Duration

	syncWrites bool
}

func (d Datastore) PutWithTTL(key ds.Key, value []byte, ttl time.Duration) error {
	panic("ttl")
}

func (d Datastore) SetTTL(key ds.Key, ttl time.Duration) error {
	panic("ttl")
}

func (d Datastore) GetExpiration(key ds.Key) (time.Time, error) {
	panic("ttl")
}

func (d Datastore) NewTransaction(readOnly bool) (ds.Txn, error) {
	panic("txn")
}

func (d Datastore) Get(key ds.Key) (value []byte, err error) {
	panic("ds.Datastore")
}

func (d Datastore) Has(key ds.Key) (exists bool, err error) {
	panic("ds.Datastore")
}

func (d Datastore) GetSize(key ds.Key) (size int, err error) {
	panic("ds.Datastore")
}

func (d Datastore) Query(q query.Query) (query.Results, error) {
	panic("ds.Datastore")
}

func (d Datastore) Put(key ds.Key, value []byte) error {
	panic("ds.Datastore")
}

func (d Datastore) Delete(key ds.Key) error {
	panic("ds.Datastore")
}

func (d Datastore) Sync(prefix ds.Key) error {
	panic("ds.Datastore")
}

func (d Datastore) Close() error {
	panic("ds.Datastore")
}

func (d Datastore) CollectGarbage() error {
	panic("ds.Datastore")
}

// Options are the badger datastore options, reexported here for convenience.
type Options struct {
	config.Security
}

var _ ds.Datastore = (*Datastore)(nil)
var _ ds.TxnDatastore = (*Datastore)(nil)
var _ ds.TTLDatastore = (*Datastore)(nil)
var _ ds.GCDatastore = (*Datastore)(nil)

var log = logger.Logger("tikv")

func NewDatastore(addr []string, options *Options) (*Datastore, error) {
	kv, err := tikv.NewRawKVClient(addr, options.Security)
	if err != nil {
		return nil, err
	}

	ds := &Datastore{
		cli:     kv,
		closing: make(chan struct{}),
	}

	return ds, nil
}
