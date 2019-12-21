package tikv

import (
	ds "github.com/ipfs/go-datastore"
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
