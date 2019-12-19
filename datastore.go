package tikv

import (
	logger "github.com/ipfs/go-log"
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

var log = logger.Logger("tikv")
