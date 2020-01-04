package tikv

import (
	"context"
	"errors"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	logger "github.com/ipfs/go-log"
	"github.com/jbenet/goprocess"
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
	t.ds.closeLk.RLock()
	defer t.ds.closeLk.RUnlock()
	if t.ds.closed {
		return nil, ErrClosed
	}
	return t.get(key)
}

func (t *txn) Has(key ds.Key) (exists bool, err error) {
	t.ds.closeLk.RLock()
	defer t.ds.closeLk.RUnlock()
	if t.ds.closed {
		return false, ErrClosed
	}
	return t.has(key)
}

func (t *txn) GetSize(key ds.Key) (size int, err error) {
	t.ds.closeLk.RLock()
	defer t.ds.closeLk.RUnlock()
	if t.ds.closed {
		return -1, ErrClosed
	}

	return t.getSize(key)
}

func (t *txn) Query(q dsq.Query) (dsq.Results, error) {
	t.ds.closeLk.RLock()
	defer t.ds.closeLk.RUnlock()
	if t.ds.closed {
		return nil, ErrClosed
	}
	return t.query(q)
}

func (t *txn) Put(key ds.Key, value []byte) error {
	return t.put(key, value)
}

func (t *txn) Delete(key ds.Key) error {
	t.ds.closeLk.RLock()
	defer t.ds.closeLk.RUnlock()
	if t.ds.closed {
		return ErrClosed
	}

	return t.delete(key)
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
//var _ ds.GCDatastore = (*Datastore)(nil)

var log = logger.Logger("tikv")

func NewDatastore(addr []string, options *Options) (*Datastore, error) {
	if options == nil {
		options = &Options{
			Config: config.Default(),
		}
	}
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
func (d *Datastore) Batch() (ds.Batch, error) {
	tx, _ := d.NewTransaction(false)
	return tx, nil
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
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}

	txn := d.newImplicitTransaction(true)
	defer txn.rollback()

	return txn.get(key)
}

func (d *Datastore) Has(key ds.Key) (exists bool, err error) {
	d.closeLk.RLock()
	defer d.closeLk.RUnlock()
	if d.closed {
		return false, ErrClosed
	}

	txn := d.newImplicitTransaction(true)
	defer txn.rollback()

	return txn.has(key)
}

func (d *Datastore) GetSize(key ds.Key) (size int, err error) {
	panic("ds.Datastore")
}

func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
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

func (d *Datastore) DiskUsage() (uint64, error) {
	return 0, nil
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

func (t *txn) iter(prefix []byte, reverse bool) (it kv.Iterator, e error) {
	if reverse {
		return t.txn.IterReverse(context.Background(), prefix)
	}
	return t.txn.Iter(context.Background(), prefix, nil)
}

func (t *txn) query(q dsq.Query) (dsq.Results, error) {

	//prefetchValues := !q.KeysOnly
	prefix := []byte(q.Prefix)
	reverse := false
	var e error
	// Handle ordering
	if len(q.Orders) > 0 {
		switch q.Orders[0].(type) {
		case dsq.OrderByKey, *dsq.OrderByKey:
		// We order by key by default.
		case dsq.OrderByKeyDescending, *dsq.OrderByKeyDescending:
			// Reverse order by key
			reverse = true
		default:

			// Ok, we have a weird order we can't handle. Let's
			// perform the _base_ query (prefix, filter, etc.), then
			// handle sort/offset/limit later.

			// Skip the stuff we can't apply.
			baseQuery := q
			baseQuery.Limit = 0
			baseQuery.Offset = 0
			baseQuery.Orders = nil

			// perform the base query.
			res, err := t.query(baseQuery)
			if err != nil {
				return nil, err
			}

			// fix the query
			res = dsq.ResultsReplaceQuery(res, q)

			// Remove the parts we've already applied.
			naiveQuery := q
			naiveQuery.Prefix = ""
			naiveQuery.Filters = nil

			// Apply the rest of the query
			return dsq.NaiveQueryApply(naiveQuery, res), nil
		}
	}
	it, e := t.iter(prefix, reverse)
	if e != nil {
		return nil, e
	}
	//it := t.txn.Iter(context.Background(), opt)
	qrb := dsq.NewResultBuilder(q)
	qrb.Process.Go(func(worker goprocess.Process) {
		t.ds.closeLk.RLock()
		closedEarly := false
		defer func() {
			t.ds.closeLk.RUnlock()
			if closedEarly {
				select {
				case qrb.Output <- dsq.Result{
					Error: ErrClosed,
				}:
				case <-qrb.Process.Closing():
				}
			}

		}()
		if t.ds.closed {
			closedEarly = true
			return
		}

		// this iterator is part of an implicit transaction, so when
		// we're done we must discard the transaction. It's safe to
		// discard the txn it because it contains the iterator only.
		if t.implicit {
			defer t.rollback()
		}

		defer it.Close()

		// All iterators must be started by rewinding.
		//it.Rewind()

		// skip to the offset
		for skipped := 0; skipped < q.Offset && it.Valid(); it.Next(context.TODO()) {
			// On the happy path, we have no filters and we can go
			// on our way.
			if len(q.Filters) == 0 {
				skipped++
				continue
			}

			// On the sad path, we need to apply filters before
			// counting the item as "skipped" as the offset comes
			// _after_ the filter.
			//item := it.Value()

			//matches := true
			//check := func(value []byte) error {
			//	e := dsq.Entry{
			//		Key:   string(it.Key()),
			//		Value: value,
			//		Size:  len(value), // this function is basically free
			//	}

			// Only calculate expirations if we need them.
			//if q.ReturnExpirations {
			//	e.Expiration = expires(item)
			//}
			//matches = filter(q.Filters, e)
			//return nil
			//}

			// Maybe check with the value, only if we need it.
			//var err error
			//if q.KeysOnly {
			//	err = check(nil)
			//} else {
			//	err = item.Value(check)
			//}

			//if err != nil {
			//	select {
			//	case qrb.Output <- dsq.Result{Error: err}:
			//	case <-t.ds.closing: // datastore closing.
			//		closedEarly = true
			//		return
			//	case <-worker.Closing(): // client told us to close early
			//		return
			//	}
			//}
			//if !matches {
			//	skipped++
			//}
		}

		for sent := 0; (q.Limit <= 0 || sent < q.Limit) && it.Valid(); it.Next(context.TODO()) {
			val := it.Value()
			e := dsq.Entry{Key: string(it.Key())}

			// Maybe get the value
			var result dsq.Result
			if !q.KeysOnly {
				var b []byte
				copy(b, val)
				e.Value = b
				e.Size = len(b)
				result = dsq.Result{Entry: e}
			} else {
				e.Size = len(it.Value())
				result = dsq.Result{Entry: e}
			}

			//if q.ReturnExpirations {
			//	result.Expiration = expires(item)
			//}

			// Finally, filter it (unless we're dealing with an error).
			if result.Error == nil && filter(q.Filters, e) {
				continue
			}

			select {
			case qrb.Output <- result:
				sent++
			case <-t.ds.closing: // datastore closing.
				closedEarly = true
				return
			case <-worker.Closing(): // client told us to close early
				return
			}
		}
	})

	go qrb.Process.CloseAfterChildren() //nolint

	return qrb.Results(), nil
}

func (t *txn) has(key ds.Key) (bool, error) {
	_, err := t.txn.Get(context.TODO(), key.Bytes())
	switch err {
	case kv.ErrNotExist:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}
func (t *txn) getSize(key ds.Key) (int, error) {
	val, err := t.txn.Get(context.TODO(), key.Bytes())
	switch err {
	case nil:
		return len(val), nil
	case kv.ErrNotExist:
		return -1, ds.ErrNotFound
	default:
		return -1, err
	}
}
func (t *txn) delete(key ds.Key) error {
	return t.txn.Delete(key.Bytes())
}

// filter returns _true_ if we should filter (skip) the entry
func filter(filters []dsq.Filter, entry dsq.Entry) bool {
	for _, f := range filters {
		if !f.Filter(entry) {
			return true
		}
	}
	return false
}

//func expires(item *badger.Item) time.Time {
//	return time.Unix(int64(item.ExpiresAt()), 0)
//}
