package tikv

import (
	dstest "github.com/ipfs/go-datastore/test"
	"testing"
)

func TestSuiteS3(t *testing.T) {
	// run docker-compose up in this repo in order to get a local
	// s3 running on port 4572
	addr := []string{"192.168.1.120:2379"}

	ds, err := NewDatastore(addr, nil)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("basic operations", func(t *testing.T) {
		dstest.SubtestBasicPutGet(t, ds)
	})
	t.Run("not found operations", func(t *testing.T) {
		dstest.SubtestNotFounds(t, ds)
	})
	t.Run("many puts and gets, query", func(t *testing.T) {
		dstest.SubtestManyKeysAndQuery(t, ds)
	})
	t.Run("return sizes", func(t *testing.T) {
		dstest.SubtestReturnSizes(t, ds)
	})
}
