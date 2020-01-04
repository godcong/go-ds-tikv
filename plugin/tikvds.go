package plugin

import (
	"fmt"
	"github.com/tikv/client-go/config"

	tikv "github.com/godcong/go-ds-tikv"
	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
)

var Plugins = []plugin.Plugin{
	&TiKVPlugin{},
}

type TiKVPlugin struct{}

func (t TiKVPlugin) Name() string {
	return "tikv-datastore-plugin"
}

func (t TiKVPlugin) Version() string {
	return "0.0.1"
}

func (t TiKVPlugin) Init() error {
	return nil
}

func (t TiKVPlugin) DatastoreTypeName() string {
	return "tikvds"
}

func (t TiKVPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		addr, ok := m["addr"].([]string)
		if !ok {
			return nil, fmt.Errorf("s3ds: no region specified")
		}

		return &TiKVConfig{
			cfg:  config.Default(),
			addr: addr,
		}, nil
	}
}

type TiKVConfig struct {
	cfg  config.Config
	addr []string
}

func (t *TiKVConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{}
}

func (t *TiKVConfig) Create(path string) (repo.Datastore, error) {
	return tikv.NewDatastore(t.addr, &tikv.Options{Config: t.cfg})
}
