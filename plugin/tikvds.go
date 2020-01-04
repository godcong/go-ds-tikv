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
		var workers int
		if v, ok := m["workers"]; ok {
			workersf, ok := v.(float64)
			workers = int(workersf)
			switch {
			case !ok:
				return nil, fmt.Errorf("tikvds: workers not a number")
			case workers <= 0:
				return nil, fmt.Errorf("tikvds: workers <= 0: %f", workersf)
			case float64(workers) != workersf:
				return nil, fmt.Errorf("tikvds: workers is not an integer: %f", workersf)
			}
		}

		return &TiKVConfig{
			cfg: config.Default(),
		}, nil
	}
}

type TiKVConfig struct {
	cfg config.Config
}

func (t *TiKVConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{}
}

func (t *TiKVConfig) Create(path string) (repo.Datastore, error) {
	return tikv.NewDatastore([]string{path}, &tikv.Options{Config: t.cfg})
}
