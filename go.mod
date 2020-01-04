module github.com/godcong/go-ds-tikv

go 1.13

require (
	github.com/coreos/etcd v3.3.18+incompatible // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/ipfs/go-datastore v0.3.1
	github.com/ipfs/go-ipfs v0.4.22
	github.com/ipfs/go-log v1.0.0
	github.com/jbenet/goprocess v0.1.3
	github.com/pingcap/check v0.0.0-20191216031241-8a5a85928f12 // indirect
	github.com/pingcap/goleveldb v0.0.0-20191031114657-7683883cfb36 // indirect
	github.com/pingcap/kvproto v0.0.0-20191217072959-393e6c0fd4b7 // indirect
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/tikv/client-go v0.0.0-20191220031903-b252c0420df4
	golang.org/x/net v0.0.0-20191209160850-c0dbc17a3553 // indirect
	google.golang.org/grpc v1.26.0 // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
)

replace github.com/go-critic/go-critic v0.0.0-20181204210945-ee9bf5809ead => github.com/go-critic/go-critic v0.4.0

replace github.com/golangci/golangci-lint v1.16.1-0.20190425135923-692dacb773b7 => github.com/golangci/golangci-lint v1.21.0
