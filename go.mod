module github.com/nicholaskh/opendts

go 1.14

require (
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/go-ini/ini v1.62.0
	github.com/go-kratos/kratos v1.0.0
	github.com/golang/protobuf v1.4.3
	github.com/juju/ratelimit v1.0.1
	github.com/prometheus/client_golang v1.11.0
	github.com/segmentio/kafka-go v0.4.17
	github.com/spyzhov/ajson v0.4.2
)

replace github.com/golang/protobuf => github.com/golang/protobuf v1.3.5
