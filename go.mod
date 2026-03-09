module github.com/cosmos/cosmos-db

go 1.25.5

require (
	github.com/cockroachdb/pebble v1.1.5
	github.com/cosmos/iavl v1.2.6
	github.com/google/btree v1.1.3
	github.com/linxGnu/grocksdb v1.8.15
	github.com/snissn/gomap v0.0.0-20251229170756-b69ad617e4ea
	github.com/spf13/cast v1.10.0
	github.com/stretchr/testify v1.11.1
	// Pinned to this version to avoid bugs in following commits. See https://github.com/cosmos/cosmos-sdk/pull/14952
	github.com/syndtr/goleveldb v1.0.1-0.20220721030215-126854af5e6d
)

require (
	cosmossdk.io/core v1.1.0
	github.com/cosmos/gogoproto v1.7.2
)

require (
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.12.0 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240816210425-c5d0cb0b6fc0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/redact v1.1.6 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20250429170803-42689b6311bb // indirect
	github.com/cosmos/ics23/go v0.10.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/emicklei/dot v1.4.2 // indirect
	github.com/getsentry/sentry-go v0.42.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.19.2 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/crypto v0.12.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// grocksdb stays at v1.8.x in cosmos-db as it should support RocksDB v8.
// the cosmos sdk v2 uses directly store/v2 which uses RocksDB v9 from 0.52+
replace github.com/linxGnu/grocksdb => github.com/linxGnu/grocksdb v1.8.12

retract v1.1.2
