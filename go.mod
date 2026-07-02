module github.com/cosmos/cosmos-db

go 1.26

require (
	github.com/cockroachdb/pebble v1.1.5
	github.com/google/btree v1.1.3
	github.com/linxGnu/grocksdb v1.8.12
	github.com/snissn/gomap v0.6.2-0.20260702024414-1afe86c1cbc0
	github.com/spf13/cast v1.8.0
	github.com/stretchr/testify v1.11.1
	// Pinned to this version to avoid bugs in following commits. See https://github.com/cosmos/cosmos-sdk/pull/14952
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
)

require github.com/cosmos/gogoproto v1.7.2

require (
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/errors v1.11.3 // indirect
	github.com/cockroachdb/fifo v0.0.0-20240606204812-0bbfbd93a7ce // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20230807174530-cc333fc44b06 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.15.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/snissn/compress v1.18.2-snissn.0.0.20260506201017-87fb149e4721 // indirect
	github.com/snissn/go-crc32-asm v0.0.0-20260522204125-08945951423a // indirect
	github.com/tidwall/btree v1.8.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20230626212559-97b1e661b5df // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto v0.0.0-20250804133106-a7a43d27e69b // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// grocksdb stays at v1.8.x in cosmos-db as it should support RocksDB v8.
// the cosmos sdk v2 uses directly store/v2 which uses RocksDB v9 from 0.52+
replace github.com/linxGnu/grocksdb => github.com/linxGnu/grocksdb v1.8.12

retract v1.1.2
