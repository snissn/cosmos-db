package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const envTreeDBCosmosDebugVisibility = "TREEDB_COSMOS_DEBUG_VISIBILITY"
const envTreeDBCosmosDebugPrefix = "TREEDB_COSMOS_DEBUG_PREFIX"

var treedbVisibilityOnce sync.Once
var treedbVisibilityEnabled atomic.Bool
var treedbVisibilityPrefix string
var treedbVisibilityMu sync.Mutex

func treedbVisibilityOn() bool {
	treedbVisibilityOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv(envTreeDBCosmosDebugVisibility))
		if raw == "" || raw == "0" || strings.EqualFold(raw, "false") {
			return
		}
		treedbVisibilityEnabled.Store(true)
		treedbVisibilityPrefix = strings.TrimSpace(os.Getenv(envTreeDBCosmosDebugPrefix))
	})
	return treedbVisibilityEnabled.Load()
}

func treedbVisibilityf(format string, args ...any) {
	if !treedbVisibilityOn() {
		return
	}
	line := fmt.Sprintf("treedb-cosmos-visibility %s "+format+"\n", append([]any{time.Now().Format(time.RFC3339Nano)}, args...)...)
	treedbVisibilityMu.Lock()
	_, _ = os.Stderr.WriteString(line)
	treedbVisibilityMu.Unlock()
}

// prefixedIAVLRootVersion returns the parsed root version for a key that ends
// with IAVL root node-key format: 's' + 8-byte version + 4-byte nonce(=1).
func prefixedIAVLRootVersion(key []byte) (version uint64, prefix []byte, ok bool) {
	if len(key) < 13 {
		return 0, nil, false
	}
	tail := key[len(key)-13:]
	if tail[0] != 's' || binary.BigEndian.Uint32(tail[9:13]) != 1 {
		return 0, nil, false
	}
	version = binary.BigEndian.Uint64(tail[1:9])
	prefix = key[:len(key)-13]
	if treedbVisibilityPrefix == "" {
		return version, prefix, true
	}
	if strings.Contains(string(prefix), treedbVisibilityPrefix) {
		return version, prefix, true
	}
	return 0, nil, false
}

func isRootMultiMetaKey(key []byte) bool {
	if bytes.Equal(key, []byte("s/latest")) {
		return true
	}
	if len(key) < 3 || key[0] != 's' || key[1] != '/' {
		return false
	}
	for i := 2; i < len(key); i++ {
		if key[i] < '0' || key[i] > '9' {
			return false
		}
	}
	return true
}

func isPrefixedIAVLVersionRange(start, end []byte) bool {
	if len(start) < 9 || len(end) < 9 || len(start) != len(end) {
		return false
	}
	if start[len(start)-9] != 's' || end[len(end)-9] != 's' {
		return false
	}
	if !bytes.Equal(start[:len(start)-9], end[:len(end)-9]) {
		return false
	}
	startVersion := binary.BigEndian.Uint64(start[len(start)-8:])
	endVersion := binary.BigEndian.Uint64(end[len(end)-8:])
	if startVersion == 0 {
		return false
	}
	if endVersion != uint64(math.MaxInt64) {
		return false
	}
	return true
}
