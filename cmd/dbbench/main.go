package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	db "github.com/cosmos/cosmos-db"
)

type phaseResult struct {	Ops       int           `json:"ops"`
	Duration  time.Duration `json:"duration"`
	Throughput float64      `json:"throughput_ops_per_sec"`
}

type rangeResult struct {
	Ranges    int           `json:"ranges"`
	Span      int           `json:"span"`
	Duration  time.Duration `json:"duration"`
	Throughput float64      `json:"throughput_ranges_per_sec"`
}

type backendResult struct {
	Backend   string       `json:"backend"`
	Load      phaseResult  `json:"load"`
	Mixed     phaseResult  `json:"mixed"`
	RangeScan rangeResult  `json:"range_scan"`
	Error     string       `json:"error,omitempty"`
}

func main() {
	var (
		backendsStr  = flag.String("backends", "gomap,goleveldb,pebbledb,memdb", "comma-separated backends to run")
		keys         = flag.Int("keys", 10000, "number of keys to load")
		valueBytes   = flag.Int("value-bytes", 128, "value size in bytes")
		mixedOps     = flag.Int("mixed-ops", 20000, "number of mixed ops (get/set/delete)")
		rangeQueries = flag.Int("range-queries", 200, "number of range queries")
		rangeSpan    = flag.Int("range-span", 100, "number of keys per range")
				seed         = flag.Int64("seed", 1, "rng seed")
				jsonOut      = flag.String("json", "", "optional path to write JSON results")
				cpuProfile   = flag.String("cpuprofile", "", "write cpu profile to file")
			)
			flag.Parse()
		
			if *cpuProfile != "" {
				f, err := os.Create(*cpuProfile)
				if err != nil {
					log.Fatal(err)
				}
				pprof.StartCPUProfile(f)
				defer pprof.StopCPUProfile()
			}
		
				backends := strings.Split(*backendsStr, ",")
		
				rng := rand.New(rand.NewSource(*seed))

	var results []backendResult
	for _, be := range backends {
		be = strings.TrimSpace(be)
		if be == "" {
			continue
		}
		res := runBackend(be, *keys, *valueBytes, *mixedOps, *rangeQueries, *rangeSpan, rng)
		results = append(results, res)
		printResult(res)
	}

	if *jsonOut != "" {
		if err := writeJSON(*jsonOut, results); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("JSON written to %s\n", *jsonOut)
	}
}

func runBackend(backend string, keys, valueBytes, mixedOps, rangeQueries, rangeSpan int, rng *rand.Rand) backendResult {
	res := backendResult{Backend: backend}

	tempDir, err := os.MkdirTemp("", "dbbench-"+backend+"-*")
	if err != nil {
		res.Error = fmt.Sprintf("tempdir: %v", err)
		return res
	}
	defer os.RemoveAll(tempDir)

	dbName := "bench"
	database, err := db.NewDB(dbName, db.BackendType(backend), tempDir)
	if err != nil {
		res.Error = fmt.Sprintf("open db: %v", err)
		return res
	}
	defer database.Close()

	keysArr := make([][]byte, keys)
	for i := 0; i < keys; i++ {
		keysArr[i] = []byte(fmt.Sprintf("k-%08d", i))
	}
	value := make([]byte, valueBytes)
	for i := range value {
		value[i] = byte(rng.Intn(256))
	}

	// Load phase
	start := time.Now()
	for _, k := range keysArr {
		if err := database.Set(k, value); err != nil {
			res.Error = fmt.Sprintf("load set: %v", err)
			return res
		}
	}
	loadDur := time.Since(start)
	res.Load = phaseResult{
		Ops:       keys,
		Duration:  loadDur,
		Throughput: float64(keys) / loadDur.Seconds(),
	}

	// Mixed ops: 50% get, 40% update, 10% delete
	start = time.Now()
	for i := 0; i < mixedOps; i++ {
		k := keysArr[rng.Intn(len(keysArr))]
		switch r := rng.Intn(100); {
		case r < 50:
			if _, err := database.Get(k); err != nil {
				res.Error = fmt.Sprintf("mixed get: %v", err)
				return res
			}
		case r < 90:
			if err := database.Set(k, value); err != nil {
				res.Error = fmt.Sprintf("mixed set: %v", err)
				return res
			}
		default:
			if err := database.Delete(k); err != nil {
				res.Error = fmt.Sprintf("mixed delete: %v", err)
				return res
			}
		}
	}
	mixedDur := time.Since(start)
	res.Mixed = phaseResult{
		Ops:       mixedOps,
		Duration:  mixedDur,
		Throughput: float64(mixedOps) / mixedDur.Seconds(),
	}

	// Range scans
	start = time.Now()
	for i := 0; i < rangeQueries; i++ {
		startIdx := rng.Intn(len(keysArr))
		endIdx := startIdx + rangeSpan
		if endIdx > len(keysArr) {
			endIdx = len(keysArr)
		}
		var startKey []byte
		var endKey []byte
		if startIdx < len(keysArr) {
			startKey = keysArr[startIdx]
		}
		if endIdx < len(keysArr) {
			endKey = keysArr[endIdx]
		}
		it, err := database.Iterator(startKey, endKey)
		if err != nil {
			res.Error = fmt.Sprintf("range iterator: %v", err)
			return res
		}
		for it.Valid() {
			it.Next()
		}
		if err := it.Error(); err != nil {
			res.Error = fmt.Sprintf("range iterator error: %v", err)
			it.Close()
			return res
		}
		it.Close()
	}
	rangeDur := time.Since(start)
	res.RangeScan = rangeResult{
		Ranges:    rangeQueries,
		Span:      rangeSpan,
		Duration:  rangeDur,
		Throughput: float64(rangeQueries) / rangeDur.Seconds(),
	}

	return res
}

func printResult(res backendResult) {
	fmt.Printf("=== %s ===\n", res.Backend)
	if res.Error != "" {
		fmt.Printf("error: %s\n\n", res.Error)
		return
	}
	fmt.Printf("load:   ops=%d dur=%s thr=%.1f ops/s\n", res.Load.Ops, res.Load.Duration, res.Load.Throughput)
	fmt.Printf("mixed:  ops=%d dur=%s thr=%.1f ops/s\n", res.Mixed.Ops, res.Mixed.Duration, res.Mixed.Throughput)
	fmt.Printf("range:  ranges=%d span=%d dur=%s thr=%.1f ranges/s\n\n",
		res.RangeScan.Ranges, res.RangeScan.Span, res.RangeScan.Duration, res.RangeScan.Throughput)
}

func writeJSON(path string, results []backendResult) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
