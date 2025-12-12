package db

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cast"
	"treedb"
)

// TreeMapGeminiBackend represents the TreeMapGemini backend.
const TreeMapGeminiBackend BackendType = "treemapgemini"

func init() {
	registerDBCreator(TreeMapGeminiBackend, func(name, dir string, opts Options) (DB, error) {
		return NewTreeMapGeminiDB(name, dir, opts)
	}, false)
}

// NewTreeMapGeminiDB creates a new TreeMapGemini database at dir/name.db.
func NewTreeMapGeminiDB(name, dir string, opts Options) (DB, error) {
	dbPath := filepath.Join(dir, name+".db")

	keepRecent := uint64(0) // Default to 0 (aggressive pruning) as it performed better in load tests
	if opts != nil {
		if v := opts.Get("keep_recent"); v != nil {
			keepRecent = cast.ToUint64(v)
		}
	}

	tdbOpts := treedb.Options{
		Dir:        dbPath,
		KeepRecent: keepRecent,
	}

	db, err := treedb.Open(tdbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open treedb: %w", err)
	}

	return &TreeDBWrapper{db}, nil
}
