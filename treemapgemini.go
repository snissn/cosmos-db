package db

import (
	"fmt"
	"path/filepath"

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

	tdbOpts := treedb.Options{
		Dir: dbPath,
	}

	db, err := treedb.Open(tdbOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open treedb: %w", err)
	}

	return &TreeDBWrapper{db}, nil
}
