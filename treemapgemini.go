package db

// TreeMapGeminiBackend represents the TreeMapGemini backend.
const TreeMapGeminiBackend BackendType = "treemapgemini"

func init() {
	registerDBCreator(TreeMapGeminiBackend, func(name, dir string, opts Options) (DB, error) {
		return NewTreeDB(name, dir, opts)
	}, false)
}

// NewTreeMapGeminiDB creates a new TreeMapGemini database at dir/name.db.
func NewTreeMapGeminiDB(name, dir string, opts Options) (DB, error) {
	return NewTreeDB(name, dir, opts)
}
