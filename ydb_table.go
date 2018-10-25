package ydb

import (
	"sync"
	"time"
)

type tableMeta struct {
	tableName       string    // Table name
	columnsFamilies []string  // Column family
	memTableLimit   int       // Max limit rows for table in memory
	creationTime    time.Time // Table create time
}

type ydbTable struct {
	metadata   tableMeta
	data       map[string]ydbColumn // Row Key -> column data
	dataLocker *sync.RWMutex        // Mutex for data store
	//inOpen     bool                 // Is opened
}

type ydbColumn struct {
	columns map[string]string // Key is column family:qualifier, val is value
}

