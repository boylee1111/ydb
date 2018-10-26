package ydb

import (
	"sync"
	"time"
)

type TableMeta struct {
	TableName       string    // Table name
	ColumnsFamilies []string  // Column family
	MemTableLimit   int       // Max limit rows for table in memory
	CreationTime    time.Time // Table create time
}

type ydbTable struct {
	metadata   TableMeta
	data       map[string]ydbColumn // Row Key -> column data
	dataLocker *sync.RWMutex        // Mutex for data store
	//inOpen     bool                 // Is opened
}

type ydbColumn struct {
	columns map[string]string // Key is column family:qualifier, val is value
}
