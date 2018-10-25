package ydbserverrpc

// Status represents the status of a RPC's reply.
type Status int

const (
	OK               Status = iota + 1 // The RPC was a success.
	TableExist                         // The specified table exist already.
	TableNotFound                      // The specified table does not exist.
	TableOpenByOther                   // Table opened by others
	WrongServer                        // The specified table does not fall in the server's hash range.
	NotReady                           // The servers are still getting ready.
)

type TableHandle struct {
	TableName      string
	ColumnFamilies []string
}

type CreateTableArgs struct {
	TableName      string
	ColumnFamilies []string
	MemTableLimit  int
}

type CreateTableReply struct {
	TableHandle TableHandle
	Status      Status
}

type OpenTableArgs struct {
	TableName string
}

type OpenTableReply struct {
	TableHandle TableHandle
	Status      Status
}

type CloseTableArgs struct {
	TableName string
}

type CloseTableReply struct {
	Status Status
}

type DestroyTableArgs struct {
	TableName string
}

type DestroyTableReply struct {
	Status Status
}

type PutRowArgs struct {
	TableName      string
	RowKey         string
	UpdatedColumns map[string]string // Key is family:qualifier, val is value
}

type PutRowReply struct {
	Status Status
}

type GetRowArgs struct {
	TableName string
	RowKey    string
}

type GetRowReply struct {
	Status Status
	Row    string
}

type GetRowsArgs struct {
	TableName   string
	StartRowKey string
	EndRowKey   string
}

type GetRowsReply struct {
	Status Status
	Rows   map[string]string
}

type GetColumnByRowArgs struct {
	TableName          string
	RowKey             string
	QualifiedColumnKey string
}

type GetColumnByRowReply struct {
	Status Status
	Value  string
}

type MemTableLimitArgs struct {
	TableName    string
	NewLimitRows int
}

type MemTableLimitReply struct {
	Status Status
}
