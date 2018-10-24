package ydbserverrpc

// Status represents the status of a RPC's reply.
type Status int

const (
	OK            Status = iota + 1 // The RPC was a success.
	TableNotFound                   // The specified table does not exist.
	WrongServer                     // The specified table does not fall in the server's hash range.
	NotReady                        // The servers are still getting ready.
)

type TableHandle struct {
	TableName      string
	ColumnFamilies map[string][]string // family -> qualifiers
}

type CreateTableArgs struct {
	TableName string
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
	UpdatedColumns map[string]map[string]string
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
	Rows   string // TODO: 用啥结构？
}

type GetRowsArgs struct {
	TableName   string
	StartRowKey string
	EndRowKey   string
}

type GetRowsReply struct {
	Status Status
	Rows   string // TODO: 用啥结构？
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
	NewLimitRows int
}

type MemTableLimitReply struct {
	Status Status
}
