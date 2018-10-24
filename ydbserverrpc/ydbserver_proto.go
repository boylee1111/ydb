package ydbserverrpc

// Status represents the status of a RPC's reply.
type Status int

const (
	OK          Status = iota + 1 // The RPC was a success.
	KeyNotFound                   // The specified key does not exist.
	WrongServer                   // The specified key does not fall in the server's hash range.
	NotReady                      // The storage servers are still getting ready.
)

type CreateTableArgs struct {
	TableName string
}

type CreateTableReply struct {
	// TODO: Table handle?
	Status Status
}

type OpenTableArgs struct {
	TableName string
}

type OpenTableReply struct {
	// TODO: Table handle
	Status Status
}

type CloseTableArgs struct {
	// TODO: Table handle
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
