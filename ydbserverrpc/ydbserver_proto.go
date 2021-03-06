package ydbserverrpc

import "time"

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

type ServerNode struct {
	HostPort string // The host:port address of the server node.
	NodeID   uint32 // The ID identifying this server node.
}

type RegisterServerArgs struct {
	ServerInfo ServerNode
}

type RegisterServerReply struct {
	Status  Status
	Servers []ServerNode
}

type GetServersArgs struct {
	// Intentionally left empty.
}

type GetServersReply struct {
	Status  Status
	Servers []ServerNode
}

type TableHandle struct {
	TableName      string
	ColumnFamilies []string
	MemTableLimit  int
	CreationTime   time.Time
}

type CreateTableArgs struct {
	TableName      string
	ColumnFamilies []string
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
	QualifiedColumnKey string // Column family:qualifier
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
