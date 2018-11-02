package ydbserverrpc

type RemoteYDBServer interface {
	RegisterServer(*RegisterServerArgs, *RegisterServerReply) error
	GetServers(*GetServersArgs, *GetServersReply) error
	CreateTable(*CreateTableArgs, *CreateTableReply) error
	OpenTable(*OpenTableArgs, *OpenTableReply) error
	CloseTable(*CloseTableArgs, *CloseTableReply) error
	DestroyTable(*DestroyTableArgs, *DestroyTableReply) error
	PutRow(*PutRowArgs, *PutRowReply) error
	GetRow(*GetRowArgs, *GetRowReply) error
	GetRows(*GetRowsArgs, *GetRowsReply) error
	GetColumnByRow(*GetColumnByRowArgs, *GetColumnByRowReply) error
	MemTableLimit(*MemTableLimitArgs, *MemTableLimitReply) error
}

type YDBServer struct {
	// Embed all methods into the struct. See the Effective Go section about
	// embedding for more details: golang.org/doc/effective_go.html#embedding
	RemoteYDBServer
}

// Wrap wraps s in a type-safe wrapper struct to ensure that only the desired
// methods are exported to receive RPCs.
func Wrap(s RemoteYDBServer) RemoteYDBServer {
	return &YDBServer{s}
}
