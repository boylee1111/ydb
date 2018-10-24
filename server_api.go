package ydb

import "github.com/boylee1111/ydb/ydbserverrpc"

type YDBServer interface {
	CreateTable(*ydbserverrpc.CreateTableArgs, *ydbserverrpc.CreateTableReply) error
	DestroyTable(*ydbserverrpc.DestroyTableArgs, *ydbserverrpc.DestroyTableReply) error
	PutRow(*ydbserverrpc.PutRowArgs, *ydbserverrpc.PutRowReply) error
	GetRow(*ydbserverrpc.GetRowArgs, *ydbserverrpc.GetRowReply) error
	GetRows(*ydbserverrpc.GetRowsArgs, *ydbserverrpc.GetRowsReply) error
	GetColumnByRow(*ydbserverrpc.GetColumnByRowArgs, *ydbserverrpc.GetColumnByRowReply) error
	MemTableLimit(*ydbserverrpc.MemTableLimitArgs, *ydbserverrpc.MemTableLimitReply) error
}
