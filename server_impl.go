package ydb

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/boylee1111/ydb/ydbserverrpc"
	"go.etcd.io/bbolt"
)

const (
	defaultConnectionType  = "tcp"       // Default connection type for RPC
	defaultHostname        = "localhost" // Default hostname
	ydbServerRPCServerName = "YDBServer" // RPC name
)

type ydbServer struct {
	meta     serverMeta
	tables   map[string]*ydbTable // Table name -> table
	nodeID   uint32               // Store current node ID
	isMaster bool                 // Specify whether current node is master
	listener net.Listener         // Node listener
	hostPort string               // Node host and port string
	indexDB  *bbolt.DB
}

type serverMeta struct {
	tableNames []string
}

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

func NewYDBServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (YDBServer, error) {
	portStr := ":" + strconv.Itoa(port)
	listener, err := net.Listen(defaultConnectionType, portStr)
	if err != nil {
		fmt.Println("Failed on Listen: ", err)
		return nil, err
	}

	ydb := &ydbServer{
		tables:   make(map[string]*ydbTable),
		nodeID:   nodeID,
		isMaster: masterServerHostPort == "",
		listener: listener,
		hostPort: defaultHostname + portStr,
		indexDB:  db,
	}
	rpc.RegisterName(ydbServerRPCServerName, ydbserverrpc.Wrap(ydb))
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return ydb, nil
}

func (ydb *ydbServer) CreateTable(args *ydbserverrpc.CreateTableArgs, reply *ydbserverrpc.CreateTableReply) error {
	if _, ok := ydb.tables[args.TableName]; !ok {
		reply.Status = ydbserverrpc.TableExist
		return nil
	}

	newTable := &ydbTable{
		data:       make(map[string]ydbColumn),
		dataLocker: new(sync.RWMutex),
		metadata: tableMeta{
			tableName:       args.TableName,
			columnsFamilies: args.ColumnFamilies,
			memTableLimit:   args.MemTableLimit,
			creationTime:    time.Now(),
		},
	}
	ydb.tables[newTable.meta.tableName] = newTable

	reply.Status = ydbserverrpc.OK
	reply.TableHandle = ydbserverrpc.TableHandle{
		TableName:      newTable.meta.tableName,
		ColumnFamilies: newTable.meta.columnsFamilies,
	}

	return nil
}

func (ydb *ydbServer) OpenTable(args *ydbserverrpc.OpenTableArgs, reply *ydbserverrpc.OpenTableReply) error {
	if table, ok := ydb.tables[args.TableName]; ok {
		if table.inOpen {
			reply.Status = ydbserverrpc.TableOpenByOther
			return nil
		} else {
			table.inOpen = true
			reply.Status = ydbserverrpc.OK
			reply.TableHandle = ydbserverrpc.TableHandle{
				TableName:      table.tableName,
				ColumnFamilies: table.columnsFamilies,
			}
			return nil
		}
	}

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) CloseTable(args *ydbserverrpc.CloseTableArgs, reply *ydbserverrpc.CloseTableReply) error {
	if table, ok := ydb.tables[args.TableName]; ok {
		table.inOpen = false
		reply.Status = ydbserverrpc.OK
		return nil
	}

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) DestroyTable(args *ydbserverrpc.DestroyTableArgs, reply *ydbserverrpc.DestroyTableReply) error {
	if table, ok := ydb.tables[args.TableName]; ok {
		if table.inOpen {
			reply.Status = ydbserverrpc.TableOpenByOther
		} else {
			delete(ydb.tables, table.tableName)
			reply.Status = ydbserverrpc.OK
			return nil
		}
	}

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) PutRow(args *ydbserverrpc.PutRowArgs, reply *ydbserverrpc.PutRowReply) error {
	fmt.Println("Put Row")
	// TODO: add record, check mem size
	return nil
}

func (ydb *ydbServer) GetRow(args *ydbserverrpc.GetRowArgs, reply *ydbserverrpc.GetRowReply) error {
	fmt.Println("Get Row")
	// TODO: get record
	return nil
}

func (ydb *ydbServer) GetRows(args *ydbserverrpc.GetRowsArgs, reply *ydbserverrpc.GetRowsReply) error {
	fmt.Println("Get Rows")
	// TODO: get records
	return nil
}

func (ydb *ydbServer) GetColumnByRow(args *ydbserverrpc.GetColumnByRowArgs, reply *ydbserverrpc.GetColumnByRowReply) error {
	fmt.Println("Get Column By Row")
	// TODO: get records
	return nil
}

func (ydb *ydbServer) MemTableLimit(args *ydbserverrpc.MemTableLimitArgs, reply *ydbserverrpc.MemTableLimitReply) error {
	fmt.Println("Mem Table Limit")
	// TODO: update mem limit, check mem size
	return nil
}

func (ydb *ydbServer) formatFilename(table *ydbTable) (string, string) {

}
