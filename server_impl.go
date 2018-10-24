package ydb

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"

	"github.com/boylee1111/ydb/ydbserverrpc"
)

const (
	defaultConnectionType  = "tcp"       // Default connection type for RPC
	defaultHostname        = "localhost" // Default hostname
	ydbServerRPCServerName = "YDBServer" // RPC name
)

type ydbServer struct {
	tables   map[string]ydbTable // Table name -> table
	nodeID   uint32              // Store current node ID
	isMaster bool                // Specify whether current node is master
	listener net.Listener        // Node listener
	hostPort string              // Node host and port string
}

type ydbTable struct {
	columnsFamilies map[string][]string  // Column family to qualifiers
	data            map[string]ydbColumn // Row Key -> column
	size            int                  // Table size
	inOpen          bool                 // Table in open
	creationTime    time.Time            // Table create time
}

type ydbColumn struct {
	columns map[string][]string
}

func NewYDBServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (YDBServer, error) {
	portStr := ":" + strconv.Itoa(port)
	listener, err := net.Listen(defaultConnectionType, portStr)
	if err != nil {
		fmt.Println("Failed on Listen: ", err)
		return nil, err
	}

	ydb := &ydbServer{
		tables:   make(map[string]ydbTable),
		nodeID:   nodeID,
		isMaster: masterServerHostPort == "",
		listener: listener,
		hostPort: defaultHostname + portStr,
	}
	rpc.RegisterName(ydbServerRPCServerName, ydbserverrpc.Wrap(ydb))
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return ydb, nil
}

func (ydb *ydbServer) CreateTable(args *ydbserverrpc.CreateTableArgs, reply *ydbserverrpc.CreateTableReply) error {
	fmt.Println("Create Table: ", args.TableName)
	return nil
}

func (ydb *ydbServer) OpenTable(args *ydbserverrpc.OpenTableArgs, reply *ydbserverrpc.OpenTableReply) error {
	return nil
}

func (ydb *ydbServer) CloseTable(args *ydbserverrpc.CloseTableArgs, reply *ydbserverrpc.CloseTableReply) error {
	return nil
}

func (ydb *ydbServer) DestroyTable(args *ydbserverrpc.DestroyTableArgs, reply *ydbserverrpc.DestroyTableReply) error {
	fmt.Println("Destroy Table")
	return nil
}

func (ydb *ydbServer) PutRow(args *ydbserverrpc.PutRowArgs, reply *ydbserverrpc.PutRowReply) error {
	fmt.Println("Put Row")
	return nil
}

func (ydb *ydbServer) GetRow(args *ydbserverrpc.GetRowArgs, reply *ydbserverrpc.GetRowReply) error {
	fmt.Println("Get Row")
	return nil
}

func (ydb *ydbServer) GetRows(args *ydbserverrpc.GetRowsArgs, reply *ydbserverrpc.GetRowsReply) error {
	fmt.Println("Get Rows")
	return nil
}

func (ydb *ydbServer) GetColumnByRow(args *ydbserverrpc.GetColumnByRowArgs, reply *ydbserverrpc.GetColumnByRowReply) error {
	fmt.Println("Get Column By Row")
	return nil
}

func (ydb *ydbServer) MemTableLimit(args *ydbserverrpc.MemTableLimitArgs, reply *ydbserverrpc.MemTableLimitReply) error {
	fmt.Println("Mem Table Limit")
	return nil
}
