package ydb

import (
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
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
	defaultMemTableLimit   = 9000        // Default memory table row limit
)

type byNodeID []ydbserverrpc.ServerNode // Definition for Server node

func (s byNodeID) Len() int {
	return len(s)
}

func (s byNodeID) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byNodeID) Less(i, j int) bool {
	return s[i].NodeID < s[j].NodeID
}

type ydbServer struct {
	meta            serverMeta
	tables          map[string]*ydbTable      // Table name -> table
	nodeID          uint32                    // Store current node ID
	numNodes        int                       // Number of nodes
	isMaster        bool                      // Specify whether current node is master
	listener        net.Listener              // Node listener
	hostPort        string                    // Node host and port string
	indexDB         *bbolt.DB                 // Bold db object
	registerLocker  *sync.RWMutex             // Mutex used for registering server
	nodes           []ydbserverrpc.ServerNode // List of all nodes (master and slaves)
	registeredCount int                       // Current registered node
}

type serverMeta struct {
	tableNames []string
}

func NewYDBServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (YDBServer, error) {
	portStr := ":" + strconv.Itoa(port)
	fmt.Println("portStr is " + portStr)
	listener, err := net.Listen(defaultConnectionType, portStr)
	if err != nil {
		fmt.Println("Failed on Listen: ", err)
		return nil, err
	}

	dbName := "index_db" + fmt.Sprint(nodeID)
	fmt.Println("Start connecting db " + dbName)
	db, err := bbolt.Open(dbName, 0666, nil)
	fmt.Println("DB connected.")

	ydb := &ydbServer{
		tables:          make(map[string]*ydbTable),
		nodeID:          nodeID,
		numNodes:        numNodes,
		isMaster:        masterServerHostPort == "",
		listener:        listener,
		hostPort:        defaultHostname + portStr,
		indexDB:         db,
		registerLocker:  new(sync.RWMutex),
		nodes:           make([]ydbserverrpc.ServerNode, numNodes),
		registeredCount: 0,
	}
	rpc.RegisterName(ydbServerRPCServerName, ydbserverrpc.Wrap(ydb))
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	fmt.Println("Start serving...")

	// Call master to join if it's from a slave server
	calleeHostport := masterServerHostPort
	if ydb.isMaster {
		calleeHostport = ydb.hostPort
	}

	// Keep dial HTTP until connected
	var client *rpc.Client
	for {
		fmt.Println("callee host port is " + calleeHostport)
		client, err = rpc.DialHTTP(defaultConnectionType, calleeHostport)
		if err == nil {
			defer client.Close()
			break
		}
		fmt.Println(err)
		time.Sleep(time.Duration(1) * time.Second)
	}

	for {
		args := ydbserverrpc.RegisterServerArgs{
			ServerInfo: ydbserverrpc.ServerNode{
				HostPort: ydb.hostPort,
				NodeID:   ydb.nodeID,
			},
		}
		var reply ydbserverrpc.RegisterServerReply
		err = client.Call("YDBServer.RegisterServer", args, &reply)
		if err != nil {
			fmt.Println(err)
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}

		// Server will ready while receiving OK with all nodes
		if reply.Status == ydbserverrpc.OK {
			ydb.nodes = reply.Servers
			ydb.numNodes = len(ydb.nodes)
			break
		}
		fmt.Println("Server not ready, try after 1 seconds.")
		time.Sleep(time.Duration(1) * time.Second)
	}

	return ydb, nil
}

func (ydb *ydbServer) RegisterServer(args *ydbserverrpc.RegisterServerArgs, reply *ydbserverrpc.RegisterServerReply) error {
	if !ydb.isMaster {
		return errors.New("Cannot register server on slave node.")
	}

	ydb.registerLocker.Lock()
	defer ydb.registerLocker.Unlock()

	// Check whether the server is registered
	isRegistered := false
	for _, node := range ydb.nodes {
		if args.ServerInfo.NodeID == node.NodeID {
			isRegistered = true
		}
	}

	if !isRegistered {
		ydb.nodes[ydb.registeredCount] = args.ServerInfo
		ydb.registeredCount++
	}

	// Generate server list if all nodes ready
	if ydb.registeredCount == ydb.numNodes {
		sort.Sort(byNodeID(ydb.nodes))
		reply.Status = ydbserverrpc.OK
		reply.Servers = ydb.nodes
	} else {
		reply.Status = ydbserverrpc.NotReady
	}

	return nil
}

func (ydb *ydbServer) GetServers(args *ydbserverrpc.GetServersArgs, reply *ydbserverrpc.GetServersReply) error {
	ydb.registerLocker.Lock()
	defer ydb.registerLocker.Unlock()

	if ydb.numNodes != len(ydb.nodes) {
		reply.Status = ydbserverrpc.NotReady
	} else {
		sort.Sort(byNodeID(ydb.nodes))
		reply.Status = ydbserverrpc.OK
		reply.Servers = ydb.nodes
	}

	return nil
}

func (ydb *ydbServer) CreateTable(args *ydbserverrpc.CreateTableArgs, reply *ydbserverrpc.CreateTableReply) error {
	// Check existence of given table name
	if _, ok := ydb.tables[args.TableName]; ok {
		reply.Status = ydbserverrpc.TableExist
		return nil
	}
	if ydb.isTableExistOnDisk(args.TableName) {
		reply.Status = ydbserverrpc.TableExist;
		return nil
	}

	// Create and serialize metadata to file
	tableMetaFilename, tableDataFilename := formatFilename(args.TableName)
	metadata := TableMeta{
		TableName:       args.TableName,
		ColumnsFamilies: args.ColumnFamilies,
		MemTableLimit:   defaultMemTableLimit,
		CreationTime:    time.Now(),
	}
	if err := writeGob(tableMetaFilename, metadata); err != nil {
		return err
	}

	os.Create(tableDataFilename)

	reply.Status = ydbserverrpc.OK
	reply.TableHandle = ydbserverrpc.TableHandle{
		TableName:      metadata.TableName,
		ColumnFamilies: metadata.ColumnsFamilies,
	}

	return nil
}

func (ydb *ydbServer) OpenTable(args *ydbserverrpc.OpenTableArgs, reply *ydbserverrpc.OpenTableReply) error {
	if _, ok := ydb.tables[args.TableName]; ok {
		reply.Status = ydbserverrpc.TableOpenByOther
		return nil
	}
	if !ydb.isTableExistOnDisk(args.TableName) {
		reply.Status = ydbserverrpc.TableNotFound
		return nil
	}

	tableMetaFilename, _ := formatFilename(args.TableName)
	// Recovery metadata
	var metadata = new(TableMeta)
	if err := readGob(tableMetaFilename, metadata); err != nil {
		return err
	}
	dataStore := make(map[string]YDBColumn)

	ydb.tables[metadata.TableName] = &ydbTable{
		metadata:   *metadata,
		data:       dataStore,
		dataLocker: new(sync.RWMutex),
	}
	table := ydb.tables[metadata.TableName]
	table.recover()
	reply.Status = ydbserverrpc.OK
	reply.TableHandle = ydbserverrpc.TableHandle{
		TableName:      metadata.TableName,
		ColumnFamilies: metadata.ColumnsFamilies,
		MemTableLimit:  metadata.MemTableLimit,
		CreationTime:   metadata.CreationTime,
	}
	return nil
}

func (ydb *ydbServer) CloseTable(args *ydbserverrpc.CloseTableArgs, reply *ydbserverrpc.CloseTableReply) error {
	tableMetaFilename, _ := formatFilename(args.TableName)
	if table, ok := ydb.tables[args.TableName]; ok {
		if err := os.Remove(tableMetaFilename); err != nil {
			return err
		}
		writeGob(tableMetaFilename, table.metadata)

		delete(ydb.tables, args.TableName)
		return nil
	}

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) DestroyTable(args *ydbserverrpc.DestroyTableArgs, reply *ydbserverrpc.DestroyTableReply) error {
	if _, ok := ydb.tables[args.TableName]; ok {
		reply.Status = ydbserverrpc.TableOpenByOther
		return nil
	}
	if !ydb.isTableExistOnDisk(args.TableName) {
		reply.Status = ydbserverrpc.TableNotFound
		return nil
	}
	tableMetaFilename, tableDataFilename := formatFilename(args.TableName)
	if err := os.Remove(tableMetaFilename); err != nil {
		return err
	}
	if err := os.Remove(tableDataFilename); err != nil {
		return err
	}
	tableWAL := tableWALName(args.TableName)

	if err := os.Remove(tableWAL); err != nil {
		return err
	}

	ydb.indexDB.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket([]byte(args.TableName))
	})
	reply.Status = ydbserverrpc.OK
	return nil
}

func (ydb *ydbServer) PutRow(args *ydbserverrpc.PutRowArgs, reply *ydbserverrpc.PutRowReply) error {
	if table, ok := ydb.tables[args.TableName]; ok {
		table.PutRow(ydb, args.RowKey, args.UpdatedColumns)

		reply.Status = ydbserverrpc.OK
		return nil
	} // TODO: add record, check mem size

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) GetRow(args *ydbserverrpc.GetRowArgs, reply *ydbserverrpc.GetRowReply) error {
	if table, ok := ydb.tables[args.TableName]; ok {
		value := table.GetRow(ydb, args.RowKey)

		reply.Status = ydbserverrpc.OK
		reply.Row = value
		return nil
	} // TODO: add record, check mem size

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) GetRows(args *ydbserverrpc.GetRowsArgs, reply *ydbserverrpc.GetRowsReply) error {
	if table, ok := ydb.tables[args.TableName]; ok {
		values := table.GetRows(ydb, args.StartRowKey, args.EndRowKey)

		reply.Status = ydbserverrpc.OK
		reply.Rows = values
		return nil
	}

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) GetColumnByRow(args *ydbserverrpc.GetColumnByRowArgs, reply *ydbserverrpc.GetColumnByRowReply) error {
	if table, ok := ydb.tables[args.TableName]; ok {
		value := table.GetColumnByRow(ydb, args.RowKey, args.QualifiedColumnKey)

		reply.Status = ydbserverrpc.OK
		reply.Value = value
		return nil
	}

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func (ydb *ydbServer) MemTableLimit(args *ydbserverrpc.MemTableLimitArgs, reply *ydbserverrpc.MemTableLimitReply) error {
	tableMetaFilename, _ := formatFilename(args.TableName)
	if table, ok := ydb.tables[args.TableName]; ok {
		tableMeta := table.metadata
		tableMeta.MemTableLimit = args.NewLimitRows

		if err := os.Remove(tableMetaFilename); err != nil {
			return err
		}
		writeGob(tableMetaFilename, table.metadata)

		reply.Status = ydbserverrpc.OK
		return nil
	}
	if ydb.isTableExistOnDisk(args.TableName) {
		var tableMeta = new(TableMeta)
		readGob(tableMetaFilename, tableMeta)

		if err := os.Remove(tableMetaFilename); err != nil {
			return err
		}
		writeGob(tableMetaFilename, tableMeta)

		reply.Status = ydbserverrpc.OK
		return nil
	}

	reply.Status = ydbserverrpc.TableNotFound
	return nil
}

func tableWALName(tableName string) string {
	return "./" + tableName + ".wal"
}

func formatFilename(tableName string) (string, string) {
	return "./" + tableName + ".meta", "./" + tableName + ".ydb"
}

func (ydb *ydbServer) isTableExistOnDisk(tableName string) bool {
	tableMetaFilename, tableDataFilename := formatFilename(tableName)
	if _, err := os.Stat(tableMetaFilename); os.IsNotExist(err) {
		return false
	}
	if _, err := os.Stat(tableDataFilename); os.IsNotExist(err) {
		return false
	}
	return true
}

func writeGob(filePath string, object interface{}) error {
	file, err := os.Create(filePath)
	if err == nil {
		encoder := gob.NewEncoder(file)
		encoder.Encode(object)
	}
	file.Close()
	return err
}

func readGob(filePath string, object interface{}) error {
	file, err := os.Open(filePath)
	if err == nil {
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(object)
	}
	file.Close()
	return err
}
