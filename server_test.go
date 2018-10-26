package ydb

import (
	"fmt"
	"github.com/boylee1111/ydb/ydbserverrpc"
	"github.com/phayes/freeport"
	"math/rand"
	"net/rpc"
	"strconv"
	"testing"
	"time"
)

const (
	NRange    = 5000
	N         = 10000
	tableName = "testTable"
)

func TestYdbServer_GetRow(t *testing.T) {
	client := serverStartup()
	putRowRecords(client, N)

	start := time.Now()
	getRowRecords(client, N)
	end := time.Now()
	diff := end.Sub(start)
	fmt.Printf("Test GetRow for %d times, take %v.\n", N, diff)

	serverCloseAndCleanup(client)
}

func TestYdbServer_PutRow(t *testing.T) {
	client := serverStartup()

	start := time.Now()
	putRowRecords(client, N)
	end := time.Now()
	diff := end.Sub(start)
	fmt.Printf("Test PutRow for %d times, take %v.\n", N, diff)

	serverCloseAndCleanup(client)
}

func TestYdbServer_GetRows(t *testing.T) {
	client := serverStartup()
	putRowRecords(client, N)

	start := time.Now()
	getRowsRecords(client, N)
	end := time.Now()
	diff := end.Sub(start)

	fmt.Printf("Test GetRows for %d times, take %v.\n", N, diff)

	serverCloseAndCleanup(client)
}

func TestYdbServer_GetColumnByRow(t *testing.T) {
	client := serverStartup()
	putRowRecords(client, N)

	start := time.Now()
	getColumnByRowRecords(client, N)
	end := time.Now()
	diff := end.Sub(start)

	fmt.Printf("Test GetColumnByRow for %d times, take %v.\n", N, diff)

	serverCloseAndCleanup(client)
}

func serverStartup() *rpc.Client {
	port, err := freeport.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}

	NewYDBServer("", 1, port, 0)
	fmt.Println("localhost:" + strconv.Itoa(port))
	client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
	}

	schema := make([]string, 0)
	schema = append(schema, "Name")
	schema = append(schema, "Address")
	createTableArgs := &ydbserverrpc.CreateTableArgs{
		TableName:      tableName,
		ColumnFamilies: schema,
	}
	var createTableReply ydbserverrpc.CreateTableReply
	if err := client.Call("YDBServer.CreateTable", createTableArgs, &createTableReply); err != nil {
		panic(err)
	}
	openArgs := &ydbserverrpc.OpenTableArgs{
		TableName: tableName,
	}
	var openReply ydbserverrpc.OpenTableReply
	if err := client.Call("YDBServer.OpenTable", openArgs, &openReply); err != nil {
		fmt.Println(err)
	}

	return client
}

func serverCloseAndCleanup(client *rpc.Client) {
	closeTableArgs := &ydbserverrpc.CloseTableArgs{
		TableName: tableName,
	}
	var closeTableReply ydbserverrpc.CloseTableReply
	if err := client.Call("YDBServer.CloseTable", closeTableArgs, &closeTableReply); err != nil {
		panic(err)
	}

	destroyArgs := ydbserverrpc.DestroyTableArgs{
		TableName: tableName,
	}
	var destroyReply ydbserverrpc.DestroyTableReply
	if err := client.Call("YDBServer.DestroyTable", destroyArgs, &destroyReply); err != nil {
		panic(err)
	}
}

func getRowRecords(client *rpc.Client, N int) {
	for i := 0; i < N; i++ {
		getRowArgs := &ydbserverrpc.GetRowArgs{
			TableName: tableName,
			RowKey:    strconv.Itoa(rand.Intn(NRange)),
		}
		var getRowReply ydbserverrpc.GetRowReply
		if err := client.Call("YDBServer.GetRow", getRowArgs, &getRowReply); err != nil {
			panic(err)
		}
	}
}

func putRowRecords(client *rpc.Client, N int) {
	for i := 0; i < N; i++ {
		columns := make(map[string]string)
		columns["Name:First Name"] = "First"
		columns["Name:Last Name"] = "Last"
		putRowArgs := &ydbserverrpc.PutRowArgs{
			TableName:      tableName,
			RowKey:         strconv.Itoa(rand.Intn(NRange)),
			UpdatedColumns: columns,
		}
		var putRowReply ydbserverrpc.PutRowReply
		if err := client.Call("YDBServer.PutRow", putRowArgs, &putRowReply); err != nil {
			panic(err)
		}
	}
}

func getRowsRecords(client *rpc.Client, N int) {
	for i := 0; i < N; i++ {
		rand1, rand2 := rand.Intn(NRange), rand.Intn(NRange)
		getRowsArgs := &ydbserverrpc.GetRowsArgs{
			TableName:   tableName,
			StartRowKey: strconv.Itoa(min(rand1, rand2)),
			EndRowKey:   strconv.Itoa(max(rand1, rand2)),
		}
		var getRowsReply ydbserverrpc.GetRowsReply
		if err := client.Call("YDBServer.GetRows", getRowsArgs, &getRowsReply); err != nil {
			panic(err)
		}
	}
}

func getColumnByRowRecords(client *rpc.Client, N int) {
	for i := 0; i < N; i++ {
		getColumnByRowArgs := &ydbserverrpc.GetColumnByRowArgs{
			TableName:          tableName,
			RowKey:             strconv.Itoa(rand.Intn(NRange)),
			QualifiedColumnKey: "Name:Last Name",
		}
		var getColumnByRowReply ydbserverrpc.GetColumnByRowReply
		if err := client.Call("YDBServer.GetColumnByRow", getColumnByRowArgs, &getColumnByRowReply); err != nil {
			panic(err)
		}
	}
}

func min(num1, num2 int) int {
	if num1 < num2 {
		return num1
	}
	return num2
}

func max(num1, num2 int) int {
	if num1 > num2 {
		return num1
	}
	return num2
}
