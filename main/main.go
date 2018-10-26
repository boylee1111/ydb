package main

import (
	"fmt"
	"net/rpc"

	"github.com/boylee1111/ydb"
	"github.com/boylee1111/ydb/ydbserverrpc"
)

func main() {
	ydb.NewYDBServer("", 1, 8181, 0)

	client, err := rpc.DialHTTP("tcp", "localhost:8181")
	if err != nil {
		fmt.Println(err)
	}

	schema := make([]string, 0)
	schema = append(schema, "Name")
	schema = append(schema, "Address")

	tableName := "abc"
	args := &ydbserverrpc.CreateTableArgs{
		TableName:      "abc",
		ColumnFamilies: schema,
	}
	var reply ydbserverrpc.CreateTableReply
	if err := client.Call("YDBServer.CreateTable", args, &reply); err != nil {
		panic(err)
	}

	openArgs := &ydbserverrpc.OpenTableArgs{
		TableName: "abc",
	}
	var openReply ydbserverrpc.OpenTableReply
	if err := client.Call("YDBServer.OpenTable", openArgs, &openReply); err != nil {
		fmt.Println(err)
	}

	columns := make(map[string]string)
	columns["Name:First Name"] = "Ivan"
	columns["Name:Last Name"] = "Jie"
	putRowArgs := &ydbserverrpc.PutRowArgs{
		TableName:      tableName,
		RowKey:         "testKey",
		UpdatedColumns: columns,
	}
	var putRowReply ydbserverrpc.PutRowReply
	if err := client.Call("YDBServer.PutRow", putRowArgs, &putRowReply); err != nil {
		panic(err)
	}
	fmt.Println("Put first record")

	putRowArgs.RowKey = "testKey2"
	columns["Name:First Name"] = "Huo"
	columns["Name:Last Name"] = "Gun"
	if err := client.Call("YDBServer.PutRow", putRowArgs, &putRowReply); err != nil {
		panic(err)
	}
	fmt.Println("Put second record")

	getRowAgrs := &ydbserverrpc.GetRowArgs{
		TableName: tableName,
		RowKey:    "testKey",
	}
	var getRowReply ydbserverrpc.GetRowReply
	if err := client.Call("YDBServer.GetRow", getRowAgrs, &getRowReply); err != nil {
		panic(err)
	}
	fmt.Println(getRowReply.Row)

	getRowAgrs.RowKey = "testKey2"
	if err := client.Call("YDBServer.GetRow", getRowAgrs, &getRowReply); err != nil {
		panic(err)
	}
	fmt.Println(getRowReply.Row)
}
