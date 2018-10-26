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

	args := &ydbserverrpc.CreateTableArgs{
		TableName:      "abc",
		ColumnFamilies: make([]string, 0),
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
	fmt.Println(openReply.Status)
}
