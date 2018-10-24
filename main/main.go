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

	args := &ydbserverrpc.CreateTableArgs{TableName: "abc table"}
	var reply ydbserverrpc.CreateTableReply
	if err := client.Call("YDBServer.CreateTable", args, &reply); err != nil {
		fmt.Println(err)
	}
}
