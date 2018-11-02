package main

import (
	"fmt"
	"github.com/boylee1111/ydb"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]
	if len(args) < 4 {
		fmt.Println("Run parameters number.")
		return
	}

	masterHostPort := args[0]
	numNodes, _ := strconv.Atoi(args[1])
	port, _ := strconv.Atoi(args[2])
	nodeId := ydb.StoreHash(args[3])

	fmt.Println("Starting server...")
	ydb.NewYDBServer(masterHostPort, numNodes, port, nodeId)
	fmt.Println("Server started.")
}
