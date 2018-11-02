package ydb

import (
	"errors"
	"net/rpc"
	"time"

	"github.com/boylee1111/ydb/ydbserverrpc"
)

type apiRouter struct {
	nodes          []ydbserverrpc.ServerNode // All server nodes
	nodeRpcClients map[uint32]*rpc.Client    // All corresponding rpc clients
}

func NewRoutingServer(address string) (*apiRouter, error) { // Using localhost:8181 as default
	router := &apiRouter{nodes: nil,
		nodeRpcClients: make(map[uint32]*rpc.Client),
	}

	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return nil, errors.New("Error bin dialing http.")
	}
	for {
		args := &ydbserverrpc.GetServersArgs{}
		var reply ydbserverrpc.GetServersReply
		err := client.Call("YDBServer.GetServers", args, &reply)
		if err == nil && reply.Status == ydbserverrpc.OK {
			router.nodes = reply.Servers
			break
		}
		time.Sleep(time.Duration(1) * time.Second)
	}

	return router, nil
}

func (router *apiRouter) getRoutingServer(key string) (*rpc.Client, error) {
	keyHash := StoreHash(key)
	selectNode := router.nodes[0]
	for _, node := range router.nodes {
		if keyHash >= node.NodeID {
			selectNode = node
			break
		}
	}

	if _, ok := router.nodeRpcClients[selectNode.NodeID]; !ok {
		if client, err := rpc.DialHTTP("tcp", selectNode.HostPort); err != nil {
			router.nodeRpcClients[selectNode.NodeID] = client
		} else {
			return nil, errors.New("Error for dialing selected node.")
		}
	}

	return router.nodeRpcClients[selectNode.NodeID], nil
}
