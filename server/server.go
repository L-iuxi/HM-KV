package main

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"TicketX/internal/kv"
	"TicketX/proto"
)

var peerAddrs = []string{
	"127.0.0.1:50051",
	"127.0.0.1:50052",
	"127.0.0.1:50053",
	"127.0.0.1:50054",
	"127.0.0.1:50055",
}

// 启动单个节点
func startNode(me int) {

	// peers = 所有节点地址（gRPC）
	peers := peerAddrs

	// 每个节点自己的 KvServer + Raft
	kvServer := kv.MakeKVServer(peers, me, "./data")

	port := peerAddrs[me]
	lis, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer()
	rf := kvServer.GetRaft()
	proto.RegisterKvServer(s, kvServer)
	proto.RegisterRaftServer(s, rf)

	fmt.Println("node start at", port)

	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}

func main() {

	for i := 0; i < 5; i++ {
		go startNode(i)
	}

	select {}
}
