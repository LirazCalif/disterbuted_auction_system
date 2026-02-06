package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"paxos/api"
	"paxos/membership"
	"paxos/logger"
)

func main() {
	// flags passed by docker compose
	serverID := flag.Int("id", 0, "Server ID (0 to n-1)")
	flag.Parse()

	logger.InitLogFilter()

	snapshotPath := "/app/data/snapshot.json"
	if _, err := os.Stat(snapshotPath); err == nil {
		log.Printf("[Server %d] Found snapshot, recovering state.", *serverID)
	}

	// data folder for WAL
	dataDir := "/app/data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Printf("Warning: couldn't create /app/data, trying saving locally: %v", err)
		dataDir = "./data"
		_ = os.MkdirAll(dataDir, 0755)
	}
	walPath := fmt.Sprintf("%s/wal-%d.json", dataDir, *serverID)

	// Etcd endpoint
	etcdAddr := os.Getenv("ETCD_ENDPOINTS")
	if etcdAddr == "" {
		etcdAddr = "etcd:2379"
	}
	etcdEndpoints := strings.Split(etcdAddr, ",")

	currPort := fmt.Sprintf("%d", 50050+*serverID)
	currAddr := fmt.Sprintf("server%d:%s", *serverID, currPort)

	currServer := api.NewPaxosServer(int32(*serverID), snapshotPath, walPath)

	mngr, err := membership.NewMembershipManager(
		int32(*serverID),
		etcdEndpoints,
		currServer.MembershipChange,
	)
	if err != nil {
		log.Fatalf("failed to initiate membership: %v", err)
	}

	// register membership when gRPC server is ready
	go func() {
		time.Sleep(1 * time.Second)
		mngr.StartMembership(context.Background(), currAddr)
	}()

	log.Printf("[Server %d] starting paxos on port %s", *serverID, currPort)

	// start the server
	api.Start(currServer, currPort, etcdEndpoints)
}