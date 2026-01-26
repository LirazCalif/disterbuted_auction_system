package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"paxos/src/server/api"
	pb "paxos/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Parse flags passed by Docker-Compose
	serverID := flag.Int("id", 0, "Server ID (0 to n-1)")
	numServers := flag.Int("n", 12, "Total number of servers in cluster")
	flag.Parse()

	// 2. Determine Etcd endpoint (use "etcd:2379" inside Docker)
	etcdAddr := os.Getenv("ETCD_ENDPOINTS")
	if etcdAddr == "" {
		etcdAddr = "etcd:2379" // Fallback for local testing
	}
	etcdEndpoints := strings.Split(etcdAddr, ",")

	peers := make(map[int32]pb.PaxosClient)
	var serverIDs []int32
	var myPort string

	// 3. Build the cluster peer list dynamically
	for i := 0; i < *numServers; i++ {
		id := int32(i)
		serverIDs = append(serverIDs, id)

		// Inside Docker, service names are "server0", "server1", etc.
		// Internal gRPC port is 50050 + ID
		addr := fmt.Sprintf("server%d:%d", id, 50050+id)

		if id == int32(*serverID) {
			myPort = fmt.Sprintf("%d", 50050+id)
			continue
		}

		// Connect to peer via gRPC
		
		var conn *grpc.ClientConn
		var err error
		conn, err = grpc.Dial(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),

			)
		
		if err != nil {
			log.Printf("Warning: Could not connect to peer %d after retries: %v", id, err)
			continue
		}

		peers[id] = pb.NewPaxosClient(conn)
	}
	snapshotPath := "/app/data/snapshot.json"
	if _, err := os.Stat(snapshotPath); err == nil {
        log.Printf("[Server %d] Found snapshot, recovering state.", *serverID)
	}

	log.Printf("Starting Paxos Server %d on internal port %s (Cluster: %d, Etcd: %s)", 
		*serverID, myPort, *numServers, etcdAddr)

	// Start the server
	// Modified Start to accept etcd endpoints if needed, or update Start itself
	api.Start(int32(*serverID), serverIDs, peers, myPort, etcdEndpoints,snapshotPath)


}