package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"

	"paxos/src/server/api"
	"paxos/src/server/election"
	"paxos/src/server/paxos"
	pb "paxos/proto"
)

// --- MOCK NETWORK ---
// We wrap the server to add a "Dead" flag for testing failures.
type TestNode struct {
	Server     *api.PaxosServer
	Election   *election.ElectionManager
	CancelFunc context.CancelFunc // To stop the election campaign
	IsDead     bool
	Mu         sync.Mutex
}

// Global map to simulate network status
var nodes = make(map[int32]*TestNode)

// MockClient redirects calls to the internal Go functions
type mockClient struct {
	TargetID int32
}

func (m *mockClient) Prepare(ctx context.Context, req *pb.PrepareRequest, opts ...grpc.CallOption) (*pb.PromiseResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()

	if node.IsDead {
		return nil, fmt.Errorf("connection refused: node %d is dead", m.TargetID)
	}
	// Simulate network delay
	// time.Sleep(10 * time.Millisecond)
	return node.Server.Prepare(ctx, req)
}

func (m *mockClient) Accept(ctx context.Context, req *pb.AcceptRequest, opts ...grpc.CallOption) (*pb.AcceptedResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()

	if node.IsDead {
		return nil, fmt.Errorf("connection refused: node %d is dead", m.TargetID)
	}
	return node.Server.Accept(ctx, req)
}

func (m *mockClient) Commit(ctx context.Context, req *pb.CommitRequest, opts ...grpc.CallOption) (*pb.CommitResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()

	if node.IsDead {
		return nil, fmt.Errorf("connection refused: node %d is dead", m.TargetID)
	}
	return node.Server.Commit(ctx, req)
}

// Mocking GetLogState for SyncLog
func (m *mockClient) GetLogState(ctx context.Context, req *pb.GetLogRequest, opts ...grpc.CallOption) (*pb.GetLogResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()

	if node.IsDead {
		return nil, fmt.Errorf("node %d is dead", m.TargetID)
	}
	return node.Server.GetLogState(ctx, req)
}

func main() {
	log.Println("--- Starting Multi-Paxos + Etcd Failure Test ---")

	serverCount := 3
	
	// 1. Initialize Nodes
	for i := 1; i <= serverCount; i++ {
		id := int32(i)
		
		// Create Election Manager (Real Etcd Connection)
		em, err := election.NewElectionManager(id, []string{"localhost:2379"})
		if err != nil {
			log.Fatalf("Failed to connect to etcd: %v", err)
		}

		// Create Server struct
		// Note: We use empty peers map initially, we fill it below
		srv := &api.PaxosServer{
			ID:            id,
			Peers:         make(map[int32]pb.PaxosClient),
			MultiInstance: paxos.NewMultiPaxosInstance(serverCount),
			Election:      em,
			Mu:            sync.Mutex{},
		}

		// Create Context for Election (so we can cancel it to kill the node)
		ctx, cancel := context.WithCancel(context.Background())
		em.StartCampaign(ctx)

		nodes[id] = &TestNode{
			Server:     srv,
			Election:   em,
			CancelFunc: cancel,
			IsDead:     false,
		}
	}

	// 2. Wire up Peers (Mesh Topology)
	for i := 1; i <= serverCount; i++ {
		me := nodes[int32(i)]
		for j := 1; j <= serverCount; j++ {
			if i == j { continue }
			// Each server gets a mock client pointing to the other node
			me.Server.Peers[int32(j)] = &mockClient{TargetID: int32(j)}
		}
	}

	// 3. Wait for Leader Election (Etcd takes a moment)
	log.Println("Waiting for leader election...")
	time.Sleep(3 * time.Second)

	var leaderID int32
	leaderID = getLeader()
	if leaderID == -1 {
		log.Fatal("No leader elected! Is etcd running?")
	}
	log.Printf("Initial Leader is Server %d", leaderID)

	// --- PHASE 1: Happy Path ---
	log.Println("\n--- Phase 1: Proposing values (Happy Path) ---")
	commands := []string{"SET x=1", "SET y=5"}
	
	for _, cmd := range commands {
		leaderNode := nodes[leaderID]
		log.Printf("Client -> Leader %d: '%s'", leaderID, cmd)
		idx := leaderNode.Server.Propose([]byte(cmd))
		log.Printf("  -> Committed at Index %d", idx)
		time.Sleep(200 * time.Millisecond)
	}

	// --- PHASE 2: KILL THE LEADER ---
	log.Printf("\n--- Phase 2: 💀 KILLING LEADER SERVER %d 💀 ---", leaderID)
	
	// A. Stop Election (Stops Heartbeats -> Lease Expires in ~5s)
	nodes[leaderID].CancelFunc() 
	
	// B. Mark Dead (Stops processing Paxos messages)
	nodes[leaderID].Mu.Lock()
	nodes[leaderID].IsDead = true
	nodes[leaderID].Mu.Unlock()

	log.Println("Leader killed. Waiting for Lease Expiry & New Election (approx 6s)...")
	time.Sleep(7 * time.Second) // Wait for TTL (5s) + buffer

	// --- PHASE 3: RECOVERY ---
	newLeaderID := getLeader()
	if newLeaderID == -1 || newLeaderID == leaderID {
		log.Fatal("Failed to elect a NEW leader!")
	}
	log.Printf("👑 New Leader is Server %d (Took over successfully!)", newLeaderID)

	// --- PHASE 4: Continued Operation ---
	log.Println("\n--- Phase 4: Proposing values to NEW Leader ---")
	newCommands := []string{"ADD x+y", "STORE result"}

	for _, cmd := range newCommands {
		leaderNode := nodes[newLeaderID]
		
		// Force SyncLog manually for the test since we didn't restart the process
		// In a real restart, Start() calls SyncLog().
		// Here, the node was already running, just became leader.
		// NOTE: A robust implementation does this inside StartCampaign or on becoming leader.
		// For this test, we assume state is consistent or we manually trigger:
		// leaderNode.Server.SyncLog() 

		log.Printf("Client -> New Leader %d: '%s'", newLeaderID, cmd)
		idx := leaderNode.Server.Propose([]byte(cmd))
		log.Printf("  -> Committed at Index %d", idx)
		time.Sleep(200 * time.Millisecond)
	}

	// 5. Verify Final State
	log.Println("\n--- Final Log State (Checking Survivors) ---")
	for id, node := range nodes {
		if node.IsDead {
			log.Printf("Server %d: [DEAD]", id)
			continue
		}
		
		log.Printf("Server %d Log:", id)
		// We expect 4 commands total (indices 0, 1, 2, 3)
		for i := int32(0); i < 4; i++ {
			inst := node.Server.MultiInstance.GetInstance(i)
			if inst.IsCommitted {
				log.Printf("  [Idx %d] %s", i, string(inst.CommittedValue))
			} else {
				log.Printf("  [Idx %d] (Empty - REPLICATION FAILED)", i)
			}
		}
	}
}

// Helper to find current leader from our test nodes
func getLeader() int32 {
	for id, node := range nodes {
		if !node.IsDead && node.Server.IsLeader() {
			return id
		}
	}
	return -1
}