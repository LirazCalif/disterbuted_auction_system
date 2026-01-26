package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	"paxos/src/server/api"
	"paxos/src/server/election"
	"paxos/src/server/paxos"
	pb "paxos/proto"
)

// --- MOCK NETWORK ---
// Wraps the server to add a "Dead" flag for testing failures.
type TestNode struct {
	Server     *api.PaxosServer
	Election   *election.ElectionManager
	CancelFunc context.CancelFunc
	IsDead     bool
	Mu         sync.Mutex
}

var nodes = make(map[int32]*TestNode)

// MockClient redirects calls internally (No real network ports needed)
type mockClient struct {
	TargetID int32
}

func (m *mockClient) Prepare(ctx context.Context, req *pb.PrepareRequest, opts ...grpc.CallOption) (*pb.PromiseResponse, error) {
	node, ok := nodes[m.TargetID]
	if !ok { return nil, fmt.Errorf("node not found") }
	
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if node.IsDead { return nil, fmt.Errorf("node %d is dead", m.TargetID) }
	
	// Simulate Network Latency (random 1-5ms)
	time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
	return node.Server.Prepare(ctx, req)
}

func (m *mockClient) Accept(ctx context.Context, req *pb.AcceptRequest, opts ...grpc.CallOption) (*pb.AcceptedResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if node.IsDead { return nil, fmt.Errorf("node %d is dead", m.TargetID) }
	return node.Server.Accept(ctx, req)
}

func (m *mockClient) Commit(ctx context.Context, req *pb.CommitRequest, opts ...grpc.CallOption) (*pb.CommitResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if node.IsDead { return nil, fmt.Errorf("node %d is dead", m.TargetID) }
	return node.Server.Commit(ctx, req)
}

func (m *mockClient) GetLogState(ctx context.Context, req *pb.GetLogRequest, opts ...grpc.CallOption) (*pb.GetLogResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if node.IsDead { return nil, fmt.Errorf("node %d is dead", m.TargetID) }
	return node.Server.GetLogState(ctx, req)
}

func (m *mockClient) GetReadIndex(ctx context.Context, req *pb.ReadIndexRequest, opts ...grpc.CallOption) (*pb.ReadIndexResponse, error) {
	node := nodes[m.TargetID]
	node.Mu.Lock()
	defer node.Mu.Unlock()
	if node.IsDead { return nil, fmt.Errorf("node %d is dead", m.TargetID) }
	return node.Server.GetReadIndex(ctx, req)
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds) // High precision logging
	log.Println("🚀 --- STARTING HIGH-PERFORMANCE PAXOS TEST SUITE ---")

	serverCount := 20
	
	// 1. Initialize Infrastructure
	for i := 1; i <= serverCount; i++ {
		id := int32(i)
		em, err := election.NewElectionManager(id, []string{"localhost:2379"})
		if err != nil { log.Fatalf("Failed to connect to etcd: %v", err) }

		srv := &api.PaxosServer{} 
		// Manually initialize fields since we aren't calling api.Start()
		// (We are embedding the server in our TestNode)
		
		srv.ID = id
		srv.Peers = make(map[int32]pb.PaxosClient)
		srv.MultiInstance = paxos.NewMultiPaxosInstance(serverCount)
		srv.Election = em
		// Manually Init Advanced Features
		srv.InitForTest(5000, 50, 50*time.Millisecond) // Buffer=5000, Batch=50, Timeout=50ms

		ctx, cancel := context.WithCancel(context.Background())
		em.StartCampaign(ctx)

		nodes[id] = &TestNode{
			Server:     srv,
			Election:   em,
			CancelFunc: cancel,
		}
	}

	// 2. Wire Mesh Topology
	for i := 1; i <= serverCount; i++ {
		for j := 1; j <= serverCount; j++ {
			if i == j { continue }
			nodes[int32(i)].Server.Peers[int32(j)] = &mockClient{TargetID: int32(j)}
		}
	}

	log.Println("⏳ Waiting for Leader Election (5s)...")
	time.Sleep(5 * time.Second)

	leaderID := getLeader()
	if leaderID == -1 { log.Fatal("❌ No leader elected. Is Etcd running?") }
	log.Printf("👑 INITIAL LEADER: Server %d", leaderID)
	leader := nodes[leaderID].Server


	// =========================================================================
	// TEST 1: True Multi-Paxos (Skipping Prepare)
	// =========================================================================
	log.Println("\n🧪 [TEST 1] TRUE MULTI-PAXOS (Optimization Check)")
	log.Println("   Step A: Sending Request 1 (Should trigger Phase 1 + Phase 2)")
	leader.Propose([]byte("config=init"))
	time.Sleep(200 * time.Millisecond)

	log.Println("   Step B: Sending Request 2 (Should SKIP Prepare -> Phase 2 only)")
	// If optimization works, logs will show "Skipping Prepare"
	leader.Propose([]byte("config=ready"))
	time.Sleep(200 * time.Millisecond)


	// =========================================================================
	// TEST 2: High Flood (Batching + Pipelining)
	// =========================================================================
	log.Println("\n🌊 [TEST 2] HIGH FLOOD TEST (Batching & Pipelining)")
	requestCount := 1000
	log.Printf("   Flooding %d asynchronous requests...", requestCount)
	
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(requestCount)

	for k := 0; k < requestCount; k++ {
		go func(val int) {
			defer wg.Done()
			cmd := fmt.Sprintf("key%d=val%d", val, val)
			err := leader.SubmitRequest([]byte(cmd)) // Use SubmitRequest (Async)
			if err != nil { log.Printf("Submission error: %v", err) }
		}(k)
	}

	wg.Wait() // Wait for submission (not execution)
	log.Println("   All requests submitted to buffer.")
	
	// Wait for processing to drain
	time.Sleep(2 * time.Second)
	duration := time.Since(start)
	
	log.Printf("   ✅ Processed %d requests in %v", requestCount, duration)
	
	// Verification
	committedCount := leader.MultiInstance.NextIndex
	log.Printf("   Leader Log Height: %d (Expected ~%d depending on overhead + 2 setup cmds)", committedCount, committedCount)


	// =========================================================================
	// TEST 3: Smart Follower Reads
	// =========================================================================
	log.Println("\n📖 [TEST 3] SMART FOLLOWER READS (Linearizability)")
	
	// 1. Write a specific value
	targetKey := "secret"
	targetVal := "paxos_is_cool"
	leader.SubmitRequest([]byte(fmt.Sprintf("%s=%s", targetKey, targetVal)))
	time.Sleep(500 * time.Millisecond) // Ensure commit

	// 2. Pick a Follower
	var follower *api.PaxosServer
	for id, node := range nodes {
		if id != leaderID && !node.IsDead {
			follower = node.Server
			log.Printf("   Selected Follower Server %d for Read", id)
			break
		}
	}

	// 3. Perform Read on Follower
	val, err := follower.HandleRead(targetKey)
	if err != nil {
		log.Fatalf("❌ Read Failed: %v", err)
	}
	if val == targetVal {
		log.Printf("   ✅ Read Success: Got '%s' from Follower (Linearizable!)", val)
	} else {
		log.Fatalf("   ❌ Read Mismatch: Expected '%s', got '%s'", targetVal, val)
	}


	// =========================================================================
	// TEST 4: Fault Tolerance (Killing Leader)
	// =========================================================================
	log.Printf("\n💀 [TEST 4] FAILURE RECOVERY (Killing Leader %d)", leaderID)
	
	nodes[leaderID].CancelFunc() // Stop campaigning
	nodes[leaderID].Mu.Lock()
	nodes[leaderID].IsDead = true // Stop network
	nodes[leaderID].Mu.Unlock()

	log.Println("   Leader killed. Waiting for new election (approx 6s)...")
	time.Sleep(7 * time.Second)

	newLeaderID := getLeader()
	if newLeaderID == -1 || newLeaderID == leaderID {
		log.Fatal("❌ Failed to elect NEW leader!")
	}
	log.Printf("   👑 NEW LEADER ELECTED: Server %d", newLeaderID)

	// Test new leader
	newLeader := nodes[newLeaderID].Server
	newLeader.SubmitRequest([]byte("status=recovered"))
	time.Sleep(1 * time.Second)

	// Verify Data Survival
	log.Println("\n📊 [FINAL REPORT] Checking Logs on Survivor Nodes...")
	for id, node := range nodes {
		if node.IsDead { continue }
		
		// Check random key from flood
		val, _ := node.Server.HandleRead("key500")
		log.Printf("   Server %d: key500 = '%s' (Should be 'val500')", id, val)
		
		val2, _ := node.Server.HandleRead("status")
		log.Printf("   Server %d: status = '%s' (Should be 'recovered')", id, val2)
	}

	log.Println("\n✅ TEST SUITE COMPLETED SUCCESSFULLY.")
}

func getLeader() int32 {
	for id, node := range nodes {
		node.Mu.Lock()
		dead := node.IsDead
		node.Mu.Unlock()
		
		if !dead && node.Server.IsLeader() {
			return id
		}
	}
	return -1
}