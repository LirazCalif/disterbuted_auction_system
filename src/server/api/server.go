package api

import (
	"context"
	"log"
	"net"
	"sync"
	"time"
	"fmt"
	"strings"
	"bytes"

	pb "paxos/proto"
	"paxos/src/server/paxos"
	"paxos/src/server/election"
	"paxos/src/server/state"

	"google.golang.org/grpc"
)

var proposalCounter int64
var proposalMu sync.Mutex

type PaxosServer struct {
	pb.UnimplementedPaxosServer
	ID       int32
	Peers    map[int32]pb.PaxosClient
	MultiInstance *paxos.MultiPaxosInstance
	Election      *election.ElectionManager
	Mu       sync.Mutex

	//smart read adding
	LastApplied int32
	kvStore map[string]string
	applyCond *sync.Cond

	//batches fileds
	requestChan  chan []byte
	batchSize    int
	batchTimeout time.Duration

	//pipeline field
	pipelineLimit chan struct{}

	//state machine
	AuctionSM     *state.AuctionStateMachine

	//snapshots
	SnapshotPath  string

}

// PaxosMessage is used for internal messaging or non-gRPC transport
type PaxosMessage struct {
	Type           string
	InstanceID     int32
	ProposalNumber int64
	Value          []byte
	SenderID       int32
	AcceptedValue  []byte
}

// Prepare: handles a paxos Prepare request. Follower
func (s *PaxosServer) Prepare(
	ctx context.Context,
	req *pb.PrepareRequest,
) (*pb.PromiseResponse, error) {

	s.Mu.Lock()
	defer s.Mu.Unlock()

	inst := s.MultiInstance.GetInstance(req.InstanceId) 
    
    resp := inst.Prepare_Instance(req.ProposalId)

	log.Printf("[Server %d] Prepare from %d proposal=%d promised=%v",
		s.ID, req.SenderId, req.ProposalId, resp.Promised)

	return resp, nil
}

// Accept: handles a paxos Accept request. Follower
func (s *PaxosServer) Accept(
	ctx context.Context,
	req *pb.AcceptRequest,
) (*pb.AcceptedResponse, error) {

	s.Mu.Lock()
	defer s.Mu.Unlock()

	inst := s.MultiInstance.GetInstance(req.InstanceId)
	resp := inst.Accept_Instance(req.ProposalId, req.Value)


	log.Printf("[Server %d] Accept from %d proposal=%d accepted=%v",
		s.ID, req.SenderId, req.ProposalId, resp.Accepted)

	return resp, nil
}

// Start: starts the paxos gRPC server on the given port.
func Start(
	id int32,
	serverIDs []int32,
	peers map[int32]pb.PaxosClient,
	port string,
	etcdEndpoints []string,
	snapshotPath string,
) {
	// Convert int to int32 for internal storage
	
	elect_manager, err := election.NewElectionManager(id, etcdEndpoints)
    if err != nil {
        log.Fatalf("Failed to init election manager: %v", err)
    }


	server := &PaxosServer{
		ID:       id,
		Peers:    peers,
		MultiInstance: paxos.NewMultiPaxosInstance(len(serverIDs)),
		Election: elect_manager,
		LastApplied: -1,
		kvStore:     make(map[string]string),

		requestChan:  make(chan []byte, 5000),
		batchSize:    100,
		batchTimeout: 50 * time.Millisecond,

		pipelineLimit: make(chan struct{}, 50),
		SnapshotPath:  snapshotPath,
		
	}

	//build the state machine
	server.BuildAuctionStateMachine()

	if err := server.AuctionSM.LoadSnapshot(server.SnapshotPath); err == nil {
        log.Printf("[Server %d] Recovery: State loaded from %s", id, server.SnapshotPath)
		server.LastApplied = int32(server.AuctionSM.LastAppliedIdx)
	}

	//start the REST API
	numericPort := 8080 + int(id) 
	apiPort := fmt.Sprintf("%d", numericPort)
	server.StartAuctionInterface(apiPort)


	server.applyCond = sync.NewCond(&server.Mu)
	
	go server.processBatches()

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPaxosServer(grpcServer, server)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("Paxos server %d started on %s", id, port)

	go func() {
        // Run every 60 seconds to clean up RAM
        ticker := time.NewTicker(60 * time.Second)
        for range ticker.C {
            // Only the leader calculates the global threshold
            if server.IsLeader() {
                server.DisposeOldLogs()
            }
        }
    }()

	log.Printf("[DEBUG-CHECK] Server %d is attempting to start campaign NOW...", id)

	elect_manager.StartCampaign(context.Background())

	go func() {
        time.Sleep(5 * time.Second) 
        server.SyncLog()
    }()


	select {}

}


// LogMessage helper restored. Updated IDs to int32 to match Server IDs.
func LogMessage(serverID, targetID int32, msgType string, instanceID int32, value []byte) {
	log.Printf("[Server %d to %d] %s Instance=%d, Value=%s", serverID, targetID, msgType, instanceID, string(value))
}

func generateProposalID(serverID int32) int64 {
	proposalMu.Lock()
	defer proposalMu.Unlock()
	proposalCounter++
	return proposalCounter*10 + int64(serverID)
}

func (s *PaxosServer) IsLeader() bool {
	if s.Election == nil {
        return false
    }
    return s.Election.IsLeader()
}

func (s *PaxosServer) Propose(originalValue []byte) int32 {
	if !s.IsLeader() {
		currentLeader := s.Election.GetLeaderID()
		log.Printf("Server %d is not the leader (Current Leader: %d). Cannot propose.", s.ID, currentLeader)
		return -1
	}

	s.Mu.Lock()
	idx := s.MultiInstance.NextInstance()
	inst := s.MultiInstance.GetInstance(idx)
	
	// is leader Stable
	canSkipPrepare := s.Election.IsStableLeader()

	var proposalID int64
	var valueToPropose = originalValue

	if canSkipPrepare {
		// Phase 2 Only
		proposalID = s.Election.GetCurrentTermProposalID()
		log.Printf("[Leader %d] Skipping Prepare (Multi-Paxos) for Instance %d using ProposalID %d", s.ID, idx, proposalID)
		
		// Implicitly promise ourselves
		inst.ResetRound()
		inst.Promises[s.ID] = true 
		s.Mu.Unlock()

		// go directly to Accept
		s.sendAccept(idx, proposalID, valueToPropose)

	} else {
		//  Phase 1 + Phase 2
		s.Mu.Unlock() 
		
		proposalID = generateProposalID(s.ID)
		var highestAcceptedID int64 = -1

		s.Mu.Lock()
        inst.Promises[s.ID] = true 
        s.Mu.Unlock()

		//  send Prepare
		for peerID, peer := range s.Peers {
			resp, err := peer.Prepare(context.Background(), &pb.PrepareRequest{
				ProposalId: proposalID,
				SenderId:   s.ID,
				InstanceId: idx,
			})

			if err != nil {
				log.Printf("Error contacting peer %d: %v", peerID, err)
				continue
			}

			if resp.Promised {
				s.Mu.Lock()
				inst.Promises[peerID] = true
				s.Mu.Unlock()

				if resp.AcceptedId > 0 && resp.AcceptedId > highestAcceptedID {
					highestAcceptedID = resp.AcceptedId
					valueToPropose = resp.AcceptedValue
				}
			}
		}

		s.Mu.Lock()
		hasQuorum := inst.HasElectionQuorum(s.MultiInstance)
		s.Mu.Unlock()

		if hasQuorum {
			s.Election.SetCurrentTermProposalID(proposalID)
			s.Election.MarkStable(true)

			log.Printf("[Leader %d] Phase 1 successful. Marked as STABLE for future instances.", s.ID)
			
			// Send Accept
			s.sendAccept(idx, proposalID, valueToPropose)
		} else {
			log.Printf("[Leader %d] Failed to reach Prepare Quorum", s.ID)
		}
	}

	return idx
}

func (s *PaxosServer)  sendAccept(idx int32, proposalID int64, value []byte) {
	inst := s.MultiInstance.GetInstance(idx)
    inst.Accepts[s.ID] = true

	for peerID, peer := range s.Peers {
		resp, err := peer.Accept(context.Background(), &pb.AcceptRequest{
			ProposalId: proposalID,
			Value:      value,
			SenderId:   s.ID,
			InstanceId: idx,
		})

		if err != nil {
			log.Printf("Error when sending Accept to peer %d: %v", peerID, err)
			continue
		}

		if resp.Accepted {
			inst.Accepts[peerID] = true
		}
	}

	if inst.HasWriteQuorum(s.MultiInstance) {
		inst.Commit(value)
		log.Printf("[Leader %d] COMMIT value=%s at index=%d", s.ID, string(value), idx)

		s.Mu.Lock()
        s.applyLog() // Leader updates its own KV store immediately
        s.Mu.Unlock()

		// propagate commit to peers
		for peerID, peer := range s.Peers {
				go func(p pb.PaxosClient, pid int32) {
					
					_, err := p.Commit(context.Background(), &pb.CommitRequest{
						ProposalId: proposalID,
						Value:      value,
						SenderId:   s.ID,
						InstanceId: idx,
					})
					if err != nil {
						log.Printf("Failed to send Commit to peer %d: %v", pid, err)
					}
				}(peer, peerID)
			}
	}
}

// Commit: handles a paxos Commit request. (Follower side)
func (s *PaxosServer) Commit(
    ctx context.Context,
    req *pb.CommitRequest,
) (*pb.CommitResponse, error) {

    s.Mu.Lock()
    defer s.Mu.Unlock()

    inst := s.MultiInstance.GetInstance(req.InstanceId)
	inst.Commit(req.Value)

	for {
			curr := s.MultiInstance.GetInstance(s.MultiInstance.NextIndex)
			if curr.IsCommitted {
				s.MultiInstance.NextIndex++
			} else {
				break
			}
		}

	s.applyLog()

	log.Printf("[Server %d] Committed value via Leader %d at Instance %d", s.ID, req.SenderId, req.InstanceId)
    return &pb.CommitResponse{}, nil
}


// recovery function

// GetLogState: Returns the current log state to a recovering node
func (s *PaxosServer) GetLogState(ctx context.Context, req *pb.GetLogRequest) (*pb.GetLogResponse, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	var entries []*pb.LogEntry

	// Loop through all instances
	for i := int32(0); i < s.MultiInstance.NextIndex; i++ {
		inst := s.MultiInstance.GetInstance(i)
		
		if inst.IsCommitted { 
			entries = append(entries, &pb.LogEntry{
				InstanceId: i,
				Value:      inst.CommittedValue, 
			})
		}
	}

	return &pb.GetLogResponse{
		NextIndex:  s.MultiInstance.NextIndex,
		LogEntries: entries,
	}, nil
}
// SyncLog asks peers for their logs and updates the local state
func (s *PaxosServer) SyncLog() {
    log.Println("[SyncLog] Starting state transfer from peers")
    
    var maxNextIndex int32 = 0
    var bestLog []*pb.LogEntry

    //  Ask all peers for their log
    for id, peer := range s.Peers {
        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
        resp, err := peer.GetLogState(ctx, &pb.GetLogRequest{SenderId: s.ID})
        cancel()

        if err != nil {
            log.Printf("[SyncLog] Failed to contact peer %d: %v", id, err)
            continue
        }

        // Adopt the longest log found
        if resp.NextIndex > maxNextIndex {
            maxNextIndex = resp.NextIndex
            bestLog = resp.LogEntries
            log.Printf("[SyncLog] Found longer log from peer %d (Length: %d)", id, maxNextIndex)
        }
    }

    // Apply the log to local state
    s.Mu.Lock()
    defer s.Mu.Unlock()

    // Reset local state to match the best peer
    s.MultiInstance.NextIndex = maxNextIndex
    
	for _, entry := range bestLog {
			inst := s.MultiInstance.GetInstance(entry.InstanceId)
			if !inst.IsCommitted {
				inst.Commit(entry.Value)
			}
	}
	
	s.applyLog()

    log.Printf("[SyncLog] Recovery complete. Synced up to Index %d", maxNextIndex)
}


// improvement 1: better follower read - reads without full data payload
// New RPC Handler: Leader answers what the latest committed index is
func (s *PaxosServer) GetReadIndex(ctx context.Context, req *pb.ReadIndexRequest) (*pb.ReadIndexResponse, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	// Verify Leadership via Etcd 
	if !s.IsLeader() {
		return nil, fmt.Errorf("server %d is not the leader", s.ID)
	}

	// Find the highest committed instance in our log
	var maxCommitted int32 = -1
	
	// checking from the end of the log
	for i := s.MultiInstance.NextIndex - 1; i >= 0; i-- {
		if s.MultiInstance.GetInstance(i).IsCommitted {
			maxCommitted = i
			break
		}
	}

	return &pb.ReadIndexResponse{LeaderCommitIndex: maxCommitted}, nil
}

//  Handle Read Request (Linearizable Read)
func (s *PaxosServer) HandleRead(key string) (string, error) {
	// 1. If we are the Leader, we can read directly (Must verify leadership first!)
	if s.IsLeader() {
		s.Mu.Lock()
		defer s.Mu.Unlock()
		return s.kvStore[key], nil
	}

	// 2. If we are a Follower, ask Leader for the "ReadIndex"
	// (Unlock Mu while doing RPC to avoid blocking internal commits)
	leaderID := s.Election.GetLeaderID()
	leaderClient, ok := s.Peers[leaderID]
	if !ok {
		return "", fmt.Errorf("leader %d not found in peers", leaderID) 
	}

	// Ask the leader: "What is the latest committed index?"
	// The leader will check its Etcd lease inside this call.
	resp, err := leaderClient.GetReadIndex(context.Background(), &pb.ReadIndexRequest{})
	if err != nil {
		return "", fmt.Errorf("failed to get read index from leader: %v", err)
	}
	readIndex := resp.LeaderCommitIndex

	// 3. Wait until our local state catches up to readIndex
	s.Mu.Lock()
	defer s.Mu.Unlock()

	for s.LastApplied < readIndex {
		// Use Cond Wait instead of sleep for efficiency
		s.applyCond.Wait()
	}

	// 4. Now safe to read locally
	return s.kvStore[key], nil
}

// applyLog: Applies committed entries to the KV store
func (s *PaxosServer) applyLog() {
	for {
		nextIdx := s.LastApplied + 1
		inst := s.MultiInstance.GetInstance(nextIdx)

		// Stop if the next instance isn't committed yet
		if !inst.IsCommitted {
			break
		}

		s.ApplyToAuction(string(inst.CommittedValue), nextIdx)

		// the committed batch string
		valStr := string(inst.CommittedValue)

		// split the batch by the delimiter "|"
		commands := strings.Split(valStr, "|")

		// apply in the batch to the KV Store
		for _, cmd := range commands {
			if cmd == "" { continue } //empty - skip
			
			parts := strings.SplitN(cmd, "=", 2)
			if len(parts) == 2 {
				s.kvStore[parts[0]] = parts[1]
			}
		}

		s.LastApplied = nextIdx
	
        err := s.AuctionSM.SaveSnapshot(s.SnapshotPath)
        if err != nil {
            log.Printf("[Server %d] Snapshot Error: %v", s.ID, err)
        } else {
            log.Printf("[Server %d] State saved index %d", s.ID, s.LastApplied)
        }
    }
	
	// notify waiting readers
	s.applyCond.Broadcast()
}



// improvement 3: batching 
// New Client Entry Point (Async Submission)
func (s *PaxosServer) SubmitRequest(val []byte) error {
if s.IsLeader() {
        // push to local batching 
        select {
        case s.requestChan <- val:
            return nil
        default:
            return fmt.Errorf("server overloaded")
        }
    }

    //  forward to leader 
	var leaderID int32 = -1
    for i := 0; i < 10; i++ { // Try for 5 seconds total
        leaderID = s.Election.GetLeaderID()
        if leaderID != -1 {
            break
        }
        log.Printf("[Server %d] Request received but leader not elected yet. Waiting... (%d/10)", s.ID, i+1)
        time.Sleep(500 * time.Millisecond)
    }

    if leaderID == -1 {
        return fmt.Errorf("Leader not elected yet after timeout")
    }

    leaderClient, ok := s.Peers[leaderID]
    if !ok {
        return fmt.Errorf("connection to leader %d not ready", leaderID)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Follower forwards to leader's gRPC endpoint 
    _, err := leaderClient.ForwardPropose(ctx, &pb.ProposeRequest{
        Value:    val,
        SenderId: s.ID,
    })
	if err != nil {
		log.Printf("[DEBUG] Forwarding failed to Leader %d: %v", leaderID, err)
		return err
	}
    return err
}

// Background Processor
func (s *PaxosServer) processBatches() {
	var batch [][]byte
	timer := time.NewTicker(s.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case req := <-s.requestChan:
			// Add to current batch
			batch = append(batch, req)
			
			// If we hit 100 items, flush
			if len(batch) >= s.batchSize {
				s.flushBatch(batch)
				batch = nil 
				timer.Reset(s.batchTimeout)
			}

		case <-timer.C:
			// low load control 
			if len(batch) > 0 {
				s.flushBatch(batch)
				batch = nil
			}
			
		}
	}
}

// Helper to encode and Propose
func (s *PaxosServer) flushBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	log.Printf("[Batching] Flushing batch of %d requests", len(batch))

	// Combine batch into single byte slice using "|" delimiter
	separator := []byte("|")
	combinedValue := bytes.Join(batch, separator)

	//pipeline
	s.pipelineLimit <- struct{}{}

	//launch Propose
	go func(val []byte) {
			// release  when finish
			defer func() { <-s.pipelineLimit }()
			
			s.Propose(val)
		}(combinedValue)


}


func (s *PaxosServer) InitForTest(bufferSize int, batchSize int, timeout time.Duration) {
    s.requestChan = make(chan []byte, bufferSize)
    s.batchSize = batchSize
    s.batchTimeout = timeout
    s.pipelineLimit = make(chan struct{}, 50)
    s.kvStore = make(map[string]string)
    s.applyCond = sync.NewCond(&s.Mu)
    s.LastApplied = -1
    
    // Log the adaptive quorum logic being used for this cluster size
    // This helps verify your Stage 1, 2, or 3 implementation in logs
    log.Printf(" [QUORUM CHECK] Cluster Size: %d | Using Logic: %s | Matrix: %dx%d", 
        s.MultiInstance.NumServers, 
        s.MultiInstance.GetQuorumType(),
        s.MultiInstance.Rows,
        s.MultiInstance.Cols)

    // Start background processor
    go s.processBatches()
}

//state machine fucntions
//make sure state machine is up to date with leader
func (s *PaxosServer) WaitUntilSynced() error {
    if s.IsLeader() {
        return nil // leader is always up to date
    }

    leaderID := s.Election.GetLeaderID()
    leaderClient, ok := s.Peers[leaderID]
    if !ok {
        return fmt.Errorf("leader not found")
    }

    // ask leader for the latest commit index
    resp, err := leaderClient.GetReadIndex(context.Background(), &pb.ReadIndexRequest{})
    if err != nil {
        return err
    }

    s.Mu.Lock()
    defer s.Mu.Unlock()
    // wait  applyLog to reach the leader's index
    for s.LastApplied < resp.LeaderCommitIndex {
        s.applyCond.Wait()
    }
    return nil
}

//dispose log

func (s *PaxosServer) GetServerStatus(ctx context.Context, req *pb.Empty) (*pb.StatusResponse, error) {
    s.Mu.Lock()
    defer s.Mu.Unlock()
    
    return &pb.StatusResponse{
        AppliedIndex: s.LastApplied,
        IsLeader:     s.IsLeader(),
    }, nil
}

func (s *PaxosServer) GetMinApplied() int32 {
    min := s.LastApplied
    
    for _, peer := range s.Peers {
        // Call a small gRPC method to get the follower's LastApplied
        resp, err := peer.GetServerStatus(context.Background(), &pb.Empty{})
        if err == nil {
            if resp.AppliedIndex < min {
                min = resp.AppliedIndex
            }
        }
    }
    return min
}

func (s *PaxosServer) DisposeOldLogs() {
    threshold := s.GetMinApplied()
    
    s.Mu.Lock()
    defer s.Mu.Unlock()
    
	for idx := range s.MultiInstance.Instances {
        if idx < threshold {
            delete(s.MultiInstance.Instances, idx)
        }
    }
    log.Printf("[Server %d] Disposed logs up to index %d", s.ID, threshold)
}


func (s *PaxosServer) ForwardPropose(ctx context.Context, req *pb.ProposeRequest) (*pb.ProposeResponse, error) {
    if !s.IsLeader() {
		return &pb.ProposeResponse{Success: false}, fmt.Errorf("not the leader")    }
		
    log.Printf("[Leader %d] Received forwarded proposal from Server %d", s.ID, req.SenderId)

	cmd, err := state.DeserializeCommand(req.Value)
    if err != nil {
        return &pb.ProposeResponse{Success: false}, fmt.Errorf("failed to deserialize command")
    }

	respChan := make(chan string, 1)
    s.AuctionSM.Mu.Lock()
    s.AuctionSM.Responses[cmd.Timestamp] = respChan
    s.AuctionSM.Mu.Unlock()

	defer func() {
        s.AuctionSM.Mu.Lock()
        delete(s.AuctionSM.Responses, cmd.Timestamp)
        s.AuctionSM.Mu.Unlock()
    }()

	err = s.SubmitRequest(req.Value)
	if err != nil {
        return &pb.ProposeResponse{Success: false}, fmt.Errorf("leader queue full")
    }

    select {
    case <-respChan:
        return &pb.ProposeResponse{Success: true}, nil
    case <-time.After(15 * time.Second):
        return nil, fmt.Errorf("Timeout")
    }
}
