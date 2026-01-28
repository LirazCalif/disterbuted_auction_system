package api

import (
	"log"
	"net"
	"sync"
	"time"
	"fmt"
	"strings"
	"bytes"
	"os"
	"context"
	"math"

	pb "paxos/proto"
	"paxos/src/server/paxos"
	"paxos/src/server/election"
	"paxos/src/server/state"
	"paxos/src/server/storage"
	"paxos/src/server/membership"


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

	//wall
	WAL *storage.WALManager

	//membership
	Membership *membership.MembershipManager

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


func NewPaxosServer(id int32, snapshotPath string, walPath string) *PaxosServer {
	return &PaxosServer{
		ID:            id,
		Peers:         make(map[int32]pb.PaxosClient),
		MultiInstance: paxos.NewMultiPaxosInstance(1), 
		LastApplied:   -1,
		kvStore:       make(map[string]string),
		requestChan:   make(chan []byte, 5000),
		batchSize:     100,
		batchTimeout:  200 * time.Millisecond,
		pipelineLimit: make(chan struct{}, 200),
		AuctionSM:     state.NewAuctionStateMachine(),
		SnapshotPath:  snapshotPath,
		WAL:           storage.NewWALManager(walPath),
	}
}

// Prepare: handles a paxos Prepare request - follower side
func (s *PaxosServer) Prepare(
	ctx context.Context,
	req *pb.PrepareRequest,
) (*pb.PromiseResponse, error) {

	s.Mu.Lock()
	defer s.Mu.Unlock()

	inst := s.MultiInstance.GetInstance(req.InstanceId) 
    
    resp := inst.Prepare_Instance(req.ProposalId)
	if s.WAL != nil {
		go s.WAL.PersistState(s.MultiInstance.Instances) 
	}


	log.Printf("[Server %d] Prepare from %d proposal=%d promised=%v",
		s.ID, req.SenderId, req.ProposalId, resp.Promised)

	return resp, nil
}

// Accept: handles a paxos Accept request - follower side
func (s *PaxosServer) Accept(
	ctx context.Context,
	req *pb.AcceptRequest,
) (*pb.AcceptedResponse, error) {

	s.Mu.Lock()
	defer s.Mu.Unlock()

	inst := s.MultiInstance.GetInstance(req.InstanceId)
	resp := inst.Accept_Instance(req.ProposalId, req.Value)
	if s.WAL != nil {
		go s.WAL.PersistState(s.MultiInstance.Instances)
	}


	log.Printf("[Server %d] Accept from %d proposal=%d accepted=%v",
		s.ID, req.SenderId, req.ProposalId, resp.Accepted)

	return resp, nil
}

// Start: starts the paxos gRPC server on the given port.
func Start(server *PaxosServer, port string, etcdEndpoints []string) {

	elect, err := election.NewElectionManager(server.ID, etcdEndpoints)
    if err != nil {
        log.Fatalf("failed to init election manager: %v", err)
    }
    server.Election = elect

	//internal port
	lis, err := net.Listen("tcp", ":"+port)
    if err != nil {
        log.Fatalf("failed to listen on gRPC port %s: %v", port, err)
    }

    grpcServer := grpc.NewServer()
    pb.RegisterPaxosServer(grpcServer, server)
	//initial variables
	server.applyCond = sync.NewCond(&server.Mu)


	if err := server.AuctionSM.LoadSnapshot(server.SnapshotPath); err == nil {
		log.Printf("[Server %d] recovery: loaded state from %s", server.ID, server.SnapshotPath)
		server.LastApplied = int32(server.AuctionSM.LastAppliedIdx)

		}
	
	//recovery from wall
	if loadedInstances, err := server.WAL.LoadState(); err == nil && len(loadedInstances) > 0 {
		log.Printf("[Server %d] recovery: loaded %d instances from WAL", server.ID, len(loadedInstances))
		server.MultiInstance.Instances = loadedInstances
		
		var maxIdx int32 = -1
		for idx := range loadedInstances {
			if idx > maxIdx {
				maxIdx = idx
			}
		}
		if maxIdx+1 > server.MultiInstance.NextIndex {
			server.MultiInstance.NextIndex = maxIdx + 1
		}
	}
	

	//start the REST API
	numericPort := 8080 + int(server.ID) 
	apiPort := fmt.Sprintf("%d", numericPort)
	server.StartAuctionInterface(apiPort)

	
	go server.processBatches()

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("paxos server %d started on %s", server.ID, port)

	go func() {
        // run every 60 seconds to clean up RAM
        ticker := time.NewTicker(60 * time.Second)
        for range ticker.C {
            // only the leader calculates global threshold
            if server.IsLeader() {
                server.DisposeOldLogs()
            }
        }
    }()

	log.Printf("[DEBUG] server %d is try to start campaign now", server.ID)

	go server.Election.StartCampaign(context.Background())

	go func() {
		waitDuration := time.Duration(5 + server.ID) * time.Second
		log.Printf("[Server %d] Waiting %v before starting SyncLog to avoid boot storm", server.ID, waitDuration)
        time.Sleep(waitDuration)
        server.SyncLog()
    }()

	select{}

}


// LogMessage helper restored
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
	if s.MultiInstance.NextIndex > s.LastApplied+1 {
		log.Printf("[Leader %d] Gap detected! Resetting NextIndex from %d to %d", 
			s.ID, s.MultiInstance.NextIndex, s.LastApplied+1)
		s.MultiInstance.NextIndex = s.LastApplied + 1
	}
	idx := s.MultiInstance.NextInstance()
	inst := s.MultiInstance.GetInstance(idx)
	
	// is leader stable
	canSkipPrepare := s.Election.IsStableLeader()

	var proposalID int64
	var valueToPropose = originalValue
	

	if canSkipPrepare {
		proposalID = s.Election.GetCurrentTermProposalID()
		log.Printf("[Leader %d] skipping Prepare for instance %d using ProposalID %d", s.ID, idx, proposalID)
		
		// implicitly promise ourselves
		inst.ResetRound()
		inst.Promises[s.ID] = true 
		s.Mu.Unlock()

		// go directly to Accept
		s.sendAccept(idx, proposalID, valueToPropose)

	} else {
		s.Mu.Unlock() 

		quorumChan := make(chan bool, 1)
		proposalID = generateProposalID(s.ID)
		var highestAcceptedID int64 = -1
		var waitgroup sync.WaitGroup
		var responders []int32
		var mu sync.Mutex
		

		s.Mu.Lock()
        inst.Promises[s.ID] = true 
		currentPeers := make(map[int32]pb.PaxosClient)
		for id, client := range s.Peers {
			currentPeers[id] = client
		}
        s.Mu.Unlock()

		//  send Prepare
		for peerID, peer := range currentPeers{
			waitgroup.Add(1)
			go func(pID int32, pClient pb.PaxosClient) {
				defer waitgroup.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				resp, err := peer.Prepare(ctx, &pb.PrepareRequest{
					ProposalId: proposalID,
					SenderId:   s.ID,
					InstanceId: idx,
				})

				if err != nil {
					log.Printf("Error contacting peer %d: %v", peerID, err)
					return
				}

				if resp.Promised {
					s.Mu.Lock()
					inst.Promises[pID] = true
					if inst.HasElectionQuorum(s.MultiInstance) {
						select { case quorumChan <- true: default: }
					}
					s.Mu.Unlock()
					mu.Lock()
					responders = append(responders, pID)
					if resp.AcceptedId > 0 && resp.AcceptedId > highestAcceptedID {
						highestAcceptedID = resp.AcceptedId
						valueToPropose = resp.AcceptedValue
					}
					mu.Unlock()
				}
			}(peerID, peer)

		}
		allDone := make(chan struct{})
		go func() { waitgroup.Wait(); close(allDone) }()
		
		log.Printf("[DEBUG %d] Phase 1 finished. Responders: %v. Needed: %d", 
			s.ID, responders, s.MultiInstance.NumServers/2+1)

		select {
		case <-quorumChan:
		case <-allDone:
		case <-time.After(15 * time.Second):
		}
		
		s.Mu.Lock()
		hasQuorum := inst.HasElectionQuorum(s.MultiInstance)
		s.Mu.Unlock()

		if hasQuorum {
			s.Election.SetCurrentTermProposalID(proposalID)
			s.Election.MarkStable(true)

			log.Printf("[Leader %d] Phase 1 successful. Marked as stable for future instances.", s.ID)
			
			// Send Accept
			s.sendAccept(idx, proposalID, valueToPropose)
		} else {
			log.Printf("[Leader %d] failed to reach Prepare quorum", s.ID)
		}
	}

	return idx
}

func (s *PaxosServer)  sendAccept(idx int32, proposalID int64, value []byte) {
	inst := s.MultiInstance.GetInstance(idx)
	s.Mu.Lock()
    inst.Accepts[s.ID] = true
	currentPeers := make(map[int32]pb.PaxosClient)
    for id, client := range s.Peers {
        currentPeers[id] = client
    }
    s.Mu.Unlock()

	if s.WAL != nil {
		go s.WAL.PersistState(s.MultiInstance.Instances)
	}
	quorumChan := make(chan bool, 1)
	allDone := make(chan struct{})
	var waitgroup sync.WaitGroup
	var acceptors []int32
	var accMu sync.Mutex


	for peerID, peer := range currentPeers {
		waitgroup.Add(1)
		go func(id int32, client pb.PaxosClient) {
        	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        	defer cancel()
        
			resp, err := client.Accept(ctx, &pb.AcceptRequest{
				ProposalId: proposalID,
				Value:      value,
				SenderId:   s.ID,
				InstanceId: idx,
			})
			if err == nil && resp.Accepted {
				s.Mu.Lock()
				inst.Accepts[id] = true

				if inst.HasWriteQuorum(s.MultiInstance) {
					select {
					case quorumChan <- true:
					default:
					}
				}
				s.Mu.Unlock()

				accMu.Lock()
				acceptors = append(acceptors, id)
				accMu.Unlock()
			}
		}(peerID, peer)
	}
	go func() { waitgroup.Wait(); close(allDone) }()

	select {
	case <-quorumChan: 
	case <-allDone:   
	case <-time.After(15 * time.Second): 
	}

	s.Mu.Lock()
	reached := inst.HasWriteQuorum(s.MultiInstance)
	s.Mu.Unlock()
	
	log.Printf("[DEBUG %d] Phase 2 finished. Acceptors: %v. Quorum Reached: %v", 
		s.ID, acceptors, reached)

	if reached {
		inst.Commit(value)
		log.Printf("[Leader %d] commit value=%s at index=%d", s.ID, string(value), idx)

		s.Mu.Lock()
        s.applyLog() // leader updates its own KV store immediately
        commitPeers := make(map[int32]pb.PaxosClient)
        for id, p := range s.Peers {
            commitPeers[id] = p
        }
		s.Mu.Unlock()

		// propagate commit to peers
		for peerID, peer := range commitPeers {
				go func(p pb.PaxosClient, pid int32) {
					
					p.Commit(context.Background(), &pb.CommitRequest{
						ProposalId: proposalID,
						Value:      value,
						SenderId:   s.ID,
						InstanceId: idx,
					})

				}(peer, peerID)
		}
		
	}
}

// Commit: handles a paxos Commit request. follower 
func (s *PaxosServer) Commit(
    ctx context.Context,
    req *pb.CommitRequest,
) (*pb.CommitResponse, error) {

    s.Mu.Lock()
    defer s.Mu.Unlock()

    inst := s.MultiInstance.GetInstance(req.InstanceId)
	inst.Commit(req.Value)

	if s.WAL != nil {
		go s.WAL.PersistState(s.MultiInstance.Instances)
	}

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

// returns the current log state to a recovering node
func (s *PaxosServer) GetLogState(ctx context.Context, req *pb.GetLogRequest) (*pb.GetLogResponse, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	var entries []*pb.LogEntry
	var snapshotBytes []byte
    var snapshotIdx int32 = 0

	//read snapshots from disk if exist
	if content, err := os.ReadFile(s.SnapshotPath); err == nil {
        snapshotBytes = content
        snapshotIdx = s.LastApplied
    }

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
		Snapshot:      snapshotBytes,
        SnapshotIndex: snapshotIdx,
	}, nil
}
// function asks peers for their logs and updates the local state
func (s *PaxosServer) SyncLog() {
    log.Println("[SyncLog] starting state transfer from peers")
    
    var resMu sync.Mutex
	var maxNextIndex int32 = 0
    var bestLog []*pb.LogEntry
	var bestSnapshot []byte
    var bestSnapshotIdx int32 = -1
	var waitgroup sync.WaitGroup

    //  ask peers for their log
    for id, peer := range s.Peers {
		waitgroup.Add(1)
		go func(pID int32, pClient pb.PaxosClient) {
			defer waitgroup.Done()
        	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        	resp, err := peer.GetLogState(ctx, &pb.GetLogRequest{SenderId: s.ID})
        	cancel()

			if err != nil {
				log.Printf("[SyncLog] can't contact peer %d: %v", id, err)
				return
			}
			resMu.Lock()

			// Adopt the longest log found
			if resp.SnapshotIndex > bestSnapshotIdx {
				bestSnapshotIdx = resp.SnapshotIndex
				bestSnapshot = resp.Snapshot
				maxNextIndex = resp.NextIndex
				bestLog = resp.LogEntries
				log.Printf("[SyncLog] found new snapshot from peer %d, in Idx: %d", id, bestSnapshotIdx)
			} else if resp.NextIndex > maxNextIndex {
				maxNextIndex = resp.NextIndex
				bestLog = resp.LogEntries
				log.Printf("[SyncLog] found longer log from peer %d with length: %d", id, maxNextIndex)

			}
			resMu.Unlock()
		}(id, peer)

	}
	waitgroup.Wait()

	if bestSnapshot != nil && bestSnapshotIdx > s.LastApplied {
    	_ = os.WriteFile(s.SnapshotPath, bestSnapshot, 0644)
	}

    // Apply the log to local state
    s.Mu.Lock()

	//install snapshots
	if bestSnapshot != nil && bestSnapshotIdx > s.LastApplied {
		log.Printf("[SyncLog] Installing new snapshot (Index %d)", bestSnapshotIdx)
    	if err := s.AuctionSM.LoadSnapshot(s.SnapshotPath); err != nil {
        	log.Printf("can't write snapshot: %v", err)
			s.LastApplied = bestSnapshotIdx
        	s.MultiInstance.NextIndex = bestSnapshotIdx + 1
    	}
	}

    // Reset local state 
	for _, entry := range bestLog {
			inst := s.MultiInstance.GetInstance(entry.InstanceId)
			if !inst.IsCommitted {
				inst.Commit(entry.Value)
			}
	}
	
	s.applyLog()
	s.Mu.Unlock()
	s.DisposeOldLogs()

    log.Printf("[SyncLog] completed recovery")
}


// improvement 1: better follower read - reads without full data payload
// RPC handler - leader answers what the latest committed index is
func (s *PaxosServer) GetReadIndex(ctx context.Context, req *pb.ReadIndexRequest) (*pb.ReadIndexResponse, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	// verify leader with etcd 
	if !s.IsLeader() {
		return nil, fmt.Errorf("server %d is not the leader", s.ID)
	}

	// find the highest committed instance in log
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

//  handle read request - linearizable read
func (s *PaxosServer) HandleRead(key string) (string, error) {
	// If the Leader read directly 
	if s.IsLeader() {
		s.Mu.Lock()
		defer s.Mu.Unlock()
		return s.kvStore[key], nil
	}

	// if the follower ask the leader for readindex
	leaderID := s.Election.GetLeaderID()
	leaderClient, ok := s.Peers[leaderID]
	if !ok {
		return "", fmt.Errorf("leader %d not found in peers", leaderID) 
	}

	resp, err := leaderClient.GetReadIndex(context.Background(), &pb.ReadIndexRequest{})
	if err != nil {
		return "", fmt.Errorf("failed to get read index from leader: %v", err)
	}
	readIndex := resp.LeaderCommitIndex

	// wait our local state catches up to readindex
	s.Mu.Lock()
	defer s.Mu.Unlock()

	for s.LastApplied < readIndex {
		// use cond wait instead of sleep for efficiency
		s.applyCond.Wait()
	}

	return s.kvStore[key], nil
}

// applies committed entries to the KV store
func (s *PaxosServer) applyLog() {
	for {
		nextIdx := s.LastApplied + 1
		inst := s.MultiInstance.GetInstance(nextIdx)

		// stop if the next instance yet committed
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

	}

	go func(idx int32) {
		err := s.AuctionSM.SaveSnapshot(s.SnapshotPath)
		if err != nil {
			log.Printf("[Server %d] Snapshot Error: %v", s.ID, err)
		} else {
			log.Printf("[Server %d] State saved index %d", s.ID, s.LastApplied)
		}
	}(s.LastApplied)
    
	
	// notify waiting readers
	s.applyCond.Broadcast()
}



// improvement 3: batching 
// new client entry point
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
    for i := 0; i < 10; i++ { 
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

    // follower forwards to leader gRPC endpoint 
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

// background processor
func (s *PaxosServer) processBatches() {
	var batch [][]byte
	timer := time.NewTicker(s.batchTimeout)
	defer timer.Stop()

	for {
		select {
		case req := <-s.requestChan:
			// add to current batch
			batch = append(batch, req)
			
			// flush after 100 items
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

// helper to encode and Propose
func (s *PaxosServer) flushBatch(batch [][]byte) {
	if len(batch) == 0 {
		return
	}

	log.Printf("[Batching] Flushing batch of %d requests", len(batch))

	// combine batch using "|" 
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
    
    // log the adaptive quorum logic being used for this cluster size
    log.Printf(" [QUORUM CHECK] cluster Size: %d,  using Logic: %s, matrix: %dx%d", 
        s.MultiInstance.NumServers, 
        s.MultiInstance.GetQuorumType(),
        s.MultiInstance.Rows,
        s.MultiInstance.Cols)

    // start background processor
    go s.processBatches()
}

//state machine fucntions
//make sure state machine is updateed with leader
func (s *PaxosServer) WaitUntilSynced() error {
    if s.IsLeader() {
        return nil
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
    // wait applyLog to reach the leader's index
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
    s.Mu.Lock()
    min := s.LastApplied
	currentPeers := make([]pb.PaxosClient, 0, len(s.Peers))
    for _, p := range s.Peers {
        currentPeers = append(currentPeers, p)
    }
    s.Mu.Unlock()
    
    for _, peer := range currentPeers {
        // call a small gRPC method to get the follower's LastApplied
        ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		resp, err := peer.GetServerStatus(ctx, &pb.Empty{})
		cancel()
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
    case <-time.After(30 * time.Second):
        return nil, fmt.Errorf("forward Timeout")
    }
}

func (s *PaxosServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
    s.Mu.Lock()
    defer s.Mu.Unlock()

	log.Printf("[Server %d] gets snapshot from leader %d , last index: %d", s.ID, req.SenderId, req.LastIncludedIndex)
    // check if the snapshot newer 
    if req.LastIncludedIndex <= s.LastApplied {
        return &pb.InstallSnapshotResponse{Success: true}, nil
    }

    // write the snapshot to disk
    err := os.WriteFile(s.SnapshotPath, req.Data, 0644)
    if err != nil {
        log.Printf("[Error] failed to write snapshot file: %v", err)
        return &pb.InstallSnapshotResponse{Success: false}, err
    }

    // reload the state machine
    err = s.AuctionSM.LoadSnapshot(s.SnapshotPath)
    if err != nil {
        log.Printf("[Error] failed to load snapshot into SM: %v", err)
        return &pb.InstallSnapshotResponse{Success: false}, err
    }

    // update Paxos - log index forward
    s.LastApplied = int32(s.AuctionSM.LastAppliedIdx)
    s.MultiInstance.NextIndex = s.LastApplied + 1

    // clean old logs
    for idx := range s.MultiInstance.Instances {
        if int32(idx) <= s.LastApplied {
            delete(s.MultiInstance.Instances, idx)
        }
    }

    log.Printf("[Server %d] snapshot installed successfully. New Index %d", s.ID, s.LastApplied)
    
    return &pb.InstallSnapshotResponse{Success: true}, nil
}


// membership
func (s *PaxosServer) MembershipChange(id int32, addr string, isJoin bool) {
	if isJoin {
		// dial outside the lock to prevent global deadlock 
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("[Membership] failed to dial peer %d: %v", id, err)
			return
		}
		client := pb.NewPaxosClient(conn)

		s.Mu.Lock()
		s.Peers[id] = client
		
		// update dynamic cluster size and grid parameters
		n := len(s.Peers) + 1
		s.MultiInstance.NumServers = n
		s.MultiInstance.Rows = int(math.Sqrt(float64(n)))
		if s.MultiInstance.Rows == 0 { s.MultiInstance.Rows = 1 }
		s.MultiInstance.Cols = n / s.MultiInstance.Rows
		s.Mu.Unlock()
		
		log.Printf("[Server %d] peer %d joined. grid updated to %dx%d", 
			s.ID, id, s.MultiInstance.Rows, s.MultiInstance.Cols)
	} else {
		s.Mu.Lock()
		delete(s.Peers, id)

		// recalculate grid after node leaves
		n := len(s.Peers) + 1
		s.MultiInstance.NumServers = n
		s.MultiInstance.Rows = int(math.Sqrt(float64(n)))
		if s.MultiInstance.Rows == 0 { s.MultiInstance.Rows = 1 }
		s.MultiInstance.Cols = n / s.MultiInstance.Rows
		s.Mu.Unlock()

		log.Printf("[Server %d] peer %d removed. grid updated to %dx%d", 
			s.ID, id, s.MultiInstance.Rows, s.MultiInstance.Cols)
	}
}