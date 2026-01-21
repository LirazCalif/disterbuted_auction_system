package api

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	pb "paxos/proto"
	"paxos/src/server/paxos"
	"paxos/src/server/election"

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
) {
	// Convert int to int32 for internal storage
	
	elect_manager, err := election.NewElectionManager(id, []string{"localhost:2379"})
    if err != nil {
        log.Fatalf("Failed to init election manager: %v", err)
    }


	server := &PaxosServer{
		ID:       id,
		Peers:    peers,
		MultiInstance: paxos.NewMultiPaxosInstance(len(serverIDs)),
		Election: elect_manager,
		
	}

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

	time.Sleep(2 * time.Second) 
	server.SyncLog()

	elect_manager.StartCampaign(context.Background())

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
    s.Mu.Unlock()

	inst.ResetRound()
	proposalID := generateProposalID(s.ID)
	inst.Promises[s.ID] = true   // leader counts as promised


	// declare variables
	var highestAcceptedID int64 = -1
	var valueToPropose = originalValue

	// send Prepare to all peers
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
			inst.Promises[peerID] = true

			if resp.AcceptedId > 0 && resp.AcceptedId > highestAcceptedID {
				highestAcceptedID = resp.AcceptedId
				valueToPropose = resp.AcceptedValue
			}
		}
	}

	if inst.HasPromiseQuorum() {
		log.Printf("[Leader %d] Quorum Reached for Prepare. Sending Accept", s.ID)
		s.sendAccept(idx, proposalID, valueToPropose)
	} else {
		log.Printf("[Leader %d] Failed to reach Prepare Quorum", s.ID)
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

	if inst.HasAcceptQuorum() {
		inst.Commit(value)
		log.Printf("[Leader %d] COMMIT value=%s at index=%d", s.ID, string(value), idx)

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

	if req.InstanceId == s.MultiInstance.NextIndex {
        s.MultiInstance.NextIndex++
    }

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

    log.Printf("[SyncLog] Recovery complete. Synced up to Index %d", maxNextIndex)
}