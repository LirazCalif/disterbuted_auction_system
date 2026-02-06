package election

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"paxos/logger"
)

type ElectionManager struct {
	ServerID    int32
	EtcdClient  *clientv3.Client
	Session     *concurrency.Session
	Election    *concurrency.Election
	
	mu          sync.RWMutex
	isLeader    bool
	leaderID    int32 

	stable         bool
	termProposalID int64

}

func NewElectionManager(serverID int32, endpoints []string) (*ElectionManager, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %v", err)
	}

	return &ElectionManager{
		ServerID:   serverID,
		EtcdClient: client,
		leaderID:   -1,
	}, nil
}

func (elect_manager *ElectionManager) StartCampaign(ctx context.Context) {
	go func() {

		for {
			select {
			case <-ctx.Done():
				return 
			default:
				// reset stability before new campaign
				elect_manager.MarkStable(false)

				log.Printf("[Election %d] Connecting to Etcd Session...", elect_manager.ServerID)

				// create a session - 5 seconds TTL
				session, err := concurrency.NewSession(elect_manager.EtcdClient, concurrency.WithTTL(5))
				if err != nil {
					log.Printf("[Election %d] failed to create a session: %v", elect_manager.ServerID, err)
					logger.Emit(fmt.Sprintf("[NODE] Server %d failed etcd session: %v", elect_manager.ServerID, err))
					time.Sleep(2 * time.Second)
					continue
				}
				log.Printf("[Election %d] Session created successfully", elect_manager.ServerID)
				elect_manager.Session = session

				elect_manager.Election = concurrency.NewElection(session, "/paxos/leader")
				
				//start a watcher for this specific session
				ctxWatch, cancelWatch := context.WithCancel(ctx)
				go elect_manager.watchLeadership(ctxWatch)


				log.Printf("[Election %d] Campaigning", elect_manager.ServerID)
				
				// campaign - blocks until  become leader
				err = elect_manager.Election.Campaign(ctx, fmt.Sprintf("%d", elect_manager.ServerID))
				if err != nil {
					//clean if fails
					session.Close()
                    cancelWatch()
					// If canceled or expired, loop and try again
					time.Sleep(1 * time.Second)
					continue
				}

				// set myself as the leader
				elect_manager.setLeader(true, elect_manager.ServerID)

				logger.EmitNodeStatus(int(elect_manager.ServerID), 0, true) 
                logger.Emit(fmt.Sprintf("[LEADER] Server %d won election and is now the proposer", elect_manager.ServerID))

				log.Printf("[Election %d] is the leader", elect_manager.ServerID)

				//  hold leadership
				select {
				case <-ctx.Done():
					elect_manager.Resign()
					cancelWatch()
					return
				case <-session.Done():
					elect_manager.setLeader(false, -1)
					elect_manager.MarkStable(false)
					log.Printf("[Election %d] Session expired, no longer leader", elect_manager.ServerID)
					
					logger.EmitNodeStatus(int(elect_manager.ServerID), 0, false)
                    logger.Emit(fmt.Sprintf("[NODE] Server %d session expired, stepped down", elect_manager.ServerID))
					
					cancelWatch()
				}
			}
		}
	}()
}

// monitors the election key
func (elect_manager *ElectionManager) watchLeadership(ctx context.Context) {
	ch := elect_manager.Election.Observe(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if len(resp.Kvs) > 0 {
				valStr := string(resp.Kvs[0].Value)
				leaderID, _ := strconv.Atoi(valStr)
				
				// update leader
				elect_manager.setLeaderID(int32(leaderID))
				
				log.Printf("[Election %d] Observed new leader: %d", elect_manager.ServerID, leaderID)
				logger.Emit(fmt.Sprintf("[QUORUM] Cluster reached consensus on Leader: %d", leaderID))
			}
		}
	}
}

func (elect_manager *ElectionManager) Resign() {
	elect_manager.mu.Lock()
	defer elect_manager.mu.Unlock()

	if elect_manager.isLeader && elect_manager.Election != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		elect_manager.Election.Resign(ctx)
		elect_manager.isLeader = false
		elect_manager.stable = false
	}
}

func (elect_manager *ElectionManager) IsLeader() bool {
	elect_manager.mu.RLock()
	defer elect_manager.mu.RUnlock()
	return elect_manager.isLeader
}

func (elect_manager *ElectionManager) GetLeaderID() int32 {
	elect_manager.mu.RLock()
	defer elect_manager.mu.RUnlock()
	return elect_manager.leaderID
}

func (elect_manager *ElectionManager) setLeader(isLeader bool, id int32) {
	elect_manager.mu.Lock()
	defer elect_manager.mu.Unlock()
	elect_manager.isLeader = isLeader
	elect_manager.leaderID = id
}

func (elect_manager *ElectionManager) setLeaderID(id int32) {
	elect_manager.mu.Lock()
	defer elect_manager.mu.Unlock()
	// update if not the leader 
	if !elect_manager.isLeader {
		elect_manager.leaderID = id
	}
}

func (elect_manager *ElectionManager) Close() {
	if elect_manager.EtcdClient != nil {
		elect_manager.EtcdClient.Close()
	}
}

//  multipaxos

// check ifthe leader is stable
func (elect_manager *ElectionManager) IsStableLeader() bool {
	elect_manager.mu.RLock()
	defer elect_manager.mu.RUnlock()
	return elect_manager.isLeader && elect_manager.stable
}

// update stability status
func (elect_manager *ElectionManager) MarkStable(stable bool) {
	elect_manager.mu.Lock()
	defer elect_manager.mu.Unlock()
	elect_manager.stable = stable
}

// Get the ProposalID of the stable term
func (elect_manager *ElectionManager) GetCurrentTermProposalID() int64 {
	elect_manager.mu.RLock()
	defer elect_manager.mu.RUnlock()
	return elect_manager.termProposalID
}

// Set the ProposalID for this term
func (elect_manager *ElectionManager) SetCurrentTermProposalID(pid int64) {
	elect_manager.mu.Lock()
	defer elect_manager.mu.Unlock()
	elect_manager.termProposalID = pid
}