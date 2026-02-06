package membership

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var timeout int64=10

type MembershipManager struct {
	ServerID   int32
	Etcdclient *clientv3.Client
	Peers      map[int32]string 
	Mu         sync.RWMutex
    Callback   func(id int32, ip string, isJoin bool) 
}

func NewMembershipManager(id int32, endpoints []string, cb func(int32, string, bool)) (*MembershipManager, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &MembershipManager{
		ServerID:   id,
		Etcdclient: client,
		Peers:      make(map[int32]string),
        Callback:   cb,
	}, nil
}

func (member *MembershipManager) StartMembership(ctx context.Context, myIP string) {
	// create a lease
	leaseGrant, err := member.Etcdclient.Grant(ctx, timeout)
	if err != nil {
		log.Fatalf("Failed to create lease: %v", err)
	}

	// check for heartbeat to renew lease 
	ch, err := member.Etcdclient.KeepAlive(ctx, leaseGrant.ID)
	if err != nil {
		log.Fatalf("Failed to start keep alive: %v", err)
	}

	// consume keep alive channel to avoid deadlock
	go func() {
		for range ch { /* heartbeat ok */ }
	}()

	// register self with the lease
	key := fmt.Sprintf("/nodes/%d", member.ServerID)
	_, err = member.Etcdclient.Put(ctx, key, myIP, clientv3.WithLease(leaseGrant.ID))
	if err != nil {
		log.Fatalf("failed to register node: %v", err)
	}
	log.Printf("[Membership] registered as %d with IP %s", member.ServerID, myIP)

	// watch for changes
	go member.watchPeers(ctx)
}

func (member *MembershipManager) watchPeers(ctx context.Context) {
	watchChan := member.Etcdclient.Watch(ctx, "/nodes/", clientv3.WithPrefix())
	
    resp, _ := member.Etcdclient.Get(ctx, "/nodes/", clientv3.WithPrefix())
    for _, kv := range resp.Kvs {
        member.handleUpdate(string(kv.Key), string(kv.Value), false)
    }

	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			key := string(event.Kv.Key)
            
			if event.Type == clientv3.EventTypePut {
                // node joined
				val := string(event.Kv.Value)
				member.handleUpdate(key, val, false)
			} else if event.Type == clientv3.EventTypeDelete {
                // node failed or left
				member.handleUpdate(key, "", true) 
			}
		}
	}
}

func (member *MembershipManager) handleUpdate(key, val string, isDelete bool) {
	var id int32
	fmt.Sscanf(key, "/nodes/%d", &id)
    
    // ignore self
    if id == member.ServerID { return }

	member.Mu.Lock()
	defer member.Mu.Unlock()

	if isDelete {
		delete(member.Peers, id)
		log.Printf("[Membership] node %d left or crashed", id)
        if member.Callback != nil { member.Callback(id, "", false) }
	} else {
		member.Peers[id] = val
		log.Printf("[Membership] node %d joined at %s", id, val)
        if member.Callback != nil { member.Callback(id, val, true) }
	}
}