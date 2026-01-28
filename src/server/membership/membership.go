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
    Callback   func(id int32, ip string, isJoin bool) // Notify PaxosServer on changes
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

func (m *MembershipManager) StartMembership(ctx context.Context, myIP string) {
	// create a lease
	leaseGrant, err := m.Etcdclient.Grant(ctx, timeout)
	if err != nil {
		log.Fatalf("Failed to create lease: %v", err)
	}

	// check for heartbeat to renew lease 
	ch, err := m.Etcdclient.KeepAlive(ctx, leaseGrant.ID)
	if err != nil {
		log.Fatalf("Failed to start keep alive: %v", err)
	}

	// consume keep alive channel to avoid deadlock
	go func() {
		for range ch { /* heartbeat ok */ }
	}()

	// register self with the lease
	key := fmt.Sprintf("/nodes/%d", m.ServerID)
	_, err = m.Etcdclient.Put(ctx, key, myIP, clientv3.WithLease(leaseGrant.ID))
	if err != nil {
		log.Fatalf("failed to register node: %v", err)
	}
	log.Printf("[Membership] registered as %d with IP %s", m.ServerID, myIP)

	// watch for changes
	go m.watchPeers(ctx)
}

func (m *MembershipManager) watchPeers(ctx context.Context) {
	watchChan := m.Etcdclient.Watch(ctx, "/nodes/", clientv3.WithPrefix())
	
    resp, _ := m.Etcdclient.Get(ctx, "/nodes/", clientv3.WithPrefix())
    for _, kv := range resp.Kvs {
        m.handleUpdate(string(kv.Key), string(kv.Value), false) // false = add
    }

	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			key := string(event.Kv.Key)
            
			if event.Type == clientv3.EventTypePut {
                // node joined
				val := string(event.Kv.Value)
				m.handleUpdate(key, val, false)
			} else if event.Type == clientv3.EventTypeDelete {
                // node failed or left
				m.handleUpdate(key, "", true) 
			}
		}
	}
}

func (m *MembershipManager) handleUpdate(key, val string, isDelete bool) {
	var id int32
	fmt.Sscanf(key, "/nodes/%d", &id)
    
    // ignore self
    if id == m.ServerID { return }

	m.Mu.Lock()
	defer m.Mu.Unlock()

	if isDelete {
		delete(m.Peers, id)
		log.Printf("[Membership] node %d left or crashed", id)
        if m.Callback != nil { m.Callback(id, "", false) }
	} else {
		m.Peers[id] = val
		log.Printf("[Membership] node %d joined at %s", id, val)
        if m.Callback != nil { m.Callback(id, val, true) }
	}
}