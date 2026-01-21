package paxos

import (
	"log"
	"sync"

	pb "paxos/proto"

)


type PaxosInstance struct {
    InstanceID     int32
    MaxPromisedID  int64
    AcceptedID     int64
    AcceptedValue  []byte

	Promises       map[int32]bool
    Accepts        map[int32]bool
	NumServers int
	CommittedValue   []byte
	IsCommitted    bool

}

// Prepare_Instance: checks if proposalID can be promised.
func (pi *PaxosInstance) Prepare_Instance(proposalID int64) *pb.PromiseResponse {
	if proposalID >= pi.MaxPromisedID {
		pi.MaxPromisedID = proposalID
		return &pb.PromiseResponse{
			Promised:     true,
			AcceptedId:   pi.AcceptedID,
			AcceptedValue: pi.AcceptedValue,
		}
	}
	return &pb.PromiseResponse{
		Promised: false,
	}
}

// Accept_Instance: checks if proposalID can be accepted and updates state.
func (pi *PaxosInstance) Accept_Instance(proposalID int64, value []byte) *pb.AcceptedResponse {
	if proposalID >= pi.MaxPromisedID {
		pi.MaxPromisedID = proposalID
		pi.AcceptedID = proposalID
		pi.AcceptedValue = value
		return &pb.AcceptedResponse{Accepted: true}
	}
	return &pb.AcceptedResponse{Accepted: false}
}

// Commit applies a value 
func (pi *PaxosInstance) Commit(value []byte) {
	pi.CommittedValue = value
	pi.IsCommitted = true
	log.Println("Value committed:", string(value))
}


func (pi *PaxosInstance) HasPromiseQuorum() bool {
    return len(pi.Promises) > pi.NumServers/2
}

func (pi *PaxosInstance) HasAcceptQuorum() bool {
    return len(pi.Accepts) > pi.NumServers/2
}

func (pi *PaxosInstance) ResetRound() {
    pi.Promises = make(map[int32]bool)
    pi.Accepts  = make(map[int32]bool)
}



// MultiPaxox
type MultiPaxosInstance struct {
    Instances map[int32]*PaxosInstance 
    NextIndex int32
	NumServers int
    Mu sync.Mutex
}

func NewMultiPaxosInstance(numServers int) *MultiPaxosInstance {
    return &MultiPaxosInstance{
        Instances: make(map[int32]*PaxosInstance),
        NextIndex: 0,
		NumServers: numServers,
    }
}

// Create a new instance for the next log index
func (m *MultiPaxosInstance) NextInstance() int32 {
    m.Mu.Lock()
    defer m.Mu.Unlock()
    idx := m.NextIndex
    m.Instances[idx] = &PaxosInstance{
        InstanceID: idx,
        Promises:   make(map[int32]bool),
        Accepts:    make(map[int32]bool),
		NumServers: m.NumServers,
    }
    m.NextIndex++
    return idx
}

// Get instance by ID
func (m *MultiPaxosInstance) GetInstance(id int32) *PaxosInstance {
    m.Mu.Lock()
    defer m.Mu.Unlock()
    
    if _, exists := m.Instances[id]; !exists {
        m.Instances[id] = &PaxosInstance{
            InstanceID: id,
            Promises:   make(map[int32]bool),
            Accepts:    make(map[int32]bool),
            NumServers: m.NumServers, // Pass the global config
        }
    }
    return m.Instances[id]
}


