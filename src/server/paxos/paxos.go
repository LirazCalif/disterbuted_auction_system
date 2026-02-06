package paxos

import (
	"log"
	"sync"
    "math"

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

// checks if proposalID can be promised.
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

//  checks if proposalID can be accepted and updates state.
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
    //for grid quorum
    Rows int
    Cols int
}

func NewMultiPaxosInstance(numServers int) *MultiPaxosInstance {
    rows := int(math.Sqrt(float64(numServers)))
	if rows == 0 { rows = 1 }
	cols := int(math.Ceil(float64(numServers) / float64(rows)))
    
    return &MultiPaxosInstance{
        Instances: make(map[int32]*PaxosInstance),
        NextIndex: 0,
		NumServers: numServers,
        Rows: rows,
        Cols: cols,

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

	inst, exists := m.Instances[id]
    if !exists {
        inst = &PaxosInstance{
            InstanceID: id,
            Promises:   make(map[int32]bool),
            Accepts:    make(map[int32]bool),
            NumServers: m.NumServers,
        }
        m.Instances[id] = inst
    }
	if inst.Promises == nil { inst.Promises = make(map[int32]bool) }
    if inst.Accepts == nil { inst.Accepts = make(map[int32]bool) }

	return inst
}


// GetQuorumType determines which logic to apply
func (m *MultiPaxosInstance) GetQuorumType() string {
	if m.NumServers <= 5 {
		return "MAJORITY"   // stage 1
	} else if m.NumServers <= 10 {
		return "ASYMMETRIC" // stage 2
	}
	return "GRID"           // stage 3
}

func (pi *PaxosInstance) HasElectionQuorum(m *MultiPaxosInstance) bool {
	qType := m.GetQuorumType()
	count := len(pi.Promises)

	switch qType {
	case "MAJORITY":
		return count > m.NumServers/2 
	case "ASYMMETRIC":
		return count >= (m.NumServers - 2)
	case "GRID":
		for c := 0; c < m.Cols; c++ {
			columnComplete := true
			for r := 0; r < m.Rows; r++ {
				serverID := int32(r*m.Cols + c)
				if serverID >= int32(m.NumServers) {
                    continue 
                }
				if !pi.Promises[serverID] {
					columnComplete = false
					break
				}
			}
			if columnComplete { return true }
		}
	}
	return false
}

func (pi *PaxosInstance) HasWriteQuorum(m *MultiPaxosInstance) bool {
	qType := m.GetQuorumType()
	count := len(pi.Accepts)

	switch qType {
	case "MAJORITY":
		return count > m.NumServers/2 
	case "ASYMMETRIC":
		return count >= 3
	case "GRID":		
		for r := 0; r < m.Rows; r++ {
			rowComplete := true
			for c := 0; c < m.Cols; c++ {
				serverID := int32(r*m.Cols + c )
				if serverID >= int32(m.NumServers) {
                    continue 
                }
				if !pi.Accepts[serverID] {
					rowComplete = false
					break
				}
			}
			if rowComplete { return true }
		}
	}
	return false
}


