package storage

import (
	"encoding/json"
	"os"
	"sync"
    "paxos/paxos"
)

type WALManager struct {
	Path string
	Mu   sync.Mutex
	lastData []byte
	counter  int
}

func NewWALManager(path string) *WALManager {
	return &WALManager{Path: path}
}

//save important paxos variables to disk
func (w *WALManager) PersistState(instances map[int32]*paxos.PaxosInstance) error {
	w.Mu.Lock()

	//make  a snapshot copy 
	snapshot := make(map[int32]paxos.PaxosInstance)
	for k, v := range instances {
		if v != nil {
			instCopy := *v

			instCopy.Promises = make(map[int32]bool)
            for pk, pv := range v.Promises {
                instCopy.Promises[pk] = pv
            }
            instCopy.Accepts = make(map[int32]bool)
            for ak, av := range v.Accepts {
                instCopy.Accepts[ak] = av
            }
            snapshot[k] = instCopy
		}
	}
	w.Mu.Unlock()

	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	w.Mu.Lock()
	w.lastData = data
	w.counter++
	shouldSync := w.counter >= 10
    if shouldSync {
        w.counter = 0
    }
    w.Mu.Unlock()

    // atomic write
	if shouldSync {
        go w.syncToDisk(data) 
    }
    return nil
}

func (w *WALManager) syncToDisk(data []byte) {
	
	if len(data) == 0 {
		return
	}

	//save temporerly
	tempPath := w.Path + ".tmp"
	//atomic write
	if err := os.WriteFile(tempPath, data, 0644); err == nil {
			_ = os.Rename(tempPath, w.Path)
		}
}



//  recovers paxos instances from disk
func (w *WALManager) LoadState() (map[int32]*paxos.PaxosInstance, error) {
	//lock until the end
	w.Mu.Lock()
	defer w.Mu.Unlock()

	//read from path
	data, err := os.ReadFile(w.Path)
	//initialize empty state if not exist
	if os.IsNotExist(err) {
		return make(map[int32]*paxos.PaxosInstance), nil
	}

	if err != nil {
		return nil, err
	}

	//unmarshal
	var instances map[int32]*paxos.PaxosInstance
	err = json.Unmarshal(data, &instances)
	
	//return recovered instances
	return instances, err
}