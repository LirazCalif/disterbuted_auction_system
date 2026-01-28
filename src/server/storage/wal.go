package storage

import (
	"encoding/json"
	"os"
	"sync"
    "paxos/src/server/paxos"
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

	data, err := json.Marshal(instances)
	if err != nil {
		w.Mu.Unlock()
		return err
	}
	w.lastData = data
	w.counter++

    // atomic write
	if w.counter >= 10 {
		w.counter = 0
		data, err := json.Marshal(instances)
        if err != nil {
            w.Mu.Unlock()
            return err
        }
        w.lastData = data
        w.Mu.Unlock()
        go w.syncToDisk() 
    } else {
        w.Mu.Unlock()
    }
    return nil
}

func (w *WALManager) syncToDisk() {
	w.Mu.Lock()
	data := w.lastData
	w.Mu.Unlock()

	if len(data) == 0 {
		return
	}

	tempPath := w.Path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err == nil {
			_ = os.Rename(tempPath, w.Path)
		}
}

//  recovers paxos instances from disk
func (w *WALManager) LoadState() (map[int32]*paxos.PaxosInstance, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	data, err := os.ReadFile(w.Path)
	if os.IsNotExist(err) {
		return make(map[int32]*paxos.PaxosInstance), nil
	}
	if err != nil {
		return nil, err
	}

	var instances map[int32]*paxos.PaxosInstance
	err = json.Unmarshal(data, &instances)
	return instances, err
}