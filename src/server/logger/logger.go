package logger

import (
	"fmt"
	"sync"
)

type EventHub struct {
	RawEvents chan string 
	Clients    map[chan string]bool 
	ClientsMu  sync.Mutex 
}

var Hub *EventHub 

func InitLogFilter() {
	Hub = &EventHub{
		RawEvents: make(chan string, 10000),
		Clients:    make(map[chan string]bool),
	}
	go Hub.run()
}

func (h *EventHub) run() {
	if h == nil { return }
	for msg := range h.RawEvents {
		h.ClientsMu.Lock()
		for clientChan := range h.Clients {
			select {
			case clientChan <- msg:
			default:
			}
		}
		h.ClientsMu.Unlock()
	}
}

func Emit(msg string) {
	if Hub == nil || Hub.RawEvents == nil { return }
	Hub.RawEvents <- msg
}

func EmitNodeStatus(id int, appliedIdx int64, isLeader bool) {
	role := "Follower"
	if isLeader {
		role = "LEADER"
	}
	msg := fmt.Sprintf("[NODE] Server %d | Applied Index: %d | Role: %s", id, appliedIdx, role)
	Emit(msg)
}