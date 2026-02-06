package api

import (
	"fmt"
	"net/http"
	"paxos/logger"
)

func (s *PaxosServer) HandleSSE(w http.ResponseWriter, r *http.Request) {
	if logger.Hub == nil {
		http.Error(w, "Logger not initialized", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	faucet := make(chan string, 1500)
	
	logger.Hub.ClientsMu.Lock()
	logger.Hub.Clients[faucet] = true
	logger.Hub.ClientsMu.Unlock()

	defer func() {
		if logger.Hub != nil {
			logger.Hub.ClientsMu.Lock()
			delete(logger.Hub.Clients, faucet)
			logger.Hub.ClientsMu.Unlock()
		}
		close(faucet)
	}()

	for {
		select {
		case msg := <-faucet:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}