package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"strconv"

	"paxos/src/server/state"
)

//initializes the state machine
func (s *PaxosServer) BuildAuctionStateMachine() {
	s.AuctionSM = state.NewAuctionStateMachine()
	log.Printf("[Server %d] Auction State Machine ready", s.ID)
}

// processes committed log entries
func (s *PaxosServer) ApplyToAuction(valStr string, logIdx int32) {
	// "|" is a batch separator
	commands := strings.Split(valStr, "|")

	var auctionCommands []state.Command

	//filter out KV commands
	for _, piece := range commands {
        if strings.HasPrefix(piece, "{") {
            cmd, err := state.DeserializeCommand([]byte(piece))
            if err == nil {
                auctionCommands = append(auctionCommands, cmd)
            }
        }
    }
	if len(auctionCommands) > 0 {
        s.AuctionSM.ApplyBatch(auctionCommands, int64(logIdx))
    }

}

// starts the REST API on a separate port
func (s *PaxosServer) StartAuctionInterface(port string) {
	mux := http.NewServeMux()

	//writes
	mux.HandleFunc("/bid", s.HandleAuctionRequest)
	mux.HandleFunc("/create", s.HandleAuctionRequest)
	mux.HandleFunc("/close", s.HandleAuctionRequest)
	mux.HandleFunc("/delete", s.HandleAuctionRequest)

	// sequential read
	mux.HandleFunc("/status", s.HandleGetItems)
	mux.HandleFunc("/status/name", s.HandleFindByName)
	mux.HandleFunc("/status/id", s.HandleFindByID)
	mux.HandleFunc("/active", s.HandleActiveAuctions)

	//linearzable read
	mux.HandleFunc("/status/bid/sync", s.HandleBidLinear)
    mux.HandleFunc("/status/total/sync", s.HandleTotalRevenueLinear)
    mux.HandleFunc("/status/creator/total/sync", s.HandleCreatorTotalRevenueLinear)

	//system
	mux.HandleFunc("/info",   s.HandleSysInfo)

	log.Printf("[Server %d] Auction REST API starting on port %s", s.ID, port)

	go func() {
        if err := http.ListenAndServe(":"+port, mux); err != nil {
            log.Fatalf("Failed to start HTTP server: %v", err)
        }
    }()
}

// handles POST requests
func (s *PaxosServer) HandleAuctionRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}
	var cmd state.Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

    if cmd.Type == "" {
        switch r.URL.Path {
        case "/create":
            cmd.Type = "CREATE_AUCTION"
        case "/bid":
            cmd.Type = "PLACE_BID"
        case "/close":
            cmd.Type = "CLOSE_AUCTION"
        case "/delete":
            cmd.Type = "DELETE_AUCTION"
        }
    }

	cmd.Timestamp = time.Now().UnixNano()


    // PreCheck
    if ok, msg := s.AuctionSM.PreCheck(cmd); !ok {
        http.Error(w, msg, http.StatusBadRequest)
        return
    }

    cmdBytes, _ := cmd.Serialize()

    if !s.IsLeader() {
        if err := s.SubmitRequest(cmdBytes); err != nil {
            http.Error(w, err.Error(), http.StatusServiceUnavailable)
            return
        }
        w.Header().Set("Content-Type", "application/json")
        fmt.Fprint(w, `{"status": "Success", "message": "Forwarded & Executed"}`)
        return
    }

    // setup Response Channel if Leader
    respChan := make(chan string, 1)
    s.AuctionSM.Mu.Lock()
    s.AuctionSM.Responses[cmd.Timestamp] = respChan
    s.AuctionSM.Mu.Unlock()

    // submit to Paxos Pipeline
    if err := s.SubmitRequest(cmdBytes); err != nil {
		s.AuctionSM.Mu.Lock()
		delete(s.AuctionSM.Responses, cmd.Timestamp)
		s.AuctionSM.Mu.Unlock()
		
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

    // wait for Consensus and Application to State Machine
    select {
    case res := <-respChan:
        fmt.Fprint(w, res)
    case <-time.After(30 * time.Second):
        http.Error(w, "Timeout", http.StatusGatewayTimeout)
    }
}

func (s *PaxosServer) HandleGetItems(w http.ResponseWriter, r *http.Request) {
    items := s.AuctionSM.GetAllItems()
    json.NewEncoder(w).Encode(items)
}

func (s *PaxosServer) HandleFindByName(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("name")
    if name == "" {
        http.Error(w, "Missing name parameter", http.StatusBadRequest)
        return
    }
    items := s.AuctionSM.FindItemByName(name)
    json.NewEncoder(w).Encode(items)
}

func (s *PaxosServer) HandleFindByID(w http.ResponseWriter, r *http.Request) {
    idStr := r.URL.Query().Get("id")
    
    // convert string to int
    id, err := strconv.Atoi(idStr)
    if err != nil {
        http.Error(w, "Invalid ID format", http.StatusBadRequest)
        return
    }

    // call your state machine function
    item, exists := s.AuctionSM.FindItemByID(int32(id))
    if !exists {
        http.Error(w, "Item not found", http.StatusNotFound)
        return
    }

    // return as JSON
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(item)
}

func (s *PaxosServer) HandleActiveAuctions(w http.ResponseWriter, r *http.Request) {
    all := s.AuctionSM.GetAllItems()
    var active []*state.AuctionItem

    for _, item := range all {
        if item.IsOpen {
            active = append(active, item)
        }
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(active)
}

//what auctions a creator open
func (s *PaxosServer) HandleStatusByCreator(w http.ResponseWriter, r *http.Request) {
    creatorID, _ := strconv.Atoi(r.URL.Query().Get("user_id"))
    all := s.AuctionSM.GetAllItems()
    var filtered []*state.AuctionItem
    
    for _, item := range all {
        if item.CreatorID == int32(creatorID) {
            filtered = append(filtered, item)
        }
    }
    json.NewEncoder(w).Encode(filtered) 

}

func (s *PaxosServer) HandleSysInfo(w http.ResponseWriter, r *http.Request) {
    s.AuctionSM.Mu.Lock()
    idx := s.AuctionSM.LastAppliedIdx
    s.AuctionSM.Mu.Unlock()
    
    // showing if the server is synced
    fmt.Fprintf(w, "Server:%d , AppliedIndex:%d , Leader:%v", s.ID, idx, s.IsLeader())
}


//lineazable read
func (s *PaxosServer) HandleBidLinear(w http.ResponseWriter, r *http.Request) {
    if err := s.WaitUntilSynced(); err != nil {
        http.Error(w, "Linear sync failed", http.StatusServiceUnavailable)
        return
    }
    id, _ := strconv.Atoi(r.URL.Query().Get("id"))
    item, exists := s.AuctionSM.FindItemByID(int32(id))
    if !exists {
        http.Error(w, "Not found", http.StatusNotFound)
        return
    }
    // Return the "fresh" winner and amount 
    json.NewEncoder(w).Encode(map[string]interface{}{
        "item_id": id, "winner": item.WinnerID, "amount": item.HighestBid,
    })
}

//total revenue accross all users
func (s *PaxosServer) HandleTotalRevenueLinear(w http.ResponseWriter, r *http.Request) {
    if err := s.WaitUntilSynced(); err != nil {
        http.Error(w, "Linear sync failed", http.StatusServiceUnavailable)
        return
    }
    items := s.AuctionSM.GetAllItems()
    var total float64
    for _, item := range items {
        total += item.HighestBid
    }
    json.NewEncoder(w).Encode(map[string]float64{"total_system_value": total})
}


//total revenue for a creator
func (s *PaxosServer) HandleCreatorTotalRevenueLinear(w http.ResponseWriter, r *http.Request) {
    // block until local log matches leader's commit Index
    if err := s.WaitUntilSynced(); err != nil {
        log.Printf("Linear sync failed: %v", err)
        http.Error(w, "Linearizable sync failed", http.StatusServiceUnavailable)
        return
    }

    // get the creator ID 
    creatorIDStr := r.URL.Query().Get("user_id")
    creatorID, _ := strconv.Atoi(creatorIDStr)

    // calculate the sum from the synchronized state machine
    all := s.AuctionSM.GetAllItems()
    var totalValue float64 = 0
    var itemCount int = 0

    for _, item := range all {
        if item.CreatorID == int32(creatorID) {
            totalValue += item.HighestBid
            itemCount++
        }
    }

    // return the global revenue for the creator
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "user_id":     creatorID,
        "total_value": totalValue,
        "item_count":  itemCount,
        "consistency": "Linearizable",
    })
}