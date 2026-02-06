package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
	"strconv"

	"paxos/state"
    "paxos/logger"
)


//initialize the state machine
func (server *PaxosServer) BuildAuctionStateMachine() {
	server.AuctionSM = state.NewAuctionStateMachine()
	log.Printf("[Server %d] Auction State Machine ready", server.ID)
}



// processes committed log entries
func (server *PaxosServer) ApplyToAuction(valStr string, logIdx int32) {
	valBytes := []byte(valStr)

	var auctionCommands []state.Command

    if strings.HasPrefix(strings.TrimSpace(valStr), "[") {
        if batchCmds, err := state.DeserializeBatch(valBytes); err == nil {
            auctionCommands = batchCmds
        }
    }


    if len(auctionCommands) == 0 {
        if cmd, err := state.DeserializeCommand(valBytes); err == nil {
            auctionCommands = append(auctionCommands, cmd)
        }
    }

	if len(auctionCommands) > 0 {
        logger.Emit(fmt.Sprintf("[CONSENSUS] Applying batch of %d commands to Auction SM at Index %d", len(auctionCommands), logIdx))
        server.AuctionSM.ApplyBatch(auctionCommands, int64(logIdx))
    }

}



// starts the REST API
func (server *PaxosServer) StartAuctionInterface(port string) {
	mux := http.NewServeMux()

	//writes
	mux.HandleFunc("/bid", server.HandleAuctionRequest)
	mux.HandleFunc("/create", server.HandleAuctionRequest)
	mux.HandleFunc("/close", server.HandleAuctionRequest)
	mux.HandleFunc("/delete", server.HandleAuctionRequest)
    mux.HandleFunc("/register", server.HandleRegisterRequest)

    mux.HandleFunc("/batch", server.HandleBatchRequest)

	// sequential read
	mux.HandleFunc("/status", server.HandleGetItems)
	mux.HandleFunc("/status/name", server.HandleFindByName)
	mux.HandleFunc("/status/item_id", server.HandleFindItemByID)
    mux.HandleFunc("/status/user_id", server.HandleFindUserByID)
	mux.HandleFunc("/active", server.HandleActiveAuctions)

	//linearzable read
	mux.HandleFunc("/status/bid/sync", server.HandleBidLinear)
    mux.HandleFunc("/status/total/sync", server.HandleTotalRevenueLinear)
    mux.HandleFunc("/status/creator/total/sync", server.HandleCreatorTotalRevenueLinear)

	//system
	mux.HandleFunc("/info",   server.HandleSysInfo)
    mux.HandleFunc("/system/logs", server.HandleSSE)

	mux.HandleFunc("/debug/peers", func(w http.ResponseWriter, r *http.Request) {
		server.Mu.Lock()
		defer server.Mu.Unlock()
		fmt.Fprintf(w, "Server %d\nPeers connected: %d\nNumServers config: %d\nQuorum Type: %s", 
			server.ID, len(server.Peers), server.MultiInstance.NumServers, server.MultiInstance.GetQuorumType())
	})
    
    wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Handle browser preflight
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		mux.ServeHTTP(w, r)
	})

    log.Printf("[Server %d] Auction REST API starting on port %s", server.ID, port)

	go func() {
        if err := http.ListenAndServe(":"+port, wrapper); err != nil {
            log.Fatalf("Failed to start HTTP server: %v", err)
        }
    }()
}



// handles POST requests
func (server *PaxosServer) HandleAuctionRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed, use POST.", http.StatusMethodNotAllowed)
		return
	}

	var cmd state.Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
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
    
    logger.Emit(fmt.Sprintf("[REQUEST] Client POST %s , User: %d , Item: %s", r.URL.Path, cmd.UserID, cmd.ItemName))



    // PreCheck
    if ok, msg := server.AuctionSM.PreCheck(cmd); !ok {

        http.Error(w, "state machine not ready: "+msg, http.StatusServiceUnavailable)

        return
    }


    cmdBytes, _ := cmd.Serialize()


    // setup Response Channel if Leader
    respChan := make(chan string, 1)

    server.AuctionSM.Mu.Lock()
    server.AuctionSM.Responses[cmd.Timestamp] = respChan

    server.AuctionSM.Mu.Unlock()

    // submit to Paxos Pipeline
    if err := server.SubmitRequest(cmdBytes); err != nil {

		server.AuctionSM.Mu.Lock()
		delete(server.AuctionSM.Responses, cmd.Timestamp)

		server.AuctionSM.Mu.Unlock()
		


		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return

	} else{
        logger.Emit(fmt.Sprintf("[SYSTEM] Proposal for %s accepted into Pipeline", cmd.Type))
    }

    // wait for Consensus and Application to State Machine
    select {
    case res := <-respChan:
       
        w.Header().Set("Content-Type", "application/json")
        
        if cmd.Type == "CREATE_AUCTION" {
            var itemID int
            
            n, _ := fmt.Sscanf(res, "Success: Auction created with ID: %d", &itemID)
            
            if n == 1 {
                fmt.Fprintf(w, `{"status": "Success", "item_id": %d}`, itemID)
                return
            }
        }
        
        fmt.Fprintf(w, `{"status": "Success", "message": "%s"}`, res)
    case <-time.After(30 * time.Second):
        server.AuctionSM.Mu.Lock()
        delete(server.AuctionSM.Responses, cmd.Timestamp)
        server.AuctionSM.Mu.Unlock()
        http.Error(w, "Timeout", http.StatusGatewayTimeout)
    }
}

func (server *PaxosServer) HandleGetItems(w http.ResponseWriter, r *http.Request) {
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET all auctions from %s", r.URL.Path))
    items := server.AuctionSM.GetAllItems()
    json.NewEncoder(w).Encode(items)
}

func (server *PaxosServer) HandleFindByName(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("name")
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET itembyname from %s", r.URL.Path))
    if name == "" {
        http.Error(w, "Missing name parameter", http.StatusBadRequest)
        return
    }
    items := server.AuctionSM.FindItemByName(name)
    json.NewEncoder(w).Encode(items)
}

func (server *PaxosServer) HandleFindItemByID(w http.ResponseWriter, r *http.Request) {
    idStr := r.URL.Query().Get("item_id")
    logger.Emit(fmt.Sprintf("[REQUEST] GET item_id=%s from %s", idStr, r.RemoteAddr))
    // convert string to int
    id, err := strconv.Atoi(idStr)
    if err != nil {
        logger.Emit(fmt.Sprintf("[REQUEST] GET failed: Invalid ID format '%s'", idStr))
        http.Error(w, "Invalid ID format", http.StatusBadRequest)
        return
    }

    // call your state machine function
    item, exists := server.AuctionSM.FindItemByID(int32(id))
    if !exists {
        logger.Emit(fmt.Sprintf("[REQUEST] GET failed: Item %d not found in state machine", id))
        http.Error(w, "Item not found", http.StatusNotFound)
        return
    }
    
    logger.Emit(fmt.Sprintf("[REQUEST] GET success: Item %d (%s) retrieved", id, item.ItemName))
    // return as JSON
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(item)
}

func (server *PaxosServer) HandleActiveAuctions(w http.ResponseWriter, r *http.Request) {
    all := server.AuctionSM.GetAllItems()
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET Active auctions from %s", r.URL.Path))
    var active []*state.AuctionItem

    for _, item := range all {
        if item.IsOpen {
            active = append(active, item)
        }
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(active)
}

//which auctions the creator open
func (server *PaxosServer) HandleStatusByCreator(w http.ResponseWriter, r *http.Request) {
    creatorID, _ := strconv.Atoi(r.URL.Query().Get("user_id"))
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET creator auctions from %s", r.URL.Path))
    all := server.AuctionSM.GetAllItems()
    var filtered []*state.AuctionItem
    
    for _, item := range all {
        if item.CreatorID == int32(creatorID) {
            filtered = append(filtered, item)
        }
    }
    json.NewEncoder(w).Encode(filtered) 

}

func (server *PaxosServer) HandleSysInfo(w http.ResponseWriter, r *http.Request) {
    logger.Emit(fmt.Sprintf("[REQUEST] GET system info from %s", r.URL.Path))
    server.AuctionSM.Mu.Lock()
    idx := server.AuctionSM.LastAppliedIdx
    server.AuctionSM.Mu.Unlock()
    
    fmt.Fprintf(w, "Server:%d , AppliedIndex:%d , Leader:%v", server.ID, idx, server.IsLeader())
}


//lineazable read
func (server *PaxosServer) HandleBidLinear(w http.ResponseWriter, r *http.Request) {
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET read Bid from auction from %s", r.URL.Path))
    if err := server.WaitUntilSynced(); err != nil {
        http.Error(w, "Linear sync failed", http.StatusServiceUnavailable)
        return
    }
    id, _ := strconv.Atoi(r.URL.Query().Get("id"))
    item, exists := server.AuctionSM.FindItemByID(int32(id))
    if !exists {
        http.Error(w, "Not found", http.StatusNotFound)
        return
    }
    json.NewEncoder(w).Encode(map[string]interface{}{
        "item_id": id, "winner": item.WinnerID, "amount": item.HighestBid,
    })
}

//total revenue accross all users
func (server *PaxosServer) HandleTotalRevenueLinear(w http.ResponseWriter, r *http.Request) {
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET total revenue from %s", r.URL.Path))
    if err := server.WaitUntilSynced(); err != nil {
        http.Error(w, "Linear sync failed", http.StatusServiceUnavailable)
        return
    }
    items := server.AuctionSM.GetAllItems()
    var total float64
    for _, item := range items {
        if item.WinnerID != -1 {
            total += item.HighestBid
        }
    }
    json.NewEncoder(w).Encode(map[string]float64{"total_system_value": total})
}


//total revenue for a creator
func (server *PaxosServer) HandleCreatorTotalRevenueLinear(w http.ResponseWriter, r *http.Request) {
    // block until local log matches leader's commit Index
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET total revenue for creator from %s",r.URL.Query().Get("user_id")))
    
    if err := server.WaitUntilSynced(); err != nil {
        log.Printf("Linear sync failed: %v", err)
        http.Error(w, "Linearizable sync failed", http.StatusServiceUnavailable)
        return
    }

    // get the creator ID 
    creatorIDStr := r.URL.Query().Get("user_id")
    creatorID, _ := strconv.Atoi(creatorIDStr)

    // calculate the sum from the synchronized state machine
    all := server.AuctionSM.GetAllItems()
    var totalValue float64 = 0
    var itemCount int = 0

    for _, item := range all {
        if item.CreatorID == int32(creatorID) && item.WinnerID != -1 {
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


// handles user registration requests
func (server *PaxosServer) HandleRegisterRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
		return
	}

	cmd := state.Command{
		Type:      state.RegisterUser,
		Timestamp: time.Now().UnixNano(),
	}

	if ok, msg := server.AuctionSM.PreCheck(cmd); !ok {
		    http.Error(w, "state machine not ready: "+msg, http.StatusServiceUnavailable)

		return
	}

	respChan := make(chan string, 1)
	server.AuctionSM.Mu.Lock()
	server.AuctionSM.Responses[cmd.Timestamp] = respChan
	server.AuctionSM.Mu.Unlock()

    cmdBytes, _ := cmd.Serialize()

	if err := server.SubmitRequest(cmdBytes); err != nil {
		server.AuctionSM.Mu.Lock()
		delete(server.AuctionSM.Responses, cmd.Timestamp)
		server.AuctionSM.Mu.Unlock()
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
    logger.Emit("[REQUEST] New User registration initiated")

	select {
	case res := <-respChan:
		var userID int
        fmt.Sscanf(res, "Success: Registered. Your UserID is: %d", &userID)
        logger.Emit(fmt.Sprintf("[NODE] Registration successful. User ID %d joined the grid", userID))
        w.Header().Set("Content-Type", "application/json")
        fmt.Fprintf(w, `{"status": "Success", "user_id": %d}`, userID)

	case <-time.After(15 * time.Second):
        server.AuctionSM.Mu.Lock()
        delete(server.AuctionSM.Responses, cmd.Timestamp)
        server.AuctionSM.Mu.Unlock()
        http.Error(w, "Timeout waiting for registration", http.StatusGatewayTimeout)	}
}

func (server *PaxosServer) HandleFindUserByID(w http.ResponseWriter, r *http.Request) {
    idStr := r.URL.Query().Get("user_id")
    logger.Emit(fmt.Sprintf("[REQUEST] Client GET, find user by ID from %s",idStr))

    user_id, err := strconv.Atoi(idStr)
    if err != nil {
        http.Error(w, "Invalid User ID", http.StatusBadRequest)
        return
    }

    all := server.AuctionSM.GetAllItems()
    var userItems []*state.AuctionItem
    
    for _, item := range all {
        if item.CreatorID == int32(user_id) {
            userItems = append(userItems, item)
        }
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "user_id": user_id,
        "auctions_created": userItems,
    })
}


func (server *PaxosServer) HandleBatchRequest(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
        return
    }

    var cmds []state.Command
    if err := json.NewDecoder(r.Body).Decode(&cmds); err != nil {
        http.Error(w, "Invalid JSON List", http.StatusBadRequest)
        return
    }

    if len(cmds) == 0 {
        http.Error(w, "Empty batch", http.StatusBadRequest)
        return
    }

    baseTime := time.Now().UnixNano()
     for i := range cmds {
        // ensure unique timestamps 
        cmds[i].Timestamp = baseTime + int64(i)
     }


    //precheck batch
    for _, cmd := range cmds {
        if ok, msg := server.AuctionSM.PreCheck(cmd); !ok {
            log.Printf("[REQUEST] Rejecting batch item: %s", msg)
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusBadRequest) 
            fmt.Fprintf(w, `{"error": "%s"}`, msg)
            return 
        }
    }

    // assign Timestamps and register response channels
    responseChans := make([]chan string, len(cmds))

    server.AuctionSM.Mu.Lock()
    for i := range cmds {
        
        // prepare channel
        respChan := make(chan string, 1)
        server.AuctionSM.Responses[cmds[i].Timestamp] = respChan
        responseChans[i] = respChan
    }
    server.AuctionSM.Mu.Unlock()

    logger.Emit(fmt.Sprintf("[REQUEST] Heavy Load Detected: Received Batch of %d commands", len(cmds)))


    fullBatchPayload, err := json.Marshal(cmds)
    if err != nil {
         http.Error(w, "Serialization failed", http.StatusInternalServerError)
         return
    }

    // submit request
    if err := server.SubmitRequest(fullBatchPayload); err != nil {

        server.AuctionSM.Mu.Lock()
        for _, cmd := range cmds {
            delete(server.AuctionSM.Responses, cmd.Timestamp)
        }

        server.AuctionSM.Mu.Unlock()

        http.Error(w, err.Error(), http.StatusServiceUnavailable)
        return
    } else {
        logger.Emit(fmt.Sprintf("[SYSTEM] Batch of %d pushed to Leader's Batching Engine", len(cmds)))
    }



    // wait for results
    results := make([]string, len(cmds))
    timeout := time.After(30 * time.Second)

    for i, ch := range responseChans {
        select {

        case res := <-ch:
            results[i] = res
        case <-timeout:
            server.AuctionSM.Mu.Lock()
            for j := i; j < len(cmds); j++ {
                delete(server.AuctionSM.Responses, cmds[j].Timestamp)
            }
            server.AuctionSM.Mu.Unlock()
            http.Error(w, "Timeout waiting for batch execution", http.StatusGatewayTimeout)
            return
        }
    }

    // return results
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{

        "status": "Success", 
        "batch_results": results,
    })
}

