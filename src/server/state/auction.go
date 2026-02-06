package state

import (
	"sync"
    "os"
	"encoding/json"
    "fmt"
    "log"
)

type AuctionItem struct {

	ItemName   string  `json:"item_name"`
	ItemID     int32   `json:"item_id"`
	HighestBid float64 `json:"highest_bid"`
	
    WinnerID   int32   `json:"winner_id"`
	CreatorID  int32   `json:"creator_id"`
	IsOpen     bool    `json:"is_open"`
}




type Command struct {
	Type      CommandType `json:"type"`
	ItemName  string      `json:"item_name"`
	
    ItemID    int32       `json:"item_id"`
	UserID    int32       `json:"user_id"`
	Amount    float64     `json:"amount"`
	Timestamp int64       `json:"timestamp"`
}


type CommandType string
const (
    PlaceBid   CommandType = "PLACE_BID"
    CloseAuction CommandType = "CLOSE_AUCTION"
	CreateAuction CommandType = "CREATE_AUCTION"
	DeleteAuction CommandType = "DELETE_AUCTION"
    RegisterUser CommandType = "REGISTER"

)



type AuctionStateMachine struct {
    Items map[int32]*AuctionItem
    Users          map[int32]bool
    Mu    sync.Mutex

	Responses map[int64]chan string
	LastAppliedIdx int64
    NextItemID int32
    NextUserID int32
}


func NewAuctionStateMachine() *AuctionStateMachine {
    return &AuctionStateMachine{
        Items: make(map[int32]*AuctionItem),
        Users:      make(map[int32]bool),
		
        Responses: make(map[int64]chan string),
        NextItemID: 1,
        NextUserID: 1,
    }
}

// process a list of commands in the exact same order
func (state *AuctionStateMachine) ApplyBatch(commands []Command, logIdx0 int64) []string {
    
    results := make([]string, len(commands))

    for i, cmd := range commands {
		currentIdx:= logIdx0 + int64(i)
        results[i] = state.Apply(cmd,currentIdx) 
    }

    return results
}

//handles the execution of commands from the replicated log.
func (state *AuctionStateMachine) Apply(cmd Command, logIdx int64) string {
	state.Mu.Lock()
    defer state.Mu.Unlock()
    state.ensuremaps()

    var result string

	switch cmd.Type {
    
    case RegisterUser:
        result = state.handleRegister(cmd)
    case CreateAuction:
        result = state.handleCreate(cmd)
    case PlaceBid:
        result = state.handleBid(cmd)
    case CloseAuction:
        result = state.handleClose(cmd)
    case DeleteAuction:
        result = state.handleDelete(cmd)
    
    default:
        result = "Error: Unknown command"
    }


	state.LastAppliedIdx = logIdx

    if ch, ok := state.Responses[cmd.Timestamp]; ok {
        select {
		case ch <- result:
		default:
			log.Printf("Warning: handler for timestamp %d not listening", cmd.Timestamp)
		}
		delete(state.Responses, cmd.Timestamp) 
    }

    return result

}



//add item to state machine
func (state *AuctionStateMachine) handleCreate(cmd Command) string {

	if err := state.validateStates(cmd); err != "" {
        log.Printf("[SM Error] Validation failed for %s: %s", cmd.ItemName, err)
		return err
	}

    id := state.NextItemID
	state.NextItemID++

    state.Items[id] = &AuctionItem{
        ItemName:          cmd.ItemName,
        ItemID:            id,
        HighestBid:        cmd.Amount, 
        WinnerID:          -1,         
        CreatorID:         cmd.UserID,
        IsOpen:            true,
    }
    log.Printf("[SM SUCCESS] Added Item %d (%s). Total Items: %d", id, cmd.ItemName, len(state.Items))
    return fmt.Sprintf("Success: Auction created with ID: %d", id)
}

// update the highest bidder logic
func (state *AuctionStateMachine) handleBid(cmd Command) string {

	if err := state.validateStates(cmd); err != "" {
		return err
	}

	item := state.Items[cmd.ItemID]
    item.HighestBid = cmd.Amount
    item.WinnerID = cmd.UserID
    return "Success: Bid accepted"
}

// Closes the auction 
func (state *AuctionStateMachine) handleClose(cmd Command) string {

	if err := state.validateStates(cmd); err != "" {
		return err
	}

    state.Items[cmd.ItemID].IsOpen = false
    return "Success: Auction closed"
}

// Removes the auction
func (state *AuctionStateMachine) handleDelete(cmd Command) string {

	if err := state.validateStates(cmd); err != "" {
        return err
    }

    delete(state.Items, cmd.ItemID)
    return "Success: Auction deleted"
}

func (state *AuctionStateMachine) handleRegister(cmd Command) string {

    id := state.NextUserID
    state.NextUserID++

    state.Users[id] = true

    return fmt.Sprintf("Success: Registered. Your UserID is: %d", id)
}

//Validation rules

//is the command's data valid 
func (command *Command) Basic_Valid() string {

    if command.Type == RegisterUser {
        return ""
    }

    if command.Type == CreateAuction {
        if command.UserID <= 0 {
            return "Rejected: You must be registered to perform this action"
        }
        if command.ItemName == "" {
            return "rejected: Item name cannot be empty"
        }
        return "" 
    }

    if command.ItemID <= 0 {
        return "Rejected: Invalid item ID"
    }
    if command.Type == PlaceBid {
        if command.Amount <= 0 {
            return "Rejected: Bid amount must be positive"
        }
        if command.UserID <= 0 {
            return "Rejected: You must be registered to perform this action"
        }
    }
    return ""
}

//validations for different states
func (state *AuctionStateMachine) validateStates(cmd Command) string {
    
    if cmd.Type != RegisterUser {
        if _, exists := state.Users[cmd.UserID]; !exists {
            return "Rejected: User ID not recognized by the cluster"
        }
    }

    if cmd.Type == CreateAuction {
		return ""
	}

    item, exists := state.Items[cmd.ItemID]

    switch cmd.Type {
    case PlaceBid:
        if !exists {
            return "Rejected: Item not found"
        }

        if !item.IsOpen {
            return "Rejected: Auction closed"
        }

        // no self bidding
        if item.CreatorID == cmd.UserID {
            return "rejected: Creator cannot bid on their own auction"
        }
        // don't outbid yourself
        if item.WinnerID == cmd.UserID {
            return "Rejected: You are already the highest bidder"
        }

        //  price Check
        if cmd.Amount <= item.HighestBid {
            return "Rejected: Bid too low"
        }

    case CloseAuction, DeleteAuction:
        if !exists {
            return "Rejected: Item not found"
        }

        //permissions
        if item.CreatorID != cmd.UserID {
            return "Rejected: Unauthorized"
        }
    }
    return ""
}

// validates a command locally before proposing it to paxos state
func (state *AuctionStateMachine) PreCheck(cmd Command) (bool, string) {
    
	if err := cmd.Basic_Valid(); err != "" {
        return false, err
    }

	state.Mu.Lock()
    state.ensuremaps()
    defer state.Mu.Unlock()

    if cmd.Type == RegisterUser {
        return true, ""
    }

    if err := state.validateStates(cmd); err != "" {
        return false, err
    }

    return true, "" // success
}


//Read Functions

// Returns all items matching a name
func (state *AuctionStateMachine) FindItemByName(name string) []*AuctionItem {
	state.Mu.Lock()
    state.ensuremaps()

	defer state.Mu.Unlock()
	var results []*AuctionItem
	for _, item := range state.Items {
		if item.ItemName == name {
			results = append(results, item)
		}
	}
	return results
}

// Returns all items matching an id
func (state *AuctionStateMachine) FindItemByID(ID int32) (*AuctionItem, bool) {
    state.Mu.Lock()
    state.ensuremaps()
    defer state.Mu.Unlock()
    item, exists := state.Items[ID]
	if !exists {
        return nil, false
    }
    
    copy := *item 
    return &copy, true
}

//a list of all current auctions
func (state *AuctionStateMachine) GetAllItems() []*AuctionItem {
    state.Mu.Lock()
    state.ensuremaps()

    defer state.Mu.Unlock()

    var results []*AuctionItem

    for _, item := range state.Items {
        results = append(results, item)
    }
    return results
}


//Serializetion
//  Turn Command into bytes to send via Paxos gRPC
func (command *Command) Serialize() ([]byte, error) {
    return json.Marshal(command)
}

// Turn bytes from Paxos Log back into a command
func DeserializeCommand(data []byte) (Command, error) {
    var cmd Command
    err := json.Unmarshal(data, &cmd)
    return cmd, err
}

// DeserializeBatch parses a byte slice containing multiple command state
func DeserializeBatch(data []byte) ([]Command, error) {
	if len(data) == 0 {
        return []Command{}, nil
    }

    var commands []Command
    err := json.Unmarshal(data, &commands)

	if err == nil && commands == nil {
        return []Command{}, nil
    }
	
    return commands, err
}

//snapshot struct
type SnapshotData struct {
    Items      map[int32]*AuctionItem `json:"items"`
    Users      map[int32]bool         `json:"users"`
    NextID     int32                  `json:"next_id"`
    NextUserID int32                  `json:"next_user_id"`
    LastIndex  int32                  `json:"last_index"`

}

//save snapshot to memory
func (state *AuctionStateMachine) SaveSnapshot(filePath string, lastIdx int32) error {
    state.Mu.Lock()
    state.ensuremaps()
    //build snapshot
    datastruct := SnapshotData{
        Items:  state.Items,
        Users:      state.Users,

        NextID: state.NextItemID,
        NextUserID: state.NextUserID,

        LastIndex:  lastIdx,
    }

    //marshel state to json
    data, err := json.Marshal(datastruct)
    state.Mu.Unlock()

    if err != nil { return err }

    //temporary write 
    tempPath := filePath + ".tmp"
    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return err
    }
    //atomic replace
    return os.Rename(tempPath, filePath)
}

func (state *AuctionStateMachine) LoadSnapshot(filePath string) (int32, error) {
    // read data from storage
    data, err := os.ReadFile(filePath)
    
    if err != nil { 
        return 0, err 
    } 

    // lock 
    state.Mu.Lock()
    defer state.Mu.Unlock()

    var datastruct SnapshotData
    if err := json.Unmarshal(data, &datastruct); err != nil {
        return 0, err
    }

    //restore data from memory
    state.Items = datastruct.Items
    state.Users = datastruct.Users
    
     state.ensuremaps()

    state.NextItemID = datastruct.NextID
    state.NextUserID = datastruct.NextUserID

    //return log index
    return datastruct.LastIndex, nil
}

// safe gaured to avoid nil pointer panics
func (state *AuctionStateMachine) ensuremaps() {
	if state.Items == nil {
		state.Items = make(map[int32]*AuctionItem)
	}
    if state.Users == nil {
        state.Users = make(map[int32]bool) 
    }
	if state.Responses == nil {
		state.Responses = make(map[int64]chan string)
	}
}


func (state *AuctionStateMachine) GetSnapshotData(filePath string) ([]byte, int32, error) {
    // read file from disk 
    data, err := os.ReadFile(filePath)
    if err != nil {
        return nil, 0, err

    }

    // decode  to find the index
    var meta SnapshotData

    if err := json.Unmarshal(data, &meta); err != nil {
        return nil, 0, err
    }

    return data, meta.LastIndex, nil
}


