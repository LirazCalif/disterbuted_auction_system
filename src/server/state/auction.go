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
    Mu    sync.Mutex

	Responses map[int64]chan string
	LastAppliedIdx int64
    NextItemID int32
    NextUserID int32
}

func NewAuctionStateMachine() *AuctionStateMachine {
    return &AuctionStateMachine{
        Items: make(map[int32]*AuctionItem),
		Responses: make(map[int64]chan string),
        NextItemID: 1,
        NextUserID: 1,
    }
}

// Process a list of commands in the exact same order
func (s *AuctionStateMachine) ApplyBatch(commands []Command, logIdx0 int64) []string {
    results := make([]string, len(commands))
    for i, cmd := range commands {
		currentIdx:= logIdx0 + int64(i)
        results[i] = s.Apply(cmd,currentIdx) 
    }
    return results
}

//handles the execution of commands from the replicated log.
func (s *AuctionStateMachine) Apply(cmd Command, logIdx int64) string {
	s.Mu.Lock()
    defer s.Mu.Unlock()
    s.ensuremaps()

    var result string

	switch cmd.Type {
    case RegisterUser:
        result = s.handleRegister(cmd)
    case CreateAuction:
        result = s.handleCreate(cmd)
    case PlaceBid:
        result = s.handleBid(cmd)
    case CloseAuction:
        result = s.handleClose(cmd)
    case DeleteAuction:
        result = s.handleDelete(cmd)
    default:
        result = "Error: Unknown command"
    }
	//Checks if an HTTP handler waiting for the result


	s.LastAppliedIdx = logIdx

    if ch, ok := s.Responses[cmd.Timestamp]; ok {
        select {
		case ch <- result:
		default:
			log.Printf("Warning: handler for timestamp %d not listening", cmd.Timestamp)
		}
		delete(s.Responses, cmd.Timestamp) 
    }

    return result

}



//add item to state machine
func (s *AuctionStateMachine) handleCreate(cmd Command) string {

	if err := s.validateStates(cmd); err != "" {
		return err
	}

    id := s.NextItemID
	s.NextItemID++

    s.Items[id] = &AuctionItem{
        ItemName:          cmd.ItemName,
        ItemID:            id,
        HighestBid:        cmd.Amount, 
        WinnerID:          -1,         // No winner
        CreatorID:         cmd.UserID,
        IsOpen:            true,
    }
    return fmt.Sprintf("Success: Auction created with ID: %d", id)
}

// update the highest bidder logic
func (s *AuctionStateMachine) handleBid(cmd Command) string {

	if err := s.validateStates(cmd); err != "" {
		return err
	}

	item := s.Items[cmd.ItemID]
    item.HighestBid = cmd.Amount
    item.WinnerID = cmd.UserID
    return "Success: Bid accepted"
}

// Closes the auction so no more bids can be placed.
// Only the Creator can close the auction
func (s *AuctionStateMachine) handleClose(cmd Command) string {

	if err := s.validateStates(cmd); err != "" {
		return err
	}

    s.Items[cmd.ItemID].IsOpen = false
    return "Success: Auction closed"
}

// Removes the auction
func (s *AuctionStateMachine) handleDelete(cmd Command) string {

	if err := s.validateStates(cmd); err != "" {
        return err
    }

    delete(s.Items, cmd.ItemID)
    return "Success: Auction deleted"
}

func (s *AuctionStateMachine) handleRegister(cmd Command) string {

    id := s.NextUserID
    s.NextUserID++

    return fmt.Sprintf("Success: Registered. Your UserID is: %d", id)
}

//Validation rules

//is the command's data valid  - stateless checks
func (c *Command) Basic_Valid() string {

    if c.Type == RegisterUser {
        return ""
    }

    if c.Type == CreateAuction {
        if c.UserID <= 0 {
            return "Rejected: You must be registered to perform this action"
        }
        if c.ItemName == "" {
            return "rejected: Item name cannot be empty"
        }
        return "" 
    }

    if c.ItemID <= 0 {
        return "Rejected: Invalid item ID"
    }
    if c.Type == PlaceBid {
        if c.Amount <= 0 {
            return "Rejected: Bid amount must be positive"
        }
        if c.UserID <= 0 {
            return "Rejected: You must be registered to perform this action"
        }
    }
    return ""
}

//validations for different states - stateful checks
func (s *AuctionStateMachine) validateStates(cmd Command) string {
    
    if cmd.Type == CreateAuction {
		return ""
	}

    item, exists := s.Items[cmd.ItemID]

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
            return "Rejected: Creator cannot bid on their own auction"
        }
        // don't outbid yourself
        if item.WinnerID == cmd.UserID {
            return "Rejected: You are already the highest bidder"
        }
        //  Price Check
        if cmd.Amount <= item.HighestBid {
            return "Rejected: Bid too low"
        }

    case CloseAuction, DeleteAuction:
        if !exists {
            return "Rejected: Item not found"
        }
        // Rule: Permissions
        if item.CreatorID != cmd.UserID {
            return "Rejected: Unauthorized"
        }
    }
    return ""
}

// validates a command locally before proposing it to Paxos.
func (s *AuctionStateMachine) PreCheck(cmd Command) (bool, string) {
    
	if err := cmd.Basic_Valid(); err != "" {
        return false, err
    }

	s.Mu.Lock()
    s.ensuremaps()
    defer s.Mu.Unlock()

    if cmd.Type == RegisterUser || cmd.Type == CreateAuction {
        return true, ""
    }

    if err := s.validateStates(cmd); err != "" {
        return false, err
    }

    return true, "" // Success
}


//Read Functions

// Returns all items matching a name.
func (s *AuctionStateMachine) FindItemByName(name string) []*AuctionItem {
	s.Mu.Lock()
    s.ensuremaps()

	defer s.Mu.Unlock()
	var results []*AuctionItem
	for _, item := range s.Items {
		if item.ItemName == name {
			results = append(results, item)
		}
	}
	return results
}

// Returns all items matching an id.
func (s *AuctionStateMachine) FindItemByID(ID int32) (*AuctionItem, bool) {
    s.Mu.Lock()
    s.ensuremaps()
    defer s.Mu.Unlock()
    item, exists := s.Items[ID]
	if !exists {
        return nil, false
    }
    
    copy := *item 
    return &copy, true
}

//a list of all current auctions
func (s *AuctionStateMachine) GetAllItems() []*AuctionItem {
    s.Mu.Lock()
    s.ensuremaps()
    defer s.Mu.Unlock()
    var results []*AuctionItem
    for _, item := range s.Items {
        results = append(results, item)
    }
    return results
}


//Serializetion
// Serialize: Turn Command into bytes to send via Paxos/gRPC
func (c *Command) Serialize() ([]byte, error) {
    return json.Marshal(c)
}

// Deserialize: Turn bytes from Paxos Log back into a Command
func DeserializeCommand(data []byte) (Command, error) {
    var cmd Command
    err := json.Unmarshal(data, &cmd)
    return cmd, err
}

// DeserializeBatch parses a byte slice containing multiple commands.
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

type SnapshotData struct {
    Items      map[int32]*AuctionItem `json:"items"`
    NextID     int32                  `json:"next_id"`
    NextUserID int32 `json:"next_user_id"`
}

func (s *AuctionStateMachine) SaveSnapshot(filePath string) error {
    s.Mu.Lock()
    s.ensuremaps()
    datastruct := SnapshotData{
        Items:  s.Items,
        NextID: s.NextItemID,
        NextUserID: s.NextUserID,
    }
    data, err := json.Marshal(datastruct)
    s.Mu.Unlock()

    if err != nil { return err }

    tempPath := filePath + ".tmp"
    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return err
    }
    return os.Rename(tempPath, filePath)
}

func (s *AuctionStateMachine) LoadSnapshot(filePath string) error {
    data, err := os.ReadFile(filePath)
    if err != nil { return err } 
    s.Mu.Lock()
    defer s.Mu.Unlock()

    var datastruct SnapshotData
    if err := json.Unmarshal(data, &datastruct); err != nil {
        return err
    }

    s.Items = datastruct.Items
    
     s.ensuremaps()

    s.NextItemID = datastruct.NextID
    s.NextUserID = datastruct.NextUserID
    return nil
}

func (s *AuctionStateMachine) ensuremaps() {
	if s.Items == nil {
		s.Items = make(map[int32]*AuctionItem)
	}
	if s.Responses == nil {
		s.Responses = make(map[int64]chan string)
	}
}



