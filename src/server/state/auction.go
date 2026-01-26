package state

import (
	"sync"
    "os"
	"encoding/json"
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

)

type AuctionStateMachine struct {
    Items map[int32]*AuctionItem
    Mu    sync.Mutex

	Responses map[int64]chan string
	LastAppliedIdx int64
}

func NewAuctionStateMachine() *AuctionStateMachine {
    return &AuctionStateMachine{
        Items: make(map[int32]*AuctionItem),
		Responses: make(map[int64]chan string),
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
	var result string

	switch cmd.Type {
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
	s.Mu.Lock()

	s.LastAppliedIdx = logIdx

    if ch, ok := s.Responses[cmd.Timestamp]; ok {
        ch <- result               
        delete(s.Responses, cmd.Timestamp) 
    }
    s.Mu.Unlock()

    return result

}



//add item to state machine
func (s *AuctionStateMachine) handleCreate(cmd Command) string {
    s.Mu.Lock()
    defer s.Mu.Unlock()

	if err := s.validateStates(cmd); err != "" {
		return err
	}

    s.Items[cmd.ItemID] = &AuctionItem{
        ItemName:          cmd.ItemName,
        ItemID:            cmd.ItemID,
        HighestBid:        cmd.Amount, 
        WinnerID:          -1,         // No winner
        CreatorID:         cmd.UserID,
        IsOpen:            true,
    }
    return "Success: Auction created"
}

// update the highest bidder logic
func (s *AuctionStateMachine) handleBid(cmd Command) string {
    s.Mu.Lock()
    defer s.Mu.Unlock()

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
    s.Mu.Lock()
    defer s.Mu.Unlock()

	if err := s.validateStates(cmd); err != "" {
		return err
	}

    s.Items[cmd.ItemID].IsOpen = false
    return "Success: Auction closed"
}

// Removes the auction
func (s *AuctionStateMachine) handleDelete(cmd Command) string {
    s.Mu.Lock()
    defer s.Mu.Unlock()

	if err := s.validateStates(cmd); err != "" {
        return err
    }

    delete(s.Items, cmd.ItemID)
    return "Success: Auction deleted"
}

//Validation rules

//is the command's data valid  - stateless checks
func (c *Command) Basic_Valid() string {
    if c.ItemID < 0 {
        return "Rejected: Invalid item ID"
    }
	if c.Type == CreateAuction && c.ItemName == "" {
        return "rejected: Item name cannot be empty"
    }
    if c.Type == PlaceBid {
        if c.Amount <= 0 {
            return "Rejected: Bid amount must be positive"
        }
        if c.UserID < 0 {
            return "Rejected: Invalid user ID"
        }
    }
    return ""
}

//validations for different states - stateful checks
func (s *AuctionStateMachine) validateStates(cmd Command) string {
    item, exists := s.Items[cmd.ItemID]

    switch cmd.Type {
    case CreateAuction:
        if exists {
            return "Rejected: Item ID already exists"
        }

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
    defer s.Mu.Unlock()

    if err := s.validateStates(cmd); err != "" {
        return false, err
    }

    return true, "" // Success
}


//Read Functions

// Returns all items matching a name.
func (s *AuctionStateMachine) FindItemByName(name string) []*AuctionItem {
	s.Mu.Lock()
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

func (s *AuctionStateMachine) SaveSnapshot(filePath string) error {
    s.Mu.Lock()
    defer s.Mu.Unlock()
    data, err := json.Marshal(s.Items)
    if err != nil { return err }
    return os.WriteFile(filePath, data, 0644)
}

func (s *AuctionStateMachine) LoadSnapshot(filePath string) error {
    data, err := os.ReadFile(filePath)
    if err != nil { return err } 
    s.Mu.Lock()
    defer s.Mu.Unlock()
    return json.Unmarshal(data, &s.Items)
}



