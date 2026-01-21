package api

// import (
// 	"bufio"
// 	"os"
// 	"strings"
	
// 	"context"
// 	"log"
// 	"time"

// 	pb "paxos/proto"
// 	"google.golang.org/grpc"
// )

// // TestClient sends a prepare request and accept request
// func TestClient(addr string) {
// 	conn, err := grpc.Dial(addr, grpc.WithInsecure())
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer conn.Close()

// 	client := pb.NewPaxosClient(conn)

// 	// prepare phase
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()

// 	log.Println("sending prepare")
// 	prepareResp, err := client.Prepare(ctx, &pb.PrepareRequest{ProposalId: 1})
// 	if err != nil {
// 		log.Fatal("Prepare error:", err)
// 	}
// 	log.Println("Prepare Response:", prepareResp)

// 	// Only send accept if prepare successful
// 	if prepareResp.Promised {
// 		// accept phase
// 		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
// 		defer cancel2()

// 		value := []byte("Hello Paxos")
// 		log.Println("Sending Accept with value:", string(value))

// 		acceptResp, err := client.Accept(ctx2, &pb.AcceptRequest{
// 			ProposalId: 1,
// 			Value:      value,
// 		})
// 		if err != nil {
// 			log.Fatal("Accept error:", err)
// 		}
// 		log.Println("Accept Response:", acceptResp)
// 	}
// }
