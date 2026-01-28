// src/app/dtos/auction.ts
export interface Auction {
  itemId: number;        // int32
  itemName: string;
  highestBid: number;    // float64
  winnerId: number;      // int32
  creatorId: number;     // int32
  isOpen: boolean;
}
