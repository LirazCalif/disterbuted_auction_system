import { Component, OnInit } from '@angular/core';
import { Auction } from '../../dtos/auction';
import { AuctionService } from '../../services/auction.service';

@Component({
  selector: 'app-auctions-list',
  templateUrl: './auctions-list.component.html',
  styleUrls: ['./auctions-list.component.sass'],
})
export class AuctionsListComponent implements OnInit {
  auctions: Auction[] = [];
  loading = true;

  constructor(private auctionService: AuctionService) {}

  ngOnInit(): void {
    this.load();
  }

  load() {
    this.auctionService.getAllAuctions().subscribe(data => {
      this.auctions = data;
      this.loading = false;
    });
  }

  placeBid(auction: Auction, userId: number, amount: number) {
    this.auctionService.sendCommand({
      type: 'PLACE_BID',
      item_id: auction.item_id,
      user_id: userId,
      amount: amount,
      timestamp: Date.now(),
    }).subscribe(() => this.load());
  }

  closeAuction(auction: Auction, userId: number) {
    this.auctionService.sendCommand({
      type: 'CLOSE_AUCTION',
      item_id: auction.item_id,
      user_id: userId,
      timestamp: Date.now(),
    }).subscribe(() => this.load());
  }
}
