import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AuctionService } from '../../auction.service'; 

@Component({
  selector: 'app-auctions-list',
  templateUrl: './auctions-list.component.html',
  styleUrls: ['./auctions-list.component.sass'],
  standalone: true,
  imports: [CommonModule]
})
export class AuctionsListComponent implements OnInit { 
  loading = false;
  auctions: any[] = []; 

  constructor(private auctionService: AuctionService) {}

  ngOnInit() {
    this.refreshList(); 
  }

  refreshList() {
    this.loading = true;
    this.auctionService.getActiveAuctions().subscribe({
      next: (data) => {
        this.auctions = data;
        this.loading = false;
      },
      error: (err) => {
        console.error('Fetch failed:', err);
        this.loading = false;
      }
    });
  }

  placeBid(auction: any, userId: number, amount: number) {
    this.auctionService.placeBid(auction.item_id, userId, amount).subscribe({
      next: () => {
        alert('Bid placed successfully!');
        this.refreshList(); 
      },
      error: (err) => alert('Bid rejected: ' + err.error) 
    });
  }

  closeAuction(auction: any, userId: number) {
    this.auctionService.closeAuction(auction.item_id, userId).subscribe({
      next: () => this.refreshList(),
      error: (err) => alert('Close failed: ' + err.error)
    });
  }
}