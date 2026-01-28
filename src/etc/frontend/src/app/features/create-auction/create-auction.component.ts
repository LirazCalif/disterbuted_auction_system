import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AuctionService } from '../../auction.service'; 

@Component({
  selector: 'app-create-auction',
  templateUrl: './create-auction.component.html',
  styleUrls: ['./create-auction.component.sass'],
  standalone: true,
  imports: [CommonModule, FormsModule] 
})
export class CreateAuctionComponent {
  auction = {
    title: '',
    description: '',
    startingPrice: 0,
    creatorID: 0
  };

  successMessage = '';

  constructor(private auctionService: AuctionService) {}

  submitForm() {
    this.auctionService.createAuction(
      this.auction.title, 
      this.auction.creatorID, 
      this.auction.startingPrice
    ).subscribe({
      next: (response) => {
        this.successMessage = `Auction for "${this.auction.title}" has been broadcast! ID: ${response.item_id}`;
        
        this.auction = { title: '', description: '', startingPrice: 0, creatorID: 0 };
      },
      error: (err) => {
        alert('Broadcast failed: ' + (err.error?.message || err.message));
      }
    });
  }
}