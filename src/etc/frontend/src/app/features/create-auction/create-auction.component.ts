import { Component, OnInit, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AuctionService } from '../../auction.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-create-auction',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './create-auction.component.html',
  styleUrls: ['./create-auction.component.sass']
})
export class CreateAuctionComponent implements OnInit {
  auction = { title: '', creatorID: 0, startingPrice: 0 };
  isSubmitting = signal(false); // prevents double-clicks
  successMessage: string = '';

  constructor(private auctionService: AuctionService, private router: Router) {}

  ngOnInit() {
    const savedId = localStorage.getItem('paxos_user_id');
    if (savedId) this.auction.creatorID = parseInt(savedId);
  }

  submitForm() {
    if (!this.auction.title || this.auction.creatorID <= 0) {
      alert("Please register for a Paxos ID first.");
      return;
    }

    this.isSubmitting.set(true);
    this.auctionService.createAuction(
      this.auction.title, 
      this.auction.creatorID, 
      this.auction.startingPrice
    ).subscribe({
      next: (res) => {
        this.successMessage = `consensus reached! ID: ${res.item_id}`;
        // go to list to watch the new item via polling
        setTimeout(() => this.router.navigate(['/']), 1500); 
      },
      error: (err) => {
        this.isSubmitting.set(false);
        alert("Consensus rejected: " + (err.error?.message || err.error));
      }
    });
  }
}