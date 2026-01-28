import { Component } from '@angular/core';

@Component({
  selector: 'app-create-auction',
  templateUrl: './create-auction.component.html'
})
export class CreateAuctionComponent {
  auction = {
    title: '',
    startingPrice: 0
  };

  createAuction() {
    console.log('Creating auction:', this.auction);
  }
}
