import { Component } from '@angular/core';

@Component({
  selector: 'app-auctions-list',
  templateUrl: './auctions-list.component.html'
})
export class AuctionsListComponent {
  // Example data, replace with real logic later
  auctions = [
    { id: 1, title: 'Auction 1', price: 100 },
    { id: 2, title: 'Auction 2', price: 200 }
  ];
}
