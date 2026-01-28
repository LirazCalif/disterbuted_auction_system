import { Routes } from '@angular/router';
import { AuctionsListComponent } from '../frontend/features/auctions-list/auctions-list.component';
import { CreateAuctionComponent } from '../frontend/features/create-auction/create-auction.component';

export const routes: Routes = [
  { path: '', component: AuctionsListComponent },
  { path: 'create', component: CreateAuctionComponent }
];
