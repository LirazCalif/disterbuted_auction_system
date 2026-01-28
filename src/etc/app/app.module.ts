import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { RouterModule } from '@angular/router';

import { AppComponent } from './app.component';
import { AuctionsListComponent } from '../frontend/features/auctions-list/auctions-list.component';
import { CreateAuctionComponent } from '../frontend/features/create-auction/create-auction.component';
import { routes } from './app.routes';

@NgModule({
  declarations: [
    AppComponent,
    AuctionsListComponent,
    CreateAuctionComponent
  ],
  imports: [
    BrowserModule,
    RouterModule.forRoot(routes)
  ],
  bootstrap: [AppComponent]
})
export class AppModule {}
