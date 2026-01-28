import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { RouterModule } from '@angular/router';
import { routes } from './app.routes';
import { AuctionsListComponent } from './features/auctions-list/auctions-list.component';
import { CreateAuctionComponent } from './features/create-auction/create-auction.component';
import { provideHttpClient } from '@angular/common/http';

@NgModule({
  imports: [
    BrowserModule,
    RouterModule.forRoot(routes),
    AuctionsListComponent,
    CreateAuctionComponent
  ],
  providers: [provideHttpClient()],
  bootstrap: [AuctionsListComponent] 
})
export class AppModule { }