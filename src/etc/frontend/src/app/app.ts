import { Component, signal, OnInit, inject, PLATFORM_ID } from '@angular/core'; 
import { RouterOutlet, RouterLink, RouterLinkActive } from '@angular/router';
import { CommonModule, isPlatformBrowser } from '@angular/common'; 
import { AuctionService } from './auction.service';
import { Routes } from '@angular/router';
import { AuctionsListComponent } from './features/auctions-list/auctions-list.component';
import { CreateAuctionComponent } from './features/create-auction/create-auction.component';
import { ApplicationConfig, provideBrowserGlobalErrorListeners } from '@angular/core';
import { provideRouter } from '@angular/router';

import { provideHttpClient, withFetch } from '@angular/common/http';

import { provideClientHydration, withEventReplay } from '@angular/platform-browser';

//route
export const routes: Routes = [
  { path: '', component: AuctionsListComponent },
  
  { path: 'create', component: CreateAuctionComponent },
  { path: '**', redirectTo: '' }
];

//config of app
export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    
    provideRouter(routes),
    provideHttpClient(withFetch()), 
    provideClientHydration(withEventReplay())
  ]
};


// import observability features
import { PaxosMonitorComponent } from './features/paxos-monitor/paxos-monitor.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    RouterOutlet, 
    RouterLink, 
    RouterLinkActive, 
    CommonModule,
    PaxosMonitorComponent   
  ], 
  templateUrl: './app.html',
  styleUrls: ['./app.sass']
})
export class App implements OnInit {
  private platformId = inject(PLATFORM_ID);
  title = signal('Auction System');

  isBrowser = false; 

  userId = signal<number | null>(null);
  globalRevenue = signal<number | null>(null);
  isBusy = signal(false); 
  constructor(private auctionService: AuctionService) {}

  ngOnInit() {
    this.isBrowser = isPlatformBrowser(this.platformId);

    // access local storage or browser APIs 
    if (this.isBrowser) {
      this.checkGlobalRevenue();
      const savedId = localStorage.getItem('paxos_user_id');
      if (savedId) {
        this.userId.set(parseInt(savedId));
      }
    }
  }

  // register user
  register() {
    
    this.isBusy.set(true);
    this.auctionService.register().subscribe({
      next: (res) => {
      
        const newId = res.user_id;
        this.userId.set(newId); 
        if (this.isBrowser) {
       
          localStorage.setItem('paxos_user_id', newId.toString());
        }
        this.isBusy.set(false);
        alert(`Registered successfully! ID: ${newId}`);
      },
      error: (err) => {
        this.isBusy.set(false);
        alert("Registration failed. Is the cluster running?");
      }
    });
  }

  // linearizable read
  checkGlobalRevenue() {
    this.isBusy.set(true);
    
    this.auctionService.getSyncTotal().subscribe({
      next: (res) => {
        console.log("Revenue Response:", res);
        this.globalRevenue.set(res.total_system_value);
      
        this.isBusy.set(false);
      },
      
      error: (err) => {
        this.isBusy.set(false);
        alert("Sync failed: " + (err.error?.message || "Check connection"));
      }
    });
  }


  logout() {
    
    if (this.isBrowser) {
      localStorage.removeItem('paxos_user_id');
      this.userId.set(null);
      window.location.reload();
    }
  }
}