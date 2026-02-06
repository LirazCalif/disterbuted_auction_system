import { Component, OnInit, OnDestroy, ChangeDetectorRef, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AuctionService } from '../../auction.service'; 
import { FormsModule } from '@angular/forms'; 
import { interval, Subscription, switchMap, filter } from 'rxjs';

@Component({
  selector: 'app-auctions-list',
  templateUrl: './auctions-list.component.html',
  styleUrls: ['./auctions-list.component.sass'],
  standalone: true,
  imports: [CommonModule, FormsModule] 
})
export class AuctionsListComponent implements OnInit, OnDestroy { 
  loading = true;
  auctions: any[] = []; 
  private pollSubscription?: Subscription; 
  private cdr = inject(ChangeDetectorRef);
  userHistory: any[] = [];
  
  searchNameStr: string = '';
  searchIdNum: number | null = null;
  isSearching = false;
  userId() {
    const id = localStorage.getItem('paxos_user_id');
    return id ? parseInt(id) : null;
  }
  checkUserHistory() {
    const uId = this.userId();
    if (!uId) return alert("please register first");

    console.log("getting history for user ID:", uId);
    this.loading = true;
    this.isSearching = true;

    this.auctionService.getUserById(uId).subscribe({
      next: (data) => {
        
        this.auctions = data.auctions_created || [];
   
        this.loading = false;
        this.cdr.detectChanges(); 
        if (this.userHistory.length === 0) {
          console.log("activity log of user is  empty"); 
        }

      },
      error: (err) => {
        this.loading = false;
        alert("failed to fetch history: " + (err.error?.message || err.message));
      }
    });
  }



  constructor(private auctionService: AuctionService) {}

  ngOnInit() {
    this.refreshList(); 

    // Auto-polling to reflect Paxos consensus every 3 seconds
    this.pollSubscription = interval(3000).pipe(filter(() => !this.isSearching),
      switchMap(() => this.auctionService.getActiveAuctions())
    ).subscribe({
      next: (data) => {
        this.updateLocalAuctions(data);
      },
      error: (err) => console.warn('cluster heartbeat waiting...')
    });
  }

  private updateLocalAuctions(data: any) {
    if (data && data.items) {
      this.auctions = data.items;
    } else {
      this.auctions = Array.isArray(data) ? data : (data ? [data] : []);
    }
    this.loading = false;
    this.cdr.detectChanges();
  }

  refreshList() {
    this.loading = true;
    this.isSearching = false;
    this.searchNameStr = '';
    this.searchIdNum = null;
    this.auctionService.getActiveAuctions().subscribe({
      next: (data) => {
        this.updateLocalAuctions(data);
      },
      error: (err) => {
        console.error('paxos cluster fetch failed:', err);
        this.loading = false; 
      }
    });
  }

  // restore search
  searchByName() {
    if (!this.searchNameStr) return this.refreshList();
    this.loading = true;
    this.isSearching = true;
    this.auctionService.getItemByName(this.searchNameStr).subscribe({
      next: (data) => {
        this.updateLocalAuctions(data);
      },
      error: (err) => {
        alert("Search failed: " + err.error);
        this.loading = false;
      }
    });
  }


  searchById() {
    if (!this.searchIdNum) return;
    this.loading = true;
    this.isSearching = true;
    this.auctionService.getItemById(this.searchIdNum).subscribe({
      next: (data) => {
        this.auctions = data ? [data] : [];
        this.loading = false;
        this.cdr.detectChanges();
      },
      error: (err) => {
        alert("ID search failed: " + err.error);
        this.loading = false;
      }
    });
  }

  // management logic
  placeBid(auction: any, userId: number, amount: number) {
    if (!userId || !amount) {
      alert("Please provide both User ID and Bid Amount");
      return;
    }
    this.auctionService.placeBid(auction.item_id, userId, amount).subscribe({
      next: () => {
        alert('Bid placed successfully');
        this.refreshList(); 
      },
      error: (err) => alert('Bid rejected: ' + (err.error?.message || err.error)) 
    });
  }

  deleteAuction(auction: any, userId: number) {
    if (!userId) return alert("User ID required to delete");
    this.auctionService.deleteAuction(auction.item_id, userId).subscribe({
      next: () => {
        alert('Delete request reached consensus');
        this.refreshList();
      },
      error: (err) => alert('Delete failed: ' + (err.error?.message || err.error))
    });
  }

  closeAuction(auction: any, userId: number) {
    if (!userId) return alert("User ID required to close auction");
    this.auctionService.closeAuction(auction.item_id, userId).subscribe({
      next: () => {
        alert('Auction closed successfully');
        this.refreshList();
      },
      error: (err) => alert('Close failed: ' + (err.error?.message || err.error))
    });
  }

  runStressTest() {
    const userId = Number(localStorage.getItem('paxos_user_id'));
    if (!userId || this.auctions.length === 0) return alert("Need a User ID and at least one active auction!");

    const batch = [];
    for (let i = 0; i < 20; i++) {
      batch.push({
        type: "PLACE_BID",
        item_id: this.auctions[0].item_id,
        user_id: userId,
        amount: Math.floor(Math.random() * 500) + (i * 10)
      });
    }

    this.auctionService.submitBatch(batch).subscribe({
      next: () => alert("batch proposal sent to leader's engine"),
      error: (err) =>{
        const errorMessage = err.error?.message || JSON.stringify(err.error) || err.message;
        alert("Batch failed: " + errorMessage);
      }
    });
  }

  verifyBidSync(itemId: number) {
    this.auctionService.getSyncBidStatus(itemId).subscribe({
      next: (data) => {
        alert(`Verified Fresh State: Item ${data.item_id} - Current High: $${data.amount} (Winner ID: ${data.winner})`);
      },
      error: (err) => alert("Linearizable sync failed: " + err.error)
    });
  }

  checkMyCreatorRevenue() {
    const userId = Number(localStorage.getItem('paxos_user_id'));
    if (!userId) return alert("Register first!");
    this.auctionService.getCreatorRevenueSync(userId).subscribe({
      next: (res) => {
        const total = res.total_value ?? res.TotalValue ?? 0;
        const count = res.item_count ?? res.ItemCount ?? 0;
        alert(`Your total revenue: $${res.total_value} across ${res.item_count} auctions.`);
      },
      error: (err) => alert("sync failed: " + err.error)
    });
  }

  ngOnDestroy() {
    if (this.pollSubscription) this.pollSubscription.unsubscribe();
  }
}