import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';





@Injectable({
  providedIn: 'root'
})
export class AuctionService {

  
   // The baseUrl points to the Nginx qateway
 
  private baseUrl = 'http://localhost/api';

  
   //direct connection to server0 
  
  private debugUrl = 'http://localhost:8080';

  constructor(private http: HttpClient) {}

  
  

  // register: proposes a new user entry to the multi paxos log
  register(): Observable<any> {
    return this.http.post(`${this.baseUrl}/register`, {});
  }


  // get user specific history
  getUserById(userId: number): Observable<any> {
    return this.http.get(`${this.baseUrl}/status/user_id?user_id=${userId}`);
  }


  // submits batch commands to the Leader
  submitBatch(commands: any[]): Observable<any> {
    return this.http.post(`${this.baseUrl}/batch`, commands);
  }

  // create auction 
  createAuction(itemName: string, userId: number, price: number): Observable<any> {
    const command = {
      item_name: itemName,
      user_id: userId,
      amount: price
    };
    return this.http.post(`${this.baseUrl}/create`, command);
  }



  // places a bid proposal on an active item
  placeBid(itemId: number, userId: number, amount: number): Observable<any> {
    const command = {
      item_id: itemId,
      user_id: userId,
      amount: amount
    };
    return this.http.post(`${this.baseUrl}/bid`, command);
  }

  // close auction 
  closeAuction(itemId: number, userId: number): Observable<any> {
    const command = {
      item_id: itemId,
      user_id: userId
    };
    return this.http.post(`${this.baseUrl}/close`, command);
  }


  //delete auction 
  deleteAuction(itemId: number, userId: number): Observable<any> {
    const command = { 
      item_id: itemId, 
      user_id: userId 
    };
    return this.http.post(`${this.baseUrl}/delete`, command);
  }


  // get active auctions
  getActiveAuctions(): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/active`);
  }



  // finds items matching a string
  getItemByName(name: string): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/status/name?name=${name}`);
  }

  //finds a specific item
  getItemById(itemId: number): Observable<any> {
    return this.http.get(`${this.baseUrl}/status/item_id?item_id=${itemId}`);
  }

  getSyncTotal(): Observable<any> {
    return this.http.get(`${this.baseUrl}/status/total/sync`);
  }

  getSyncBidStatus(itemId: number): Observable<any> {
    return this.http.get(`${this.baseUrl}/status/bid/sync?id=${itemId}`);
  }



  getCreatorRevenueSync(userId: number): Observable<any> {
    return this.http.get(`${this.baseUrl}/status/creator/total/sync?user_id=${userId}`);
  }




  // trigger election: forces a node to start a new campaign 
  triggerElection(): Observable<any> {
    return this.http.post(`${this.baseUrl}/system/elect-me`, {});
  }

  // returns the  EventSource for the monitor
  getLogStream(): EventSource {
    return new EventSource(`${this.debugUrl}/system/logs`);
  }



  triggerRestart(): Observable<any> {
  return this.http.post(`${this.baseUrl}/system/restart`, {});
}

}