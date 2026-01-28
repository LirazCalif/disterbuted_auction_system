import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AuctionService {
  // need to modify !!!
  private baseUrl = 'http://localhost:8080'; 

  constructor(private http: HttpClient) {}

  // POST /create
  createAuction(itemName: string, userId: number, price: number): Observable<any> {
    const command = {
      item_name: itemName,
      user_id: userId,
      amount: price
    };
    return this.http.post(`${this.baseUrl}/create`, command);
  }

  // POST /bid
  placeBid(itemId: number, userId: number, amount: number): Observable<any> {
    const command = {
      item_id: itemId,
      user_id: userId,
      amount: amount
    };
    return this.http.post(`${this.baseUrl}/bid`, command);
  }

  // GET /active
  getActiveAuctions(): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/active`);
  }

  // POST /close
  closeAuction(itemId: number, userId: number): Observable<any> {
    const command = {
      item_id: itemId,
      user_id: userId
    };
    return this.http.post(`${this.baseUrl}/close`, command);
  }
}