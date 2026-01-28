import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Auction } from '../dtos/auction';
import { Command } from '../dtos/command';
import { Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class AuctionService {
  private url = 'http://localhost:8080';

  constructor(private http: HttpClient) {}

  getAllAuctions(): Observable<Auction[]> {
    return this.http.get<Auction[]>(`${this.url}/auctions`);
  }

  sendCommand(cmd: Command): Observable<string> {
    return this.http.post(
      `${this.url}/command`,
      cmd,
      { responseType: 'text' }
    );
  }
}
