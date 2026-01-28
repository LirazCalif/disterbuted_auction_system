import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({ providedIn: 'root' })
export class ServerService {
  private url = 'http://localhost:8080';

  constructor(private http: HttpClient) {}

  getInfo() {
    return this.http.get(`${this.url}/info`, { responseType: 'text' });
  }
}
