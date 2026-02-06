import { Component, OnInit, OnDestroy, ChangeDetectorRef, inject, PLATFORM_ID } from '@angular/core';
import { CommonModule, isPlatformBrowser } from '@angular/common';
import { HttpClient, HttpClientModule } from '@angular/common/http';
import { FilterOnlinePipe } from '../node-status-grid/filter-online.pipe'; 

export interface LogEntry {
  time: string;
  type: 'NODE' | 'LEADER' | 'QUORUM' | 'CONSENSUS' | 'REQUEST' | 'SYSTEM';
  message: string;
  sourceServer?: number;
}

export interface NodeState {
  id: number;
  appliedIndex: number;
  role: string;
  status: 'ONLINE' | 'OFFLINE';
  isLeader: boolean;
  lastSeen: number;
}

@Component({
  selector: 'app-paxos-monitor',
  standalone: true,
  imports: [CommonModule, HttpClientModule, FilterOnlinePipe],
  templateUrl: './paxos-monitor.component.html',
  styleUrl: './paxos-monitor.component.sass'
})
export class PaxosMonitorComponent implements OnInit, OnDestroy {
  private platformId = inject(PLATFORM_ID); 
  allLogs: LogEntry[] = [];
  filteredLogs: LogEntry[] = [];
  currentFilter: string = 'ALL';
  
  nodeStates: NodeState[] = Array.from({ length: 12 }, (_, i) => ({
    id: i,
    appliedIndex: -1,
    role: 'Follower',
    status: 'OFFLINE',
    isLeader: false,
    lastSeen: 0
  }));

  private ports = [8080, 8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088, 8089, 8090, 8091];
  private eventSources: EventSource[] = [];

  constructor(private http: HttpClient, private cdr: ChangeDetectorRef) {}

  ngOnInit() {
    if (isPlatformBrowser(this.platformId)) {
      // connection
      this.initializeCluster();
      this.startWatchdog();
    }
  }

  private initializeCluster() {
    this.ports.forEach((port, index) => {
      try {
        //tricks the browser into thinking these are different servers.
        const ip = `127.0.0.${index + 1}`; 
        const es = new EventSource(`http://${ip}:${port}/system/logs`);        
        es.onmessage = (event) => {
          const raw = event.data;
          
          // update grid
          if (raw.includes('[NODE]')) {
            this.syncGrid(raw);
          }

          // parse and add the log
          const logEntry = this.parseLog(raw);
          logEntry.sourceServer = index;
          
          this.allLogs.unshift(logEntry);
          
          // keep the log buffer high enough 
          if (this.allLogs.length > 5000) this.allLogs.pop();
          
          this.applyFilter(this.currentFilter);
          
          // ui update
          this.cdr.detectChanges();
        };

        es.onerror = () => {
          if (this.nodeStates[index]) {
            this.nodeStates[index].status = 'OFFLINE';
            this.cdr.detectChanges();
          }
        };

        this.eventSources.push(es);
      } catch (err) {
        console.error(`Stream error on SVR_${index}:`, err);
      }
    });
  }

  private startWatchdog() {
    setInterval(() => {
      const now = Date.now();
      let changed = false;
      this.nodeStates.forEach(n => {
        // If no heartbeat in 5 seconds, mark offline
        if (n.lastSeen > 0 && now - n.lastSeen > 5000 && n.status !== 'OFFLINE') {
          n.status = 'OFFLINE';
          n.isLeader = false;
          n.role = 'OFFLINE';
          changed = true;
        }
      });
      if (changed) this.cdr.detectChanges();
    }, 2000);
  }

  private syncGrid(raw: string) {
    try {
      const parts = raw.split('|');
      const id = parseInt(parts[0].match(/\d+/)?.[0] || '0');
      const idx = parseInt(parts[1].match(/-?\d+/)?.[0] || '0');
      const role = parts[2].split(':')[1].trim();

      const node = this.nodeStates[id];
      if (node) {
        node.appliedIndex = idx;
        node.role = role;
        node.isLeader = role.toUpperCase() === 'LEADER';
        node.status = 'ONLINE';
        node.lastSeen = Date.now();
      }
    } catch (e) { /* silent parse fail */ }
  }

  parseLog(raw: string): LogEntry {
    let type: 'NODE' | 'LEADER' | 'QUORUM' | 'CONSENSUS' | 'REQUEST' | 'SYSTEM' = 'SYSTEM';
    if (raw.includes('[CONSENSUS]')) type = 'CONSENSUS';
    else if (raw.includes('[QUORUM]')) type = 'QUORUM';
    else if (raw.includes('[LEADER]')) type = 'LEADER';
    else if (raw.includes('[NODE]')) type = 'NODE';
    else if (raw.includes('[REQUEST]')) type = 'REQUEST';

    return {
      time: new Date().toLocaleTimeString(),
      message: type === 'SYSTEM' ? raw : raw.replace(/\[.*?\]/, '').trim(),
      type: type
    };
  }

searchTerm: string = '';

applyFilter(filter: string, search: string = this.searchTerm) {
  this.currentFilter = filter;
  this.searchTerm = search;
  
  let logs = (filter === 'ALL') ? this.allLogs : this.allLogs.filter(l => l.type === filter);
  
  if (this.searchTerm) {
    logs = logs.filter(l => l.message.toLowerCase().includes(this.searchTerm.toLowerCase()));
  }
  
  this.filteredLogs = logs;
}


  clearLogs() {
    this.allLogs = [];
    this.filteredLogs = [];
    this.cdr.detectChanges();
  }

  ngOnDestroy() {
    this.eventSources.forEach(es => es.close());
  }
}