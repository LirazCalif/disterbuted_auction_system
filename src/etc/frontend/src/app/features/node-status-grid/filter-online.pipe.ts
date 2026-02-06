import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'filterOnline',
  standalone: true
})
export class FilterOnlinePipe implements PipeTransform {
  transform(nodes: any[]): any[] {
    if (!nodes) return [];
    // filters nodes based on the status
    return nodes.filter(node => node.status === 'ONLINE');
  }
}