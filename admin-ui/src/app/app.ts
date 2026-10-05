import { Component } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, RouterLink, RouterLinkActive],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {
  protected readonly nav = [
    { path: '/dashboard', label: 'Dashboard' },
    { path: '/logs', label: 'Delivery logs' },
    { path: '/templates', label: 'Templates' },
    { path: '/dlq', label: 'Dead letters' },
    { path: '/rate-limits', label: 'Rate limits' },
  ];
}
