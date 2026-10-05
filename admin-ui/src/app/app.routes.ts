import { Routes } from '@angular/router';

export const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: 'dashboard' },
  { path: 'dashboard', title: 'Dashboard', loadComponent: () => import('./pages/dashboard').then((m) => m.Dashboard) },
  { path: 'logs', title: 'Delivery logs', loadComponent: () => import('./pages/logs').then((m) => m.Logs) },
  { path: 'templates', title: 'Templates', loadComponent: () => import('./pages/templates').then((m) => m.Templates) },
  { path: 'dlq', title: 'Dead letters', loadComponent: () => import('./pages/dlq').then((m) => m.Dlq) },
  { path: 'rate-limits', title: 'Rate limits', loadComponent: () => import('./pages/rate-limits').then((m) => m.RateLimits) },
  { path: '**', redirectTo: 'dashboard' },
];
