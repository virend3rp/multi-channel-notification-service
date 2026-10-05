import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Api, RateLimit, errorMessage } from '../api.service';

@Component({
  selector: 'app-rate-limits',
  imports: [FormsModule],
  template: `
    <h1>Rate limits</h1>
    <p class="sub">
      Token bucket per channel. Workers reload these values every 15 seconds; no restart needed.
    </p>

    @if (error()) { <div class="error">{{ error() }}</div> }
    @if (notice()) { <div class="ok">{{ notice() }}</div> }

    <div class="grid">
      @for (r of limits(); track r.channel) {
        <form class="card" (ngSubmit)="save(r)">
          <h2>{{ r.channel }}</h2>
          <label>Permits per second
            <input type="number" min="0.1" step="0.1" [name]="'pps-' + r.channel" [(ngModel)]="r.permitsPerSec" required />
          </label>
          <label>Burst
            <input type="number" min="1" step="1" [name]="'burst-' + r.channel" [(ngModel)]="r.burst" required />
          </label>
          <p class="muted hint">≈ {{ perMinute(r) }} messages / minute sustained</p>
          <button class="primary" type="submit">Save</button>
        </form>
      }
    </div>
  `,
  styles: `
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; }
    form { display: flex; flex-direction: column; gap: 12px; }
    .hint { margin: 0; font-size: 12px; }
    button { align-self: flex-start; }
  `,
})
export class RateLimits {
  private api = inject(Api);
  protected readonly limits = signal<RateLimit[]>([]);
  protected readonly error = signal('');
  protected readonly notice = signal('');

  constructor() {
    this.api.rateLimits().subscribe({
      next: (l) => this.limits.set(l),
      error: (e) => this.error.set(errorMessage(e)),
    });
  }

  protected perMinute(r: RateLimit): string {
    return Math.round((r.permitsPerSec || 0) * 60).toLocaleString();
  }

  protected save(r: RateLimit): void {
    this.api.setRateLimit({ ...r, permitsPerSec: Number(r.permitsPerSec), burst: Number(r.burst) }).subscribe({
      next: () => {
        this.error.set('');
        this.notice.set(`${r.channel} limit saved.`);
      },
      error: (e) => this.error.set(errorMessage(e)),
    });
  }
}
