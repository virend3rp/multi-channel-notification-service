import { DecimalPipe } from '@angular/common';
import { Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { Subject, switchMap, timer } from 'rxjs';
import { Api, ChannelStats, Status, errorMessage } from '../api.service';

interface Segment {
  key: string;
  label: string;
  statuses: Status[];
  color: string;
}

// Grouped by what an operator acts on. Colors are status semantics, so each one also has
// a legend entry, a tooltip and an exact count in the table: never color alone.
const SEGMENTS: Segment[] = [
  { key: 'delivered', label: 'Delivered', statuses: ['DELIVERED'], color: 'var(--bar-good)' },
  { key: 'sent', label: 'Sent (awaiting receipt)', statuses: ['SENT'], color: 'var(--bar-info)' },
  { key: 'inflight', label: 'Queued / sending', statuses: ['QUEUED', 'SENDING'], color: 'var(--bar-neutral)' },
  { key: 'retrying', label: 'Retrying', statuses: ['RETRYING'], color: 'var(--bar-warn)' },
  { key: 'failed', label: 'Failed / dead-lettered', statuses: ['FAILED', 'DEAD_LETTERED'], color: 'var(--bar-bad)' },
];

@Component({
  selector: 'app-dashboard',
  imports: [FormsModule, DecimalPipe],
  template: `
    <div class="toolbar">
      <div>
        <h1>Dashboard</h1>
        <p class="sub">Per-channel outcomes, refreshed every 5 seconds.</p>
      </div>
      <span class="spacer"></span>
      <label>
        Window
        <select [ngModel]="window()" (ngModelChange)="setWindow($event)">
          <option value="15m">Last 15 minutes</option>
          <option value="1h">Last hour</option>
          <option value="24h">Last 24 hours</option>
          <option value="168h">Last 7 days</option>
        </select>
      </label>
    </div>

    @if (error()) {
      <div class="error">{{ error() }}</div>
    }

    <div class="tiles">
      @for (s of stats(); track s.channel) {
        <section class="card tile">
          <header>
            <span class="dot" [attr.data-ch]="s.channel"></span>
            <h2>{{ s.channel }}</h2>
          </header>
          <div class="kpis">
            <div><span class="num">{{ total(s) | number }}</span><span class="lbl">accepted</span></div>
            <div><span class="num">{{ successRate(s) }}</span><span class="lbl">sent or delivered</span></div>
            <div><span class="num">{{ s.p95LatencyMs | number: '1.0-0' }}<small>ms</small></span><span class="lbl">p95 provider latency</span></div>
            <div>
              <span class="num" [class.alert]="s.dlqPending > 0">{{ s.dlqPending | number }}</span>
              <span class="lbl">awaiting replay</span>
            </div>
          </div>

          <div class="bar" role="img" [attr.aria-label]="ariaFor(s)">
            @for (seg of segments; track seg.key) {
              @if (count(s, seg) > 0) {
                <div
                  class="seg"
                  [style.flex-grow]="count(s, seg)"
                  [style.background]="seg.color"
                  [title]="seg.label + ': ' + count(s, seg) + ' (' + pct(s, seg) + ')'"
                ></div>
              }
            }
            @if (total(s) === 0) {
              <div class="seg empty-bar"></div>
            }
          </div>

          <table class="counts">
            <tbody>
              @for (seg of segments; track seg.key) {
                <tr>
                  <td><span class="swatch" [style.background]="seg.color"></span>{{ seg.label }}</td>
                  <td class="r">{{ count(s, seg) | number }}</td>
                  <td class="r muted">{{ pct(s, seg) }}</td>
                </tr>
              }
            </tbody>
          </table>

          <footer class="muted">
            {{ s.attempts | number }} provider attempts · {{ s.failedAttempts | number }} failed ·
            avg {{ s.avgLatencyMs | number: '1.0-0' }} ms
          </footer>
        </section>
      } @empty {
        <div class="card empty">Loading…</div>
      }
    </div>
  `,
  styles: `
    :host {
      --bar-good: #12b76a;
      --bar-info: #4c6ef5;
      --bar-neutral: #98a2b3;
      --bar-warn: #f79009;
      --bar-bad: #f04438;
    }
    @media (prefers-color-scheme: dark) {
      :host {
        --bar-good: #32d583;
        --bar-info: #7b93fd;
        --bar-neutral: #667085;
        --bar-warn: #fdb022;
        --bar-bad: #f97066;
      }
    }
    .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }
    .tile header { display: flex; align-items: center; gap: 8px; }
    .tile h2 { margin: 0; }
    .dot { width: 10px; height: 10px; border-radius: 50%; }
    .dot[data-ch='EMAIL'] { background: var(--c-email); }
    .dot[data-ch='SMS'] { background: var(--c-sms); }
    .dot[data-ch='PUSH'] { background: var(--c-push); }
    .kpis { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin: 16px 0; }
    .kpis > div { display: flex; flex-direction: column; }
    .num { font-size: 24px; font-weight: 700; font-variant-numeric: tabular-nums; }
    .num small { font-size: 13px; font-weight: 500; margin-left: 2px; color: var(--text-muted); }
    .num.alert { color: var(--tone-bad-fg); }
    .lbl { font-size: 12px; color: var(--text-muted); }
    .bar { display: flex; gap: 2px; height: 12px; margin-bottom: 12px; }
    .seg { min-width: 4px; height: 100%; border-radius: 4px; }
    .empty-bar { flex: 1; background: var(--surface-2); }
    .counts td { padding: 4px 0; border: 0; font-size: 13px; }
    .counts .r { text-align: right; font-variant-numeric: tabular-nums; padding-left: 12px; }
    .swatch { display: inline-block; width: 10px; height: 10px; border-radius: 3px; margin-right: 8px; vertical-align: -1px; }
    footer { margin-top: 12px; font-size: 12px; }
  `,
})
export class Dashboard {
  private api = inject(Api);
  protected readonly segments = SEGMENTS;
  protected readonly window = signal('1h');
  protected readonly stats = signal<ChannelStats[]>([]);
  protected readonly error = signal('');
  private readonly reload = new Subject<void>();

  protected readonly totals = computed(() => new Map(this.stats().map((s) => [s.channel, this.sum(s, [])])));

  constructor() {
    const destroyRef = inject(DestroyRef);
    // timer restarts whenever the window changes, so the new window loads immediately.
    this.reload
      .pipe(
        switchMap(() => timer(0, 5000)),
        switchMap(() => this.api.stats(this.window())),
        takeUntilDestroyed(destroyRef),
      )
      .subscribe({
        next: (r) => {
          this.stats.set(r.channels);
          this.error.set('');
        },
        error: (e) => this.error.set(errorMessage(e)),
      });
    this.reload.next();
  }

  protected setWindow(w: string): void {
    this.window.set(w);
    this.reload.next();
  }

  private sum(s: ChannelStats, statuses: Status[]): number {
    const entries = Object.entries(s.byStatus) as [Status, number][];
    return entries.filter(([k]) => statuses.length === 0 || statuses.includes(k)).reduce((a, [, v]) => a + v, 0);
  }

  protected total(s: ChannelStats): number {
    return this.totals().get(s.channel) ?? 0;
  }

  protected count(s: ChannelStats, seg: Segment): number {
    return this.sum(s, seg.statuses);
  }

  protected pct(s: ChannelStats, seg: Segment): string {
    const t = this.total(s);
    return t === 0 ? '–' : `${((this.count(s, seg) / t) * 100).toFixed(1)}%`;
  }

  protected successRate(s: ChannelStats): string {
    const t = this.total(s);
    if (t === 0) return '–';
    return `${((this.sum(s, ['SENT', 'DELIVERED']) / t) * 100).toFixed(1)}%`;
  }

  protected ariaFor(s: ChannelStats): string {
    return SEGMENTS.map((seg) => `${seg.label} ${this.count(s, seg)}`).join(', ');
  }
}
