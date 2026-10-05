import { DatePipe } from '@angular/common';
import { Component, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Api, DlqMessage, errorMessage } from '../api.service';

interface Row extends DlqMessage {
  recipient: string;
}

@Component({
  selector: 'app-dlq',
  imports: [FormsModule, DatePipe],
  template: `
    <div class="toolbar">
      <div>
        <h1>Dead letters</h1>
        <p class="sub">Notifications that used up their retries. Replaying one re-queues it with a fresh attempt budget.</p>
      </div>
      <span class="spacer"></span>
      <label class="check">
        <input type="checkbox" [ngModel]="includeReplayed()" (ngModelChange)="includeReplayed.set($event); load()" />
        Show replayed
      </label>
      <button class="primary" (click)="replaySelected()" [disabled]="selectedIds().length === 0 || busy()">
        Replay selected ({{ selectedIds().length }})
      </button>
    </div>

    @if (error()) { <div class="error">{{ error() }}</div> }
    @if (notice()) { <div class="ok">{{ notice() }}</div> }

    <div class="card flush">
      <table>
        <thead>
          <tr>
            <th class="w">
              <input type="checkbox" aria-label="Select all" [checked]="allSelected()" (change)="toggleAll()" />
            </th>
            <th>Dead-lettered</th><th>Channel</th><th>Recipient</th><th>Reason</th><th></th>
          </tr>
        </thead>
        <tbody>
          @for (m of rows(); track m.id) {
            <tr>
              <td class="w">
                @if (!m.replayedAt) {
                  <input type="checkbox" [attr.aria-label]="'Select ' + m.id" [checked]="selected().has(m.id)" (change)="toggle(m.id)" />
                }
              </td>
              <td class="nowrap">{{ m.createdAt | date: 'MMM d, HH:mm:ss' }}</td>
              <td>{{ m.channel }}</td>
              <td class="mono">{{ m.recipient }}</td>
              <td class="reason">
                {{ m.reason }}
                <button class="link" (click)="toggleExpanded(m.id)">{{ expanded() === m.id ? 'Hide' : 'Payload' }}</button>
                @if (expanded() === m.id) { <pre>{{ pretty(m.payload) }}</pre> }
              </td>
              <td class="nowrap">
                @if (m.replayedAt) {
                  <span class="muted">replayed {{ m.replayedAt | date: 'HH:mm:ss' }}</span>
                } @else {
                  <button (click)="replay(m)" [disabled]="busy()">Replay</button>
                }
              </td>
            </tr>
          } @empty {
            <tr><td colspan="6" class="empty">The dead-letter queue is empty.</td></tr>
          }
        </tbody>
      </table>
      <div class="pager">
        <span class="muted">{{ total() }} message(s)</span>
        <span class="spacer"></span>
        <button (click)="page(-1)" [disabled]="offset() === 0">Previous</button>
        <button (click)="page(1)" [disabled]="offset() + limit >= total()">Next</button>
      </div>
    </div>
  `,
  styles: `
    .flush { padding: 0; overflow-x: auto; }
    .w { width: 32px; }
    .nowrap { white-space: nowrap; }
    .reason { max-width: 520px; word-break: break-word; }
    .check { flex-direction: row; align-items: center; gap: 6px; color: var(--text); font-size: 13px; }
    .link { border: 0; background: none; color: var(--accent); padding: 0 4px; font-size: 12px; }
    pre {
      background: var(--surface-2); padding: 8px; border-radius: 6px; font-size: 12px;
      white-space: pre-wrap; word-break: break-word; margin: 6px 0 0;
    }
    .pager { display: flex; align-items: center; gap: 8px; padding: 10px 12px; }
  `,
})
export class Dlq {
  private api = inject(Api);
  protected readonly limit = 50;

  protected readonly rows = signal<Row[]>([]);
  protected readonly total = signal(0);
  protected readonly offset = signal(0);
  protected readonly includeReplayed = signal(false);
  protected readonly selected = signal(new Set<string>());
  protected readonly expanded = signal<string | null>(null);
  protected readonly busy = signal(false);
  protected readonly error = signal('');
  protected readonly notice = signal('');

  protected readonly selectedIds = computed(() => [...this.selected()]);
  protected readonly allSelected = computed(() => {
    const open = this.rows().filter((r) => !r.replayedAt);
    return open.length > 0 && open.every((r) => this.selected().has(r.id));
  });

  constructor() {
    this.load();
  }

  load(): void {
    this.api.dlq(this.includeReplayed(), this.limit, this.offset()).subscribe({
      next: (p) => {
        this.rows.set(p.items.map((m) => ({ ...m, recipient: this.recipientOf(m.payload) })));
        this.total.set(p.total);
        this.selected.set(new Set());
        this.error.set('');
      },
      error: (e) => this.error.set(errorMessage(e)),
    });
  }

  protected page(dir: number): void {
    this.offset.set(Math.max(0, this.offset() + dir * this.limit));
    this.load();
  }

  protected toggle(id: string): void {
    this.selected.update((s) => {
      const next = new Set(s);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  protected toggleAll(): void {
    const open = this.rows().filter((r) => !r.replayedAt).map((r) => r.id);
    this.selected.set(this.allSelected() ? new Set() : new Set(open));
  }

  protected toggleExpanded(id: string): void {
    this.expanded.update((cur) => (cur === id ? null : id));
  }

  protected replay(m: Row): void {
    this.busy.set(true);
    this.api.replay(m.id).subscribe({
      next: () => {
        this.busy.set(false);
        this.notice.set(`Re-queued notification ${m.notificationId}.`);
        this.load();
      },
      error: (e) => {
        this.busy.set(false);
        this.error.set(errorMessage(e));
      },
    });
  }

  protected replaySelected(): void {
    this.busy.set(true);
    this.api.replayMany(this.selectedIds()).subscribe({
      next: (results) => {
        this.busy.set(false);
        const ok = results.filter((r) => r.ok).length;
        const failed = results.length - ok;
        this.notice.set(`Re-queued ${ok} notification(s)${failed ? `; ${failed} skipped` : ''}.`);
        this.load();
      },
      error: (e) => {
        this.busy.set(false);
        this.error.set(errorMessage(e));
      },
    });
  }

  protected pretty(payload: string): string {
    try {
      return JSON.stringify(JSON.parse(payload), null, 2);
    } catch {
      return payload;
    }
  }

  private recipientOf(payload: string): string {
    try {
      return (JSON.parse(payload) as { recipient?: string }).recipient ?? '';
    } catch {
      return '';
    }
  }
}
