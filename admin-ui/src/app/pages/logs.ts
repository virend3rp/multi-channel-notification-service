import { DatePipe, JsonPipe } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Api, CHANNELS, LogFilter, Notification, STATUSES, errorMessage } from '../api.service';
import { StatusBadge } from '../shared/status-badge';

@Component({
  selector: 'app-logs',
  imports: [FormsModule, DatePipe, JsonPipe, StatusBadge],
  template: `
    <h1>Delivery logs</h1>
    <p class="sub">Every notification accepted by the API, newest first.</p>

    <form class="toolbar" (ngSubmit)="search()">
      <label>Channel
        <select name="channel" [(ngModel)]="filter.channel">
          <option value="">All</option>
          @for (c of channels; track c) { <option [value]="c">{{ c }}</option> }
        </select>
      </label>
      <label>Status
        <select name="status" [(ngModel)]="filter.status">
          <option value="">All</option>
          @for (s of statuses; track s) { <option [value]="s">{{ s }}</option> }
        </select>
      </label>
      <label>Recipient
        <input name="recipient" [(ngModel)]="filter.recipient" placeholder="exact match" />
      </label>
      <label>From
        <input name="from" type="datetime-local" [(ngModel)]="fromLocal" />
      </label>
      <label>To
        <input name="to" type="datetime-local" [(ngModel)]="toLocal" />
      </label>
      <button class="primary" type="submit">Search</button>
      <button type="button" (click)="clear()">Clear</button>
    </form>

    @if (error()) { <div class="error">{{ error() }}</div> }

    <div class="card flush">
      <table>
        <thead>
          <tr>
            <th>Created</th><th>Channel</th><th>Recipient</th><th>Template</th>
            <th>Status</th><th class="r">Attempts</th><th>Last error</th>
          </tr>
        </thead>
        <tbody>
          @for (n of rows(); track n.id) {
            <tr class="clickable" (click)="open(n.id)" [class.selected]="selected()?.id === n.id">
              <td class="nowrap">{{ n.createdAt | date: 'MMM d, HH:mm:ss' }}</td>
              <td>{{ n.channel }}</td>
              <td class="mono">{{ n.recipient }}</td>
              <td>{{ n.templateCode }} <span class="muted">v{{ n.templateVersion }}</span></td>
              <td><app-badge [label]="n.status" /></td>
              <td class="r">{{ n.attempts }}</td>
              <td class="muted trunc" [title]="n.lastError ?? ''">{{ n.lastError }}</td>
            </tr>
          } @empty {
            <tr><td colspan="7" class="empty">{{ loading() ? 'Loading…' : 'No notifications match these filters.' }}</td></tr>
          }
        </tbody>
      </table>
      <div class="pager">
        <span class="muted">{{ total() === 0 ? 0 : filter.offset + 1 }}–{{ filter.offset + rows().length }} of {{ total() }}</span>
        <span class="spacer"></span>
        <button (click)="page(-1)" [disabled]="filter.offset === 0">Previous</button>
        <button (click)="page(1)" [disabled]="filter.offset + filter.limit >= total()">Next</button>
      </div>
    </div>

    @if (selected(); as n) {
      <div class="scrim" (click)="selected.set(null)"></div>
      <aside class="drawer" role="dialog" aria-label="Notification detail">
        <header>
          <div>
            <h2>{{ n.channel }} to <span class="mono">{{ n.recipient }}</span></h2>
            <app-badge [label]="n.status" />
          </div>
          <button (click)="selected.set(null)" aria-label="Close">✕</button>
        </header>

        <dl>
          <dt>Notification ID</dt><dd class="mono">{{ n.id }}</dd>
          <dt>Message ID</dt><dd class="mono">{{ n.messageId }}</dd>
          <dt>Template</dt><dd>{{ n.templateCode }} v{{ n.templateVersion }}</dd>
          <dt>Priority</dt><dd>{{ n.priority }}</dd>
          <dt>Provider ref</dt><dd class="mono">{{ n.providerRef || '–' }}</dd>
          <dt>Created</dt><dd>{{ n.createdAt | date: 'medium' }}</dd>
          <dt>Updated</dt><dd>{{ n.updatedAt | date: 'medium' }}</dd>
        </dl>

        <h3>Attempts</h3>
        <ol class="timeline">
          @for (a of n.attemptsHistory ?? []; track a.id) {
            <li>
              <div class="row">
                <strong>#{{ a.attemptNo }}</strong>
                <app-badge [label]="a.outcome" />
                <span class="muted">{{ a.provider }} · {{ a.latencyMs }} ms</span>
                <span class="spacer"></span>
                <span class="muted">{{ a.attemptedAt | date: 'HH:mm:ss.SSS' }}</span>
              </div>
              @if (a.errorCode) {
                <div class="err mono">{{ a.errorCode }}: {{ a.errorMessage }}</div>
              }
            </li>
          } @empty {
            <li class="muted">No attempts yet.</li>
          }
        </ol>

        <h3>Rendered content</h3>
        @if (n.subject) { <p><strong>{{ n.subject }}</strong></p> }
        <pre class="content">{{ n.body }}</pre>

        @if (n.payload) {
          <h3>Template data</h3>
          <pre class="content">{{ n.payload | json }}</pre>
        }
      </aside>
    }
  `,
  styles: `
    .flush { padding: 0; overflow-x: auto; }
    .r { text-align: right; }
    .nowrap { white-space: nowrap; }
    .trunc { max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    tr.selected td { background: var(--surface-2); }
    .pager { display: flex; align-items: center; gap: 8px; padding: 10px 12px; }
    .scrim { position: fixed; inset: 0; background: rgba(0, 0, 0, 0.25); z-index: 10; }
    .drawer {
      position: fixed; top: 0; right: 0; bottom: 0; width: min(560px, 100vw);
      background: var(--surface); border-left: 1px solid var(--border);
      padding: 20px 24px; overflow-y: auto; z-index: 11;
      box-shadow: -8px 0 24px rgba(0, 0, 0, 0.12);
    }
    .drawer header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; }
    .drawer h2 { margin-bottom: 6px; }
    h3 { font-size: 13px; margin: 20px 0 8px; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-muted); }
    dl { display: grid; grid-template-columns: 120px 1fr; gap: 6px 12px; margin: 0; }
    dt { color: var(--text-muted); font-size: 12px; }
    dd { margin: 0; word-break: break-all; }
    .timeline { list-style: none; padding: 0; margin: 0; border-left: 2px solid var(--border); }
    .timeline li { padding: 6px 0 10px 14px; position: relative; }
    .timeline li::before {
      content: ''; position: absolute; left: -6px; top: 11px; width: 10px; height: 10px;
      border-radius: 50%; background: var(--surface); border: 2px solid var(--border);
    }
    .row { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .err { color: var(--tone-bad-fg); margin-top: 4px; word-break: break-word; }
    .content {
      background: var(--surface-2); border-radius: 6px; padding: 10px; white-space: pre-wrap;
      word-break: break-word; font-size: 12px; margin: 0;
    }
  `,
})
export class Logs {
  private api = inject(Api);
  protected readonly channels = CHANNELS;
  protected readonly statuses = STATUSES;

  protected filter: LogFilter = { channel: '', status: '', recipient: '', limit: 25, offset: 0 };
  protected fromLocal = '';
  protected toLocal = '';

  protected readonly rows = signal<Notification[]>([]);
  protected readonly total = signal(0);
  protected readonly loading = signal(false);
  protected readonly error = signal('');
  protected readonly selected = signal<Notification | null>(null);

  constructor() {
    this.load();
  }

  protected search(): void {
    this.filter.offset = 0;
    this.load();
  }

  protected clear(): void {
    this.filter = { channel: '', status: '', recipient: '', limit: 25, offset: 0 };
    this.fromLocal = this.toLocal = '';
    this.load();
  }

  protected page(dir: number): void {
    this.filter.offset = Math.max(0, this.filter.offset + dir * this.filter.limit);
    this.load();
  }

  protected open(id: string): void {
    this.api.notification(id).subscribe({
      next: (n) => this.selected.set(n),
      error: (e) => this.error.set(errorMessage(e)),
    });
  }

  private load(): void {
    this.loading.set(true);
    const f: LogFilter = {
      ...this.filter,
      from: this.fromLocal ? new Date(this.fromLocal).toISOString() : undefined,
      to: this.toLocal ? new Date(this.toLocal).toISOString() : undefined,
    };
    this.api.notifications(f).subscribe({
      next: (p) => {
        this.rows.set(p.items);
        this.total.set(p.total);
        this.error.set('');
        this.loading.set(false);
      },
      error: (e) => {
        this.error.set(errorMessage(e));
        this.loading.set(false);
      },
    });
  }
}
