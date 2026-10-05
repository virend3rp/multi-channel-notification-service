import { Component, input } from '@angular/core';

/** Colored pill for a notification status, attempt outcome or channel. */
@Component({
  selector: 'app-badge',
  template: `<span class="badge" [attr.data-tone]="tone()">{{ label() }}</span>`,
  styles: `
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.02em;
      white-space: nowrap;
      background: var(--tone-neutral-bg);
      color: var(--tone-neutral-fg);
    }
    .badge[data-tone='good'] { background: var(--tone-good-bg); color: var(--tone-good-fg); }
    .badge[data-tone='info'] { background: var(--tone-info-bg); color: var(--tone-info-fg); }
    .badge[data-tone='warn'] { background: var(--tone-warn-bg); color: var(--tone-warn-fg); }
    .badge[data-tone='bad'] { background: var(--tone-bad-bg); color: var(--tone-bad-fg); }
  `,
})
export class StatusBadge {
  readonly label = input.required<string>();

  protected tone(): string {
    switch (this.label()) {
      case 'DELIVERED':
      case 'SUCCESS':
        return 'good';
      case 'SENT':
      case 'SENDING':
      case 'QUEUED':
        return 'info';
      case 'RETRYING':
      case 'RETRYABLE_ERROR':
      case 'CIRCUIT_OPEN':
        return 'warn';
      case 'FAILED':
      case 'DEAD_LETTERED':
      case 'PERMANENT_ERROR':
        return 'bad';
      default:
        return 'neutral';
    }
  }
}
