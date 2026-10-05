import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';

export type Channel = 'EMAIL' | 'SMS' | 'PUSH';
export type Status = 'QUEUED' | 'SENDING' | 'RETRYING' | 'SENT' | 'DELIVERED' | 'FAILED' | 'DEAD_LETTERED';

export const CHANNELS: Channel[] = ['EMAIL', 'SMS', 'PUSH'];
export const STATUSES: Status[] = ['QUEUED', 'SENDING', 'RETRYING', 'SENT', 'DELIVERED', 'FAILED', 'DEAD_LETTERED'];

export interface Template {
  id: string;
  code: string;
  channel: Channel;
  subject?: string;
  body: string;
  version: number;
  active: boolean;
  createdAt: string;
}

export interface DeliveryAttempt {
  id: string;
  attemptNo: number;
  provider: string;
  outcome: 'SUCCESS' | 'RETRYABLE_ERROR' | 'PERMANENT_ERROR' | 'CIRCUIT_OPEN';
  errorCode?: string;
  errorMessage?: string;
  latencyMs: number;
  attemptedAt: string;
}

export interface Notification {
  id: string;
  messageId: string;
  channel: Channel;
  recipient: string;
  templateCode: string;
  templateVersion: number;
  payload?: Record<string, unknown>;
  subject?: string;
  body: string;
  status: Status;
  priority: string;
  attempts: number;
  lastError?: string;
  providerRef?: string;
  createdAt: string;
  updatedAt: string;
  attemptsHistory?: DeliveryAttempt[];
}

export interface DlqMessage {
  id: string;
  notificationId: string;
  channel: Channel;
  reason: string;
  payload: string;
  createdAt: string;
  replayedAt?: string;
}

export interface Page<T> {
  items: T[];
  total: number;
}

export interface ChannelStats {
  channel: Channel;
  byStatus: Partial<Record<Status, number>>;
  attempts: number;
  failedAttempts: number;
  p95LatencyMs: number;
  avgLatencyMs: number;
  dlqPending: number;
}

export interface RateLimit {
  channel: Channel;
  permitsPerSec: number;
  burst: number;
}

export interface LogFilter {
  channel?: string;
  status?: string;
  recipient?: string;
  from?: string;
  to?: string;
  limit: number;
  offset: number;
}

const BASE = '/api/v1';
const ADMIN = `${BASE}/admin`;

function params(obj: object): HttpParams {
  let p = new HttpParams();
  for (const [k, v] of Object.entries(obj)) {
    if (v !== undefined && v !== null && v !== '') p = p.set(k, String(v));
  }
  return p;
}

@Injectable({ providedIn: 'root' })
export class Api {
  private http = inject(HttpClient);

  stats(window: string): Observable<{ window: string; channels: ChannelStats[] }> {
    return this.http.get<{ window: string; channels: ChannelStats[] }>(`${ADMIN}/stats`, { params: params({ window }) });
  }

  notifications(f: LogFilter): Observable<Page<Notification>> {
    return this.http.get<Page<Notification>>(`${ADMIN}/notifications`, { params: params(f) });
  }

  notification(id: string): Observable<Notification> {
    return this.http.get<Notification>(`${BASE}/notifications/${id}`);
  }

  templates(): Observable<Template[]> {
    return this.http.get<Template[]>(`${ADMIN}/templates`);
  }

  templateVersions(code: string): Observable<Template[]> {
    return this.http.get<Template[]>(`${ADMIN}/templates/${encodeURIComponent(code)}`);
  }

  createTemplate(t: { code: string; channel: Channel; subject?: string; body: string }): Observable<Template> {
    return this.http.post<Template>(`${ADMIN}/templates`, t);
  }

  updateTemplate(code: string, t: { subject?: string; body: string }): Observable<Template> {
    return this.http.put<Template>(`${ADMIN}/templates/${encodeURIComponent(code)}`, t);
  }

  activateTemplate(code: string, version: number): Observable<void> {
    return this.http.post<void>(`${ADMIN}/templates/${encodeURIComponent(code)}/versions/${version}/activate`, {});
  }

  preview(req: { channel: Channel; subject?: string; body: string; data: unknown }): Observable<{ subject?: string; body: string }> {
    return this.http.post<{ subject?: string; body: string }>(`${ADMIN}/templates/preview`, req);
  }

  dlq(includeReplayed: boolean, limit: number, offset: number): Observable<Page<DlqMessage>> {
    return this.http.get<Page<DlqMessage>>(`${ADMIN}/dlq`, { params: params({ includeReplayed, limit, offset }) });
  }

  replay(id: string): Observable<{ notificationId: string }> {
    return this.http.post<{ notificationId: string }>(`${ADMIN}/dlq/${id}/replay`, {});
  }

  replayMany(ids: string[]): Observable<{ id: string; ok: boolean; error?: string }[]> {
    return this.http.post<{ id: string; ok: boolean; error?: string }[]>(`${ADMIN}/dlq/replay`, { ids });
  }

  rateLimits(): Observable<RateLimit[]> {
    return this.http.get<RateLimit[]>(`${ADMIN}/rate-limits`);
  }

  setRateLimit(r: RateLimit): Observable<RateLimit> {
    return this.http.put<RateLimit>(`${ADMIN}/rate-limits/${r.channel}`, { permitsPerSec: r.permitsPerSec, burst: r.burst });
  }
}

/** Extracts the API's error message from an HttpErrorResponse. */
export function errorMessage(err: unknown): string {
  const e = err as { error?: { message?: string }; message?: string; status?: number };
  if (e?.error?.message) return e.error.message;
  if (e?.status === 0) return 'API unreachable';
  return e?.message ?? 'Unexpected error';
}
