import { DatePipe } from '@angular/common';
import { Component, DestroyRef, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { Subject, catchError, debounceTime, of, switchMap } from 'rxjs';
import { Api, CHANNELS, Channel, Template, errorMessage } from '../api.service';

interface Draft {
  code: string;
  channel: Channel;
  subject: string;
  body: string;
  sampleData: string;
}

const SAMPLE_DATA: Record<string, string> = {
  'welcome-email': '{\n  "name": "Ada",\n  "email": "ada@example.com"\n}',
  'otp-sms': '{\n  "code": "482913",\n  "minutes": 10\n}',
  'order-shipped-push': '{\n  "orderId": "A-1001",\n  "eta": "Thursday"\n}',
};

/** Finds {{variable}} names so new templates get a sample-data skeleton. */
function variablesOf(...texts: string[]): string[] {
  const names = new Set<string>();
  for (const t of texts) {
    for (const m of t.matchAll(/{{\s*[#^/]?\s*([\w.]+)\s*}}/g)) names.add(m[1]);
  }
  return [...names].filter((n) => n !== '.');
}

@Component({
  selector: 'app-templates',
  imports: [FormsModule, DatePipe],
  template: `
    <div class="toolbar">
      <div>
        <h1>Templates</h1>
        <p class="sub">Mustache templates. Saving creates a new version; older versions stay available for rollback.</p>
      </div>
      <span class="spacer"></span>
      <button class="primary" (click)="startNew()">New template</button>
    </div>

    @if (error()) { <div class="error">{{ error() }}</div> }
    @if (notice()) { <div class="ok">{{ notice() }}</div> }

    <div class="layout">
      <nav class="card list">
        @for (t of templates(); track t.code) {
          <button class="item" [class.active]="!isNew() && draft().code === t.code" (click)="select(t)">
            <span class="code">{{ t.code }}</span>
            <span class="muted">{{ t.channel }} · v{{ t.version }}</span>
          </button>
        } @empty {
          <p class="muted">No templates yet.</p>
        }
      </nav>

      <section class="card editor">
        <div class="row">
          <label class="grow">Code
            <input [ngModel]="draft().code" (ngModelChange)="patch({ code: $event })" [disabled]="!isNew()"
                   placeholder="e.g. password-reset-email" />
          </label>
          <label>Channel
            <select [ngModel]="draft().channel" (ngModelChange)="patch({ channel: $event })" [disabled]="!isNew()">
              @for (c of channels; track c) { <option [value]="c">{{ c }}</option> }
            </select>
          </label>
        </div>
        @if (draft().channel !== 'SMS') {
          <label>{{ draft().channel === 'PUSH' ? 'Title' : 'Subject' }}
            <input [ngModel]="draft().subject" (ngModelChange)="patch({ subject: $event })" />
          </label>
        }
        <label>Body
          <textarea rows="9" class="mono" [ngModel]="draft().body" (ngModelChange)="patch({ body: $event })"></textarea>
        </label>
        <label>Sample data (JSON)
          <textarea rows="5" class="mono" [ngModel]="draft().sampleData" (ngModelChange)="patch({ sampleData: $event })"></textarea>
        </label>
        <div class="row">
          <button class="primary" (click)="save()" [disabled]="saving()">
            {{ isNew() ? 'Create template' : 'Save as new version' }}
          </button>
          @if (draft().channel === 'SMS') {
            <span class="muted">{{ previewBody().length }} characters · {{ smsSegments() }} SMS segment(s)</span>
          }
        </div>

        @if (!isNew() && versions().length > 0) {
          <h3>Versions</h3>
          <table>
            <tbody>
              @for (v of versions(); track v.id) {
                <tr>
                  <td>v{{ v.version }}</td>
                  <td class="muted">{{ v.createdAt | date: 'medium' }}</td>
                  <td>
                    @if (v.active) { <strong>active</strong> }
                    @else { <button (click)="activate(v)">Activate</button> }
                  </td>
                </tr>
              }
            </tbody>
          </table>
        }
      </section>

      <section class="card preview">
        <h2>Live preview</h2>
        @if (previewError()) {
          <div class="error">{{ previewError() }}</div>
        } @else {
          @if (previewSubject()) { <p class="subject">{{ previewSubject() }}</p> }
          @switch (draft().channel) {
            @case ('EMAIL') {
              <iframe sandbox="" [attr.srcdoc]="previewBody()" title="Email preview"></iframe>
            }
            @case ('SMS') {
              <div class="phone"><div class="bubble">{{ previewBody() }}</div></div>
            }
            @default {
              <div class="push">
                <strong>{{ previewSubject() || 'Acme' }}</strong>
                <span>{{ previewBody() }}</span>
              </div>
            }
          }
        }
      </section>
    </div>
  `,
  styles: `
    .layout { display: grid; grid-template-columns: 220px minmax(0, 1fr) minmax(0, 1fr); gap: 16px; align-items: start; }
    @media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }
    .list { padding: 6px; display: flex; flex-direction: column; gap: 2px; }
    .item { display: flex; flex-direction: column; align-items: flex-start; border: 0; background: none; text-align: left; padding: 8px 10px; }
    .item.active { background: var(--surface-2); }
    .item .code { font-weight: 600; }
    .item .muted { font-size: 12px; }
    .editor { display: flex; flex-direction: column; gap: 12px; }
    .row { display: flex; gap: 12px; align-items: flex-end; flex-wrap: wrap; }
    .grow { flex: 1; }
    textarea { resize: vertical; }
    h3 { font-size: 13px; margin: 8px 0 0; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.04em; }
    .preview { position: sticky; top: 20px; }
    .subject { font-weight: 600; margin: 0 0 8px; }
    iframe { width: 100%; height: 360px; border: 1px solid var(--border); border-radius: 6px; background: #fff; }
    .phone { background: var(--surface-2); border-radius: 18px; padding: 20px 14px; min-height: 160px; }
    .bubble {
      background: #e9e9eb; color: #111; padding: 10px 14px; border-radius: 16px 16px 16px 4px;
      max-width: 85%; white-space: pre-wrap; word-break: break-word;
    }
    .push {
      display: flex; flex-direction: column; gap: 2px; background: var(--surface-2); border-radius: 12px;
      padding: 12px 14px; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    }
  `,
})
export class Templates {
  private api = inject(Api);
  protected readonly channels = CHANNELS;

  protected readonly templates = signal<Template[]>([]);
  protected readonly versions = signal<Template[]>([]);
  protected readonly draft = signal<Draft>({ code: '', channel: 'EMAIL', subject: '', body: '', sampleData: '{}' });
  protected readonly isNew = signal(true);
  protected readonly saving = signal(false);
  protected readonly error = signal('');
  protected readonly notice = signal('');

  protected readonly previewSubject = signal('');
  protected readonly previewBody = signal('');
  protected readonly previewError = signal('');
  private readonly previewRequests = new Subject<Draft>();

  constructor() {
    this.previewRequests
      .pipe(
        debounceTime(250),
        switchMap((d) => {
          let data: unknown;
          try {
            data = JSON.parse(d.sampleData || '{}');
          } catch {
            return of({ error: 'Sample data is not valid JSON' });
          }
          return this.api
            .preview({ channel: d.channel, subject: d.channel === 'SMS' ? '' : d.subject, body: d.body, data })
            .pipe(catchError((e) => of({ error: errorMessage(e) })));
        }),
        takeUntilDestroyed(inject(DestroyRef)),
      )
      .subscribe((r) => {
        if ('error' in r) {
          this.previewError.set(r.error);
          return;
        }
        this.previewError.set('');
        this.previewSubject.set(r.subject ?? '');
        this.previewBody.set(r.body);
      });

    this.loadList(true);
  }

  protected smsSegments(): number {
    const len = this.previewBody().length;
    // GSM-7: 160 chars in one segment, 153 per segment once concatenated.
    return len <= 160 ? 1 : Math.ceil(len / 153);
  }

  protected patch(p: Partial<Draft>): void {
    this.draft.update((d) => ({ ...d, ...p }));
    this.notice.set('');
    if (this.draft().body) this.previewRequests.next(this.draft());
  }

  protected startNew(): void {
    this.isNew.set(true);
    this.versions.set([]);
    this.draft.set({ code: '', channel: 'EMAIL', subject: 'Hello {{name}}', body: '<p>Hi {{name}},</p>', sampleData: '{\n  "name": "Ada"\n}' });
    this.previewRequests.next(this.draft());
  }

  protected select(t: Template): void {
    this.isNew.set(false);
    this.error.set('');
    this.notice.set('');
    const skeleton = Object.fromEntries(variablesOf(t.subject ?? '', t.body).map((v) => [v, `<${v}>`]));
    this.draft.set({
      code: t.code,
      channel: t.channel,
      subject: t.subject ?? '',
      body: t.body,
      sampleData: SAMPLE_DATA[t.code] ?? JSON.stringify(skeleton, null, 2),
    });
    this.previewRequests.next(this.draft());
    this.api.templateVersions(t.code).subscribe({ next: (v) => this.versions.set(v) });
  }

  protected save(): void {
    const d = this.draft();
    const subject = d.channel === 'SMS' ? undefined : d.subject;
    this.saving.set(true);
    const req = this.isNew()
      ? this.api.createTemplate({ code: d.code, channel: d.channel, subject, body: d.body })
      : this.api.updateTemplate(d.code, { subject, body: d.body });
    req.subscribe({
      next: (t) => {
        this.saving.set(false);
        this.error.set('');
        this.notice.set(`Saved ${t.code} v${t.version}.`);
        this.isNew.set(false);
        this.loadList(false);
        this.api.templateVersions(t.code).subscribe({ next: (v) => this.versions.set(v) });
      },
      error: (e) => {
        this.saving.set(false);
        this.error.set(errorMessage(e));
      },
    });
  }

  protected activate(v: Template): void {
    this.api.activateTemplate(v.code, v.version).subscribe({
      next: () => {
        this.notice.set(`${v.code} v${v.version} is now active.`);
        this.loadList(false);
        this.api.templateVersions(v.code).subscribe({
          next: (vs) => {
            this.versions.set(vs);
            const active = vs.find((x) => x.active);
            if (active) this.select(active);
          },
        });
      },
      error: (e) => this.error.set(errorMessage(e)),
    });
  }

  private loadList(selectFirst: boolean): void {
    this.api.templates().subscribe({
      next: (ts) => {
        this.templates.set(ts);
        if (selectFirst && ts.length > 0) this.select(ts[0]);
      },
      error: (e) => this.error.set(errorMessage(e)),
    });
  }
}
