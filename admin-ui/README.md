# Admin console

Angular 22 (standalone components, signals, zoneless) operator console for the notification service.

| Page | What it does |
|---|---|
| Dashboard | Per-channel accepted / delivered / failed / DLQ counts and p95 provider latency, auto-refreshing |
| Delivery logs | Filter by channel, status, recipient and time range; click a row for the attempt timeline |
| Templates | Create and version Mustache templates with a live rendered preview (email in a sandboxed iframe, SMS with segment count) |
| Dead letters | Inspect dead-lettered messages, replay one or many |
| Rate limits | Edit per-channel token-bucket limits; workers pick them up without a restart |

```bash
npm install
npm start          # http://localhost:4200, proxies /api to http://localhost:8080
npm run build      # production bundle in dist/admin-ui/browser
```

In Docker Compose the console is served by nginx, which proxies `/api` to the API container (see `nginx.conf`).
