// k6 load test: sustained notification traffic across all three channels.
//
//   k6 run load-tests/send.js
//   k6 run -e RATE=500 -e DURATION=5m load-tests/send.js
//
// k6 measures API acceptance (p95 of POST /notifications). End-to-end delivery
// throughput and latency come from the worker's Prometheus metrics; the summary at
// the end pulls the admin stats so both numbers land in one report.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const API = __ENV.API_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 200); // requests per second
const DURATION = __ENV.DURATION || '2m';

const accepted = new Counter('notifications_accepted');

export const options = {
  scenarios: {
    send: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{name:send}': ['p(95)<250'],
  },
};

const requests = [
  () => ({
    templateCode: 'otp-sms',
    recipient: `+1555${String(Math.floor(Math.random() * 1e7)).padStart(7, '0')}`,
    data: { code: String(Math.floor(100000 + Math.random() * 900000)), minutes: 5 },
  }),
  () => ({
    templateCode: 'order-shipped-push',
    recipient: `device-${Math.floor(Math.random() * 10000)}`,
    data: { orderId: `A-${Math.floor(Math.random() * 1e6)}`, eta: 'Friday' },
  }),
  () => ({
    templateCode: 'welcome-email',
    recipient: `user${Math.floor(Math.random() * 1e6)}@example.test`,
    data: { name: 'Load Test', email: 'load@example.test' },
  }),
];

export default function () {
  const body = requests[Math.floor(Math.random() * requests.length)]();
  const res = http.post(`${API}/api/v1/notifications`, JSON.stringify(body), {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': uuidv4() },
    tags: { name: 'send' },
  });
  if (check(res, { 'accepted (202)': (r) => r.status === 202 })) {
    accepted.add(1);
  }
}

export function handleSummary(data) {
  const stats = http.get(`${API}/api/v1/admin/stats?window=15m`);
  const p95 = data.metrics['http_req_duration{name:send}']?.values['p(95)'];
  const count = data.metrics.notifications_accepted?.values.count ?? 0;
  const seconds = data.state.testRunDurationMs / 1000;
  const lines = [
    '',
    `accepted:            ${count} notifications in ${seconds.toFixed(0)}s`,
    `accepted per minute: ${Math.round((count / seconds) * 60)}`,
    `API p95 (send):      ${p95?.toFixed(1)} ms`,
    '',
    'per-channel delivery stats (last 15m):',
    stats.status === 200 ? JSON.stringify(stats.json(), null, 2) : `stats unavailable (${stats.status})`,
    '',
  ];
  return { stdout: lines.join('\n') };
}
