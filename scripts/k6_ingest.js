// k6 load test: N concurrent PDF ingestion jobs through the edge, followed to completion.
//
//   make k6-ingest                                   # 50 jobs x 20 pages = 1,000 pages
//   k6 run -e PDF=$PWD/fixtures/doc20p_100kb.pdf scripts/k6_ingest.js
//
// Each virtual user uploads one PDF (POST /jobs), then follows the job's WebSocket
// (/jobs/{id}/ws) until job_complete, resuming with last_event_id if the socket drops.
// All users start at once, so N jobs are in flight together.
//
// Uploads rejected by the edge's DoS limits (429 rate, 503 concurrent uploads) are retried
// after Retry-After, like a real client, and counted. Pipeline latencies are measured from
// the moment the upload was accepted (202).
//
// Environment:
//   BASE_URL       edge URL (default http://localhost:8080)
//   PDF            PDF to upload, absolute path (make k6-ingest generates a 20-page one)
//   JOBS           concurrent jobs = virtual users (default 50)
//   CLIENT_ID      one X-Client-ID for every job (default: a distinct id per job)
//   JOB_TIMEOUT_S  give up on a job after this long (default 900)
//   SUMMARY        where to write the JSON summary (default results/k6-ingest-summary.json)

import http from 'k6/http';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { WebSocket } from 'k6/websockets';
import { setTimeout, clearTimeout } from 'k6/timers';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const WS_URL = BASE_URL.replace(/^http/, 'ws');
const JOBS = parseInt(__ENV.JOBS || '50', 10);
const JOB_TIMEOUT_S = parseInt(__ENV.JOB_TIMEOUT_S || '900', 10);
const SUMMARY = __ENV.SUMMARY || 'results/k6-ingest-summary.json';
const MAX_UPLOAD_ATTEMPTS = 30;

if (!__ENV.PDF) {
  throw new Error('set PDF to a PDF file (absolute path), e.g. -e PDF=$PWD/fixtures/doc20p_100kb.pdf');
}
const PDF = open(__ENV.PDF, 'b');

export const options = {
  scenarios: {
    ingest: { executor: 'per-vu-iterations', vus: JOBS, iterations: 1, maxDuration: `${JOB_TIMEOUT_S + 120}s` },
  },
  thresholds: {
    job_success: ['rate==1'], // every job finished with all of its pages
  },
  summaryTrendStats: ['min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// Latencies (ms). Page and job latencies start when the upload is accepted.
const uploadMs = new Trend('upload_ms', true); // the accepted POST /jobs
const admissionMs = new Trend('upload_admission_ms', true); // first attempt -> 202, incl. edge retries
const firstPageMs = new Trend('first_page_ms', true);
const pageMs = new Trend('page_ms', true);
const jobMs = new Trend('job_ms', true);
// Wall-clock marks (epoch ms): min accepted -> max completed is the pipeline's busy window.
const acceptedAt = new Trend('accepted_at');
const completedAt = new Trend('completed_at');
const pageDoneAt = new Trend('page_done_at');

const pagesDone = new Counter('pages_completed');
const pagesVlm = new Counter('pages_vlm');
const pagesFallback = new Counter('pages_fallback');
const pagesFailed = new Counter('pages_failed');
const throttled429 = new Counter('upload_throttled_429');
const rejected503 = new Counter('upload_rejected_503');
const uploadErrors = new Counter('upload_errors');
const reconnects = new Counter('ws_reconnects');
const jobSuccess = new Rate('job_success');

const PAGE_EVENTS = { page_result: true, page_fallback: true, page_failed: true };

function upload(clientId) {
  const started = Date.now();
  for (let attempt = 1; attempt <= MAX_UPLOAD_ATTEMPTS; attempt++) {
    const res = http.post(`${BASE_URL}/jobs`, PDF, {
      headers: { 'X-Client-ID': clientId, 'Content-Type': 'application/pdf' },
      tags: { name: 'POST /jobs' },
      responseCallback: http.expectedStatuses(202, 429, 503), // edge throttling is expected here
      timeout: '300s',
    });
    if (res.status === 202) {
      uploadMs.add(res.timings.duration);
      admissionMs.add(Date.now() - started);
      return res.json();
    }
    if (res.status === 429 || res.status === 503) {
      (res.status === 429 ? throttled429 : rejected503).add(1);
      const retryAfter = parseFloat(res.headers['Retry-After'] || '1');
      sleep(retryAfter * (0.5 + Math.random())); // jitter: don't retry in lockstep
      continue;
    }
    uploadErrors.add(1);
    console.error(`upload failed: HTTP ${res.status} ${res.body}`);
    return null;
  }
  uploadErrors.add(1);
  console.error(`upload still throttled after ${MAX_UPLOAD_ATTEMPTS} attempts`);
  return null;
}

// Follow one job's events until job_complete / job_failed, reconnecting on drops.
function follow(job, clientId, accepted) {
  const deadline = accepted + JOB_TIMEOUT_S * 1000;
  const state = { lastSeq: 0, pages: 0, finished: false };

  const finish = (ok, reason) => {
    state.finished = true;
    if (!ok) console.error(`job ${job.job_id}: ${reason}`);
    jobSuccess.add(ok);
  };

  const connect = () => {
    const url = `${WS_URL}/jobs/${job.job_id}/ws?client_id=${clientId}&last_event_id=${state.lastSeq}`;
    const ws = new WebSocket(url);
    const timer = setTimeout(() => ws.close(), Math.max(0, deadline - Date.now()));

    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.event === 'ping' || msg.id <= state.lastSeq) return; // keep-alive or duplicate
      state.lastSeq = msg.id;
      const now = Date.now();
      if (PAGE_EVENTS[msg.event]) {
        if (state.pages === 0) firstPageMs.add(now - accepted);
        state.pages += 1;
        pageMs.add(now - accepted);
        pageDoneAt.add(now);
        pagesDone.add(1);
        if (msg.event === 'page_failed') pagesFailed.add(1);
        else if (msg.data.payload.source === 'vlm') pagesVlm.add(1);
        else pagesFallback.add(1);
      } else if (msg.event === 'job_complete' || msg.event === 'job_failed') {
        jobMs.add(now - accepted);
        completedAt.add(now);
        const complete = msg.event === 'job_complete' && state.pages === job.total_pages;
        finish(complete, complete ? '' : `${msg.event} with ${state.pages}/${job.total_pages} pages`);
        ws.close();
      }
    };
    ws.onclose = () => {
      clearTimeout(timer);
      if (state.finished) return;
      if (Date.now() >= deadline) {
        finish(false, `timed out after ${JOB_TIMEOUT_S}s with ${state.pages}/${job.total_pages} pages`);
        return;
      }
      reconnects.add(1);
      setTimeout(connect, 1000); // resume after the last event seen
    };
    ws.onerror = (e) => console.warn(`job ${job.job_id}: websocket error ${e.error}`);
  };
  connect();
}

// Runs once; every virtual user gets its result, so all jobs of a run share one run id.
export function setup() {
  return { runId: Math.random().toString(36).slice(2, 8) };
}

export default function (data) {
  const clientId = __ENV.CLIENT_ID || `k6-${data.runId}-${__VU}`;
  const job = upload(clientId);
  if (!job) {
    jobSuccess.add(false);
    return;
  }
  const accepted = Date.now();
  acceptedAt.add(accepted);
  follow(job, clientId, accepted);
}

// --- report ---------------------------------------------------------------------------

function value(data, metric, stat) {
  const m = data.metrics[metric];
  return m && m.values[stat] !== undefined ? m.values[stat] : 0;
}

function row(data, label, metric) {
  const cells = ['med', 'p(90)', 'p(95)', 'p(99)', 'max'].map((s) => {
    const ms = value(data, metric, s);
    return (ms >= 10000 ? `${(ms / 1000).toFixed(1)} s` : `${Math.round(ms)} ms`).padStart(10);
  });
  return `  ${label.padEnd(34)}${cells.join('')}`;
}

export function handleSummary(data) {
  const pages = value(data, 'pages_completed', 'count');
  const jobsOk = value(data, 'job_success', 'passes');
  const jobsBad = value(data, 'job_success', 'fails');
  const firstAccepted = value(data, 'accepted_at', 'min');
  const windowS = (value(data, 'completed_at', 'max') - firstAccepted) / 1000;
  const bulkS = (value(data, 'page_done_at', 'p(95)') - firstAccepted) / 1000; // 95% of pages done
  const runS = data.state.testRunDurationMs / 1000;
  const report = {
    base_url: BASE_URL,
    jobs: { requested: JOBS, completed: jobsOk, failed: jobsBad },
    pages: {
      completed: pages,
      vlm: value(data, 'pages_vlm', 'count'),
      layout_fallback: value(data, 'pages_fallback', 'count'),
      failed: value(data, 'pages_failed', 'count'),
    },
    throughput: {
      window_s: windowS,
      pages_per_s: windowS > 0 ? pages / windowS : 0,
      jobs_per_s: windowS > 0 ? jobsOk / windowS : 0,
      pages_95pct_s: bulkS,
      pages_per_s_first_95pct: bulkS > 0 ? (0.95 * pages) / bulkS : 0,
      test_duration_s: runS,
    },
    uploads: {
      throttled_429: value(data, 'upload_throttled_429', 'count'),
      rejected_503: value(data, 'upload_rejected_503', 'count'),
      errors: value(data, 'upload_errors', 'count'),
    },
    ws_reconnects: value(data, 'ws_reconnects', 'count'),
    latency_ms: Object.fromEntries(
      ['upload_ms', 'upload_admission_ms', 'first_page_ms', 'page_ms', 'job_ms'].map((m) => [m, data.metrics[m] ? data.metrics[m].values : null]),
    ),
  };

  const t = report.throughput;
  const lines = [
    '',
    `VisionForge ingestion load test: ${JOBS} concurrent jobs against ${BASE_URL}`,
    '',
    `  jobs         ${jobsOk}/${JOBS} completed with every page${jobsBad ? `, ${jobsBad} failed` : ''}`,
    `  pages        ${pages} (vlm ${report.pages.vlm}, layout fallback ${report.pages.layout_fallback}, failed ${report.pages.failed})`,
    `  throughput   ${t.pages_per_s.toFixed(2)} pages/s, ${t.jobs_per_s.toFixed(3)} jobs/s ` +
      `(first upload accepted -> last job complete: ${t.window_s.toFixed(1)} s; whole run ${runS.toFixed(1)} s)`,
    `               95% of pages done after ${bulkS.toFixed(1)} s (${t.pages_per_s_first_95pct.toFixed(2)} pages/s); ` +
      `the last 5% took another ${(windowS - bulkS).toFixed(1)} s`,
    `  uploads      edge throttled ${report.uploads.throttled_429}x (429) and ${report.uploads.rejected_503}x (503), ` +
      `all retried; ${report.uploads.errors} errors`,
    `  websockets   ${report.ws_reconnects} reconnects`,
    '',
    `  latency${' '.repeat(29)}${['median', 'p90', 'p95', 'p99', 'max'].map((h) => h.padStart(10)).join('')}`,
    row(data, 'upload (accepted POST /jobs)', 'upload_ms'),
    row(data, 'upload admission (incl. retries)', 'upload_admission_ms'),
    row(data, 'accepted -> first page result', 'first_page_ms'),
    row(data, 'accepted -> each page result', 'page_ms'),
    row(data, 'accepted -> job complete', 'job_ms'),
    '',
    `  full metrics: ${SUMMARY}`,
    '',
  ];
  return {
    stdout: lines.join('\n'),
    [SUMMARY]: JSON.stringify({ report, metrics: data.metrics }, null, 2),
  };
}
