// k6 load scenarios for POST /jobs. Run through scripts/loadtest.py, which
// sets the env vars, resets the stack and measures the worker side.
//
//   SCENARIO   burst  - JOBS enqueues as fast as VUS clients can send them
//              steady - RATE enqueues/sec for DURATION
//   JOB_TYPE   handler to submit (default image.resize)
//   FAIL_PCT   percent of jobs that fail: half flaky (succeed on retry),
//              half poison (end in the dead-letter queue)
import http from 'k6/http';
import { check } from 'k6';

const API = __ENV.API_URL || 'http://api:8000';
const SCENARIO = __ENV.SCENARIO || 'burst';
const JOB_TYPE = __ENV.JOB_TYPE || 'image.resize';
const FAIL_PCT = Number(__ENV.FAIL_PCT || 0);

const scenarios = {
  burst: {
    executor: 'shared-iterations',
    vus: Number(__ENV.VUS || 50),
    iterations: Number(__ENV.JOBS || 5000),
    maxDuration: '10m',
  },
  steady: {
    executor: 'constant-arrival-rate',
    rate: Number(__ENV.RATE || 100),
    timeUnit: '1s',
    duration: __ENV.DURATION || '3m',
    preAllocatedVUs: 50,
    maxVUs: 400,
  },
};

export const options = {
  scenarios: { [SCENARIO]: scenarios[SCENARIO] },
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds: { http_req_failed: ['rate<0.01'] },
};

const PAYLOADS = {
  'image.resize': () => ({ width: 1920, height: 1080, target_width: 640 }),
  'email.send': () => ({ to: `user${__VU}@example.com`, subject: 'load' }),
  sleep: () => ({ seconds: Number(__ENV.SLEEP_SECONDS || 0.05) }),
};

function nextJob() {
  const roll = Math.random() * 100;
  if (roll < FAIL_PCT / 2) return { type: 'flaky', payload: { fail_times: 1 } };
  if (roll < FAIL_PCT) return { type: 'poison', payload: {}, max_attempts: 3 };
  return { type: JOB_TYPE, payload: PAYLOADS[JOB_TYPE]() };
}

export default function () {
  const res = http.post(`${API}/jobs`, JSON.stringify(nextJob()), {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'accepted (201)': (r) => r.status === 201 });
}

// Machine-readable summary on stdout; the runner parses it.
export function handleSummary(data) {
  return { stdout: JSON.stringify(data) };
}
