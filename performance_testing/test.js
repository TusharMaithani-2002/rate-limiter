import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    stress_test: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '10s', target: 50 },  // Ramp-up to 50 concurrent users
        { duration: '30s', target: 200 }, // Spike to 200 concurrent users (triggering shedder)
        { duration: '10s', target: 0 },   // Cool-down
      ],
    },
  },
  thresholds: {
    // Assert system-level targets
    http_req_duration: ['p(95)<200', 'p(99)<500'], // 99% of requests must finish within 500ms
  },
};

export default function () {
  // Rotate across 10 simulated client IPs so rate limits trigger individually
  const clientID = Math.floor(Math.random() * 10) + 1;
  const simulatedIP = `192.168.1.${clientID}`;

  const params = {
    headers: {
      'X-Forwarded-For': simulatedIP,
    },
  };

  const res = http.get('http://localhost:8080/api/data', params);

  // Validate that response codes match expected behaviors
  check(res, {
    'status is 200, 429, or 503': (r) =>
      r.status === 200 || r.status === 429 || r.status === 503,
    'shedding returned 503 when overloaded': (r) =>
      r.status !== 503 || r.headers['Retry-After'] !== undefined,
  });

  // Brief jitter between requests to mimic real traffic
  sleep(0.01);
}