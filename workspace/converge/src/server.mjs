import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { constantTimeEqual } from './crypto-util.mjs';
import { loadConfig } from './config.mjs';
import { ConvergenceStore, ApiError } from './store.mjs';
import { NotificationError } from './notifier.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const MAX_BODY = 1024 * 1024;

/** role -> allowed (method, route). Prefix entries end with '/'. */
const POLICY = {
  operator: [
    'POST /api/envelopes',
    'POST /api/envelopes/digest',
    'GET /api/objects',
    'GET /api/objects/',
    'GET /api/cases',
    'GET /api/cases/',
    'GET /api/receipts',
    'GET /api/receipts/',
    'POST /api/receipts/verify',
    'POST /api/archive/checkpoints',
    'POST /api/archive/restore',
    'GET /api/archive/status',
    'GET /api/archive/proofs',
    'GET /api/archive/segments/',
  ],
  reviewer: [
    'GET /api/objects',
    'GET /api/objects/',
    'GET /api/cases',
    'GET /api/cases/',
    'POST /api/reviews/',
    'GET /api/receipts',
    'GET /api/receipts/',
    'POST /api/receipts/verify',
    'GET /api/archive/status',
  ],
  // Inspectors never see station ids, request keys or sequences: only the
  // desensitised receipt stream, integrity verification and desensitised
  // archive proofs. They can never start a compression or a restore.
  inspector: [
    'GET /api/receipts',
    'GET /api/receipts/',
    'POST /api/receipts/verify',
    'GET /api/archive/proofs',
    'GET /api/archive/segments/',
    'GET /api/subscriptions/events',
    'GET /api/subscriptions/events/',
    'POST /api/subscriptions/events/verify',
  ],
  // Admin: full lifecycle of downstream event subscriptions, key rotation and
  // dead-letter replay. Everything is enforced again inside handleApi.
  admin: [
    'GET /api/subscriptions',
    'POST /api/subscriptions',
    'GET /api/subscriptions/',
    'PATCH /api/subscriptions/',
    'POST /api/subscriptions/',
    'GET /api/subscriptions/dead-letters',
    'GET /api/subscriptions/events',
    'GET /api/subscriptions/events/',
    'POST /api/subscriptions/events/verify',
  ],
};

function routeAllowed(role, method, pathname) {
  const key = `${method} ${pathname}`;
  if (role === 'inspector' && method === 'GET' && /^\/api\/objects\/[^/]+\/receipts\/download$/.test(pathname)) return true;
  // Operator gets read-only visibility of subscription status / pending counts
  // / dead-letter reasons (no secrets, no management actions).
  if (role === 'operator' && method === 'GET' &&
      (pathname === '/api/subscriptions' ||
       /^\/api\/subscriptions\/(?!dead-letters|events)[^/]+$/.test(pathname) ||
       pathname === '/api/subscriptions/dead-letters' ||
       pathname === '/api/subscriptions/events')) {
    return true;
  }
  // The raw (identity-bearing) segment dump is operator-only; /pack and /proof
  // are the desensitised inspector-safe routes on the same prefix.
  if (role !== 'operator' && /^\/api\/archive\/segments\/[^/]+\/raw$/.test(pathname)) return false;
  return POLICY[role].some((p) => {
    const [pm, pp] = p.split(' ');
    if (pm !== method) return false;
    if (pp.endsWith('/')) return pathname.startsWith(pp);
    return pathname === pp;
  });
}

export function createServer(config = loadConfig()) {
  const store = new ConvergenceStore(config);
  store.open();

  const server = http.createServer(async (req, res) => {
    try {
      await handle(req, res, config, store);
    } catch (err) {
      sendError(res, err);
    }
  });

  server.on('close', () => store.close());
  return { server, store };
}

async function handle(req, res, config, store) {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;

  if (pathname === '/healthz') return sendJson(res, 200, { ok: true });
  if (pathname === '/' || pathname === '/console' || pathname === '/index.html') {
    return serveFile(res, path.join(PUBLIC_DIR, 'index.html'), 'text/html; charset=utf-8');
  }
  if (pathname === '/app.js') return serveFile(res, path.join(PUBLIC_DIR, 'app.js'), 'application/javascript; charset=utf-8');
  if (pathname.startsWith('/api/')) return handleApi(req, res, url, config, store);
  sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'not found' } });
}

function authenticate(req, config) {
  const header = req.headers.authorization || '';
  const m = /^Bearer (.+)$/.exec(header);
  if (!m) throw new ApiError(401, 'UNAUTHENTICATED', 'bearer token required');
  const token = m[1];
  let matchedRole = null;
  for (const [known, role] of Object.entries(config.tokens)) {
    if (constantTimeEqual(token, known)) {
      matchedRole = role;
      break;
    }
  }
  if (!matchedRole) throw new ApiError(403, 'FORBIDDEN', 'token not recognized');
  const reviewerName = config.reviewerIdentities[token] || null;
  return { role: matchedRole, reviewerId: reviewerName ? `anon-${matchedRole}-${reviewerName.slice(-2)}` : null, reviewerName };
}

async function handleApi(req, res, url, config, store) {
  const auth = authenticate(req, config);
  if (!routeAllowed(auth.role, req.method, url.pathname)) {
    throw new ApiError(403, 'ROLE_NOT_PERMITTED', `${auth.role} may not ${req.method} ${url.pathname}`);
  }

  const body = req.method === 'POST' ? await readJson(req) : null;
  const p = url.pathname;

  if (p === '/api/envelopes' && req.method === 'POST') {
    const result = await store.submitEnvelope(body);
    return sendJson(res, 200, result);
  }
  if (p === '/api/envelopes/digest' && req.method === 'POST') {
    return sendJson(res, 200, await store.previewEnvelopeDigest(body));
  }
  if (p === '/api/objects' && req.method === 'GET') {
    return sendJson(res, 200, { objects: await store.listObjects() });
  }
  if (p === '/api/cases' && req.method === 'GET') {
    return sendJson(res, 200, { cases: await store.listCases() });
  }
  if (p === '/api/receipts' && req.method === 'GET') {
    const objectKey = url.searchParams.get('object_key') || null;
    return sendJson(res, 200, { receipts: await store.listReceipts(objectKey) });
  }
  if (p === '/api/receipts/verify' && req.method === 'POST') {
    return sendJson(res, 200, await store.verifyReceipt(body?.receipt ?? body));
  }

  // ---------------------------------------------- downstream subscriptions ----
  if (p === '/api/subscriptions' && req.method === 'POST') {
    if (auth.role !== 'admin') throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'only admins register subscriptions');
    return sendJson(res, 200, await store.createSubscription(body));
  }
  if (p === '/api/subscriptions' && req.method === 'GET') {
    // Admin sees full operational views (never the secret); operator sees
    // status / pending counts / dead-letter reasons read-only.
    return sendJson(res, 200, { subscriptions: await store.listSubscriptions() });
  }
  if (p === '/api/subscriptions/dead-letters' && req.method === 'GET') {
    if (auth.role !== 'admin' && auth.role !== 'operator') {
      throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'dead letters are admin/operator visibility only');
    }
    return sendJson(res, 200, { dead_letters: await store.listDeadLetters() });
  }
  if (p === '/api/subscriptions/events' && req.method === 'GET') {
    // Desensitised notification proofs: inspectors and admins may read;
    // never contains callback URLs, secrets or response error bodies.
    const objectKey = url.searchParams.get('object_key') || null;
    const kind = url.searchParams.get('kind') || null;
    return sendJson(res, 200, {
      proofs: await store.listNotificationProofs({
        objectKey, kind, includeDeliveryReason: auth.role === 'admin',
      }),
    });
  }
  if (p === '/api/subscriptions/events/verify' && req.method === 'POST') {
    return sendJson(res, 200, await store.verifyNotificationProof(body));
  }

  // ------------------------------------------------- history compression ----
  if (p === '/api/archive/checkpoints' && req.method === 'POST') {
    return sendJson(res, 200, await store.createCheckpoint(body));
  }
  if (p === '/api/archive/restore' && req.method === 'POST') {
    return sendJson(res, 200, await store.restoreObject(body));
  }
  if (p === '/api/archive/status' && req.method === 'GET') {
    return sendJson(res, 200, await store.archiveStatus());
  }
  if (p === '/api/archive/proofs' && req.method === 'GET') {
    return sendJson(res, 200, { proofs: await store.listProofs() });
  }

  let m;
  if ((m = /^\/api\/subscriptions\/events\/([^/]+)$/.exec(p)) && req.method === 'GET') {
    return sendJson(res, 200, {
      proofs: await store.getNotificationProof(decodeURIComponent(m[1]),
        { includeDeliveryReason: auth.role === 'admin' }),
    });
  }
  if ((m = /^\/api\/subscriptions\/deliveries\/([^/]+)\/resend$/.exec(p)) && req.method === 'POST') {
    if (auth.role !== 'admin') throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'only admins resend dead letters');
    return sendJson(res, 200, await store.resendDeadLetter(decodeURIComponent(m[1])));
  }
  if ((m = /^\/api\/subscriptions\/([^/]+)\/pause$/.exec(p)) && req.method === 'POST') {
    if (auth.role !== 'admin') throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'only admins pause subscriptions');
    return sendJson(res, 200, await store.updateSubscription(decodeURIComponent(m[1]), { status: 'paused' }));
  }
  if ((m = /^\/api\/subscriptions\/([^/]+)\/resume$/.exec(p)) && req.method === 'POST') {
    if (auth.role !== 'admin') throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'only admins resume subscriptions');
    return sendJson(res, 200, await store.updateSubscription(decodeURIComponent(m[1]), { status: 'active' }));
  }
  if ((m = /^\/api\/subscriptions\/([^/]+)\/rotate-key$/.exec(p)) && req.method === 'POST') {
    if (auth.role !== 'admin') throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'only admins rotate signing keys');
    return sendJson(res, 200, await store.rotateSubscriptionKey(decodeURIComponent(m[1]), body || {}));
  }
  if ((m = /^\/api\/subscriptions\/([^/]+)$/.exec(p)) && req.method === 'GET') {
    return sendJson(res, 200, await store.getSubscription(decodeURIComponent(m[1])));
  }
  if ((m = /^\/api\/subscriptions\/([^/]+)$/.exec(p)) && req.method === 'PATCH') {
    if (auth.role !== 'admin') throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'only admins update subscriptions');
    return sendJson(res, 200, await store.updateSubscription(decodeURIComponent(m[1]), body || {}));
  }
  if ((m = /^\/api\/objects\/([^/]+)$/.exec(p)) && req.method === 'GET') {
    return sendJson(res, 200, await store.getObject(decodeURIComponent(m[1])));
  }
  if ((m = /^\/api\/objects\/([^/]+)\/receipts\/download$/.exec(p)) && req.method === 'GET') {
    const objectKey = decodeURIComponent(m[1]);
    const receipts = await store.listReceipts(objectKey);
    if (!receipts.length) throw new ApiError(404, 'RECEIPT_NOT_FOUND', 'no receipts for object');
    // Inspector-safe: listReceipts already strips request_key/site/site_seq.
    const payload = JSON.stringify({ object_key: objectKey, receipts });
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'content-disposition': `attachment; filename="receipts-${objectKey.replace(/[^A-Za-z0-9_.-]/g, '_')}.json"`,
    });
    return res.end(payload);
  }
  if ((m = /^\/api\/receipts\/([^/]+)$/.exec(p)) && req.method === 'GET') {
    return sendJson(res, 200, await store.getReceipt(decodeURIComponent(m[1])));
  }
  if ((m = /^\/api\/archive\/segments\/([^/]+)\/pack$/.exec(p)) && req.method === 'GET') {
    // Inspector-safe desensitised proof pack; tamper is detected before delivery.
    const pack = await store.downloadProofPack(decodeURIComponent(m[1]));
    const payload = JSON.stringify(pack, null, 2);
    res.writeHead(200, {
      'content-type': 'application/json; charset=utf-8',
      'content-disposition': `attachment; filename="archive-proof-${m[1].slice(0, 16)}.json"`,
    });
    return res.end(payload);
  }
  if ((m = /^\/api\/archive\/segments\/([^/]+)\/raw$/.exec(p)) && req.method === 'GET') {
    // Operator-only raw segment (contains station ids and request keys).
    const seg = await store.readSegmentRaw(decodeURIComponent(m[1]));
    res.writeHead(200, {
      'content-type': 'application/octet-stream',
      'content-disposition': `attachment; filename="segment-${m[1].slice(0, 16)}.seg"`,
      'x-segment-frames': String(seg.frame_count),
    });
    return res.end(seg.buffer);
  }
  if ((m = /^\/api\/cases\/([^/]+)$/.exec(p)) && req.method === 'GET') {
    const view = await store.getCase(m[1]);
    return sendJson(res, 200, view);
  }
  if ((m = /^\/api\/reviews\/([^/]+)$/.exec(p)) && req.method === 'POST') {
    if (auth.role !== 'reviewer') throw new ApiError(403, 'ROLE_NOT_PERMITTED', 'only reviewers adjudicate');
    const result = await store.submitReview(decodeURIComponent(m[1]), auth.reviewerId, auth.reviewerName, body?.choices);
    return sendJson(res, 200, result);
  }

  throw new ApiError(404, 'NOT_FOUND', 'no such route');
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY) {
        reject(new ApiError(413, 'BODY_TOO_LARGE', 'request body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(new ApiError(400, 'BAD_JSON', 'body must be valid JSON'));
      }
    });
    req.on('error', reject);
  });
}

function serveFile(res, filePath, contentType) {
  fs.readFile(filePath, (err, data) => {
    if (err) return sendJson(res, 404, { error: { code: 'NOT_FOUND', message: 'not found' } });
    res.writeHead(200, { 'content-type': contentType });
    res.end(data);
  });
}

function sendJson(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(body);
}

function sendError(res, err) {
  if (err instanceof ApiError || err instanceof NotificationError) {
    return sendJson(res, err.status, { error: { code: err.code, message: err.message, details: err.details } });
  }
  // Never leak internals; log server-side.
  console.error('unhandled error:', err);
  sendJson(res, 500, { error: { code: 'INTERNAL', message: 'internal error' } });
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const config = loadConfig();
  const { server } = createServer(config);
  server.listen(config.port, config.host, () => {
    console.log(`converge service listening on http://${config.host}:${config.port} (data: ${config.dataDir})`);
  });
  const shutdown = () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1000).unref();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
