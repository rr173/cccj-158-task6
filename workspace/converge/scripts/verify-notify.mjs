#!/usr/bin/env node
/**
 * End-to-end verification for reliable downstream event subscriptions.
 * Spawns the real HTTP server on an ephemeral port plus local callback
 * receivers, and exercises:
 *   1  happy-path callback: stable id, surrogate key, kind, business cursor,
 *      signature; never site id / request key / image digests
 *   2  cursor order + per-subscription isolation + dedup by stable event id
 *   3  response timeout -> capped backoff -> recovery; acked cursor advances
 *   4  ack loss -> duplicate redelivery with the SAME event id; cursor never
 *      moves backwards; unconfirmed events never skipped (head-of-line)
 *   5  signing-key rotation boundary: in-boxed batches keep the old version,
 *      new batches use the new one; pause/resume honour the same rule
 *   6  pause accumulates a backlog; resume flushes it in business order
 *   7  retry cap -> dead letter with reason; human resend keeps the original
 *      event id and original business order
 *   8  process restart: pending survives, no duplicates, cursor monotonic
 *   9  RBAC: admin manages, operator view-only, inspector desensitised proofs
 *  10  a slow callback never blocks convergence submissions or history reads
 *  11  conflict.closed + cold_segment.generated events; neither carries
 *      site/request/image content
 *  12  transactional outbox: business commit and event inbox are one durable
 *      result (simulated crash window repaired on restart)
 */
import { spawn } from 'node:child_process';
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, '..');

let passed = 0;
let failed = 0;
function check(name, cond, detail = '') {
  if (cond) { passed++; console.log(`  ✓ ${name}`); }
  else { failed++; console.error(`  ✗ ${name} ${detail}`); }
}
function fail(name, detail) { check(name, false, detail || ''); }

const PORT = 49300 + Math.floor(Math.random() * 800);
const DATA = fs.mkdtempSync(path.join(os.tmpdir(), 'converge-notify-'));
const SECRET = 'test-secret';
const TOK = {
  op: 'operator-token', r1: 'reviewer1-token', r2: 'reviewer2-token',
  insp: 'inspector-token', admin: 'admin-token', bogus: 'nope',
};
const digest = (s) => crypto.createHash('sha256').update(s).digest('hex');
const hmac = (secret, canonicalJson) => crypto.createHmac('sha256', secret).update(canonicalJson).digest('hex');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Must match src/crypto-util.mjs canonical() exactly (recursively sorted keys).
function canonical(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  const keys = Object.keys(value).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
}

// ----------------------------------------------------------- callback sinks --
/** A configurable downstream receiver. mode: ok | fail | timeout | drop-once. */
class Sink {
  constructor(name) {
    this.name = name;
    this.received = [];
    this.mode = 'ok';
    this.droppedEventIds = new Set();
    this.latencyMs = 0;
    this.server = http.createServer((req, res) => this._handle(req, res));
  }
  start(port) {
    return new Promise((resolve) => this.server.listen(port, '127.0.0.1', resolve));
  }
  close() { return new Promise((r) => this.server.close(r)); }
  _read(req) {
    return new Promise((resolve) => {
      let b = '';
      req.on('data', (c) => (b += c));
      req.on('end', () => resolve(b));
    });
  }
  async _handle(req, res) {
    const raw = await this._read(req);
    const body = JSON.parse(raw);
    await sleep(this.latencyMs);
    if (this.mode === 'timeout') {
      // Never respond: the sender must hit its AbortController timeout.
      res.socket?.reset?.();
      return;
    }
    if (this.mode === 'fail') {
      res.writeHead(500); res.end('downstream broken'); return;
    }
    if (this.mode === 'drop-once') {
      const key = `${req.headers['x-convergence-subscription']}|${body.event_id}`;
      // The HTTP response (ack) is lost after the receiver acted on it.
      if (!this.droppedEventIds.has(key)) {
        this.droppedEventIds.add(key);
        this.received.push({ headers: req.headers, body, duplicated: false });
        res.socket?.reset?.();
        return;
      }
    }
    const prior = this.received.find((r) =>
      r.body.event_id === body.event_id && r.headers['x-convergence-subscription'] === req.headers['x-convergence-subscription']);
    this.received.push({ headers: req.headers, body, duplicated: !!prior });
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{"ok":true}');
  }
  byEvent() {
    const m = new Map();
    for (const r of this.received) if (!m.has(r.body.event_id)) m.set(r.body.event_id, r);
    return m;
  }
}

const sinkA = new Sink('A');
const sinkB = new Sink('B');
const sinkSlow = new Sink('SLOW');
const sinkTimeout = new Sink('TIMEOUT');
const sinkKey = new Sink('KEY');
const sinkDlq = new Sink('DLQ');
await sinkA.start(9941);
await sinkB.start(9942);
await sinkSlow.start(9943);
await sinkTimeout.start(9944);
await sinkKey.start(9945);
await sinkDlq.start(9946);

// ------------------------------------------------------------------- server --
let serverProc = null;
function startServer(envExtra = {}) {
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, [path.join(ROOT, 'src', 'server.mjs')], {
      env: {
        ...process.env,
        CONVERGE_HOST: '127.0.0.1', CONVERGE_PORT: String(PORT),
        CONVERGE_DATA_DIR: DATA, CONVERGE_RECEIPT_SECRET: SECRET,
        CONVERGE_SETTLE_MS: '15',
        CONVERGE_DELIVERY_TICK_MS: '25',
        CONVERGE_DELIVERY_TIMEOUT_MS: '300',
        CONVERGE_DELIVERY_MAX_ATTEMPTS: '3',
        CONVERGE_DELIVERY_BACKOFF_BASE_MS: '40',
        CONVERGE_DELIVERY_BACKOFF_CAP_MS: '200',
        ...envExtra,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    proc.stdout.on('data', (d) => { out += d; });
    proc.stderr.on('data', (d) => { out += d; });
    serverProc = proc;
    const deadline = Date.now() + 5000;
    const poll = async () => {
      try {
        const r = await fetch(`http://127.0.0.1:${PORT}/healthz`);
        if (r.ok) return resolve();
      } catch { /* not yet */ }
      if (Date.now() > deadline) { proc.kill('SIGKILL'); return reject(new Error('server did not start: ' + out)); }
      setTimeout(poll, 30);
    };
    poll();
  });
}
async function stopServer() {
  if (!serverProc) return;
  const p = serverProc; serverProc = null;
  await new Promise((res) => {
    p.on('exit', res);
    p.kill('SIGTERM');
    setTimeout(() => { p.kill('SIGKILL'); res(); }, 1200);
  });
}

async function req(method, urlPath, token, body) {
  const res = await fetch(`http://127.0.0.1:${PORT}${urlPath}`, {
    method,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { json = null; }
  return { status: res.status, json };
}

/** Submit an envelope and wait until the object has a receipt of the given seq. */
async function settleEnvelope(o, { site = 'station-x', seq = 1, region, change, reqkey, preds = [] }) {
  const r = await req('POST', '/api/envelopes', TOK.op, {
    object_key: o, site, site_seq: seq, coverage_region: region,
    change_digest: digest(change), predecessor_digests: preds, request_key: reqkey,
  });
  return r;
}
async function waitForReceipts(o, n, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { json } = await req('GET', `/api/receipts?object_key=${o}`, TOK.op);
    if ((json.receipts || []).length >= n) return json.receipts;
    await sleep(40);
  }
  return null;
}
async function waitFor(fn, { tries = 80, interval = 50, label = '' } = {}) {
  for (let i = 0; i < tries; i++) {
    const v = await fn();
    if (v) return v;
    await sleep(interval);
  }
  fail(label || 'condition', `timed out after ${tries * interval}ms`);
  return null;
}
function verifySignature(secret, body) {
  const { signature, ...content } = body;
  return hmac(secret, canonical(content)) === signature;
}

await startServer();

// ------------------------------------------------------- 1. happy path --------
console.log('1. 成功回调：稳定标识/对象键/种类/业务游标/签名，无站点/请求键/图像内容');
{
  const create = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'compliance-a',
    callback_url: 'http://127.0.0.1:9941/hook',
    events: ['decision.created', 'conflict.closed', 'cold_segment.generated'],
  });
  check('admin creates subscription 200', create.status === 200, JSON.stringify(create.json));
  const sub = create.json;
  check('subscription has id and auto-generated signing secret', sub.id?.startsWith('sub_') && /^[a-f0-9]{32,}$/.test(sub.signing_secret || ''));
  check('subscription starts active with key v1', sub.status === 'active' && sub.current_key_version === 'v1');
  check('admin list does not leak the secret',
    (await req('GET', '/api/subscriptions', TOK.admin)).json.subscriptions[0].signing_secret === undefined);
  const secretA = sub.signing_secret;

  await settleEnvelope('obj-hook-1', { region: 't:1', change: 'c1', reqkey: 'rk1', site: 'station-alpha', seq: 1 });
  const got = await waitFor(() => sinkA.byEvent().size >= 1 ? sinkA.received[sinkA.received.length - 1] : null,
    { label: 'callback received' });
  const b = got.body;
  check('event has stable id', typeof b.event_id === 'string' && b.event_id.startsWith('ev_'));
  check('event carries object surrogate key', b.object_key === 'obj-hook-1', b.object_key);
  check('event kind', b.kind === 'decision.created', b.kind);
  check('business cursor = receipt seq', b.cursor === 1 && b.cursor_name === 'receipt_seq', `${b.cursor}/${b.cursor_name}`);
  check('key version v1 and signature present', b.key_version === 'v1' && /^[0-9a-f]{64}$/.test(b.signature));
  check('signature verifies with the signing key', verifySignature(secretA, b), 'sig mismatch');
  check('signature also delivered as header', got.headers['x-convergence-signature'] === b.signature);
  check('delivery cursor header present', got.headers['x-convergence-delivery-cursor'] === String(b.delivery_cursor));
  const asText = JSON.stringify(b);
  check('payload has NO site id', !asText.includes('station-alpha'));
  check('payload has NO request key', !asText.includes('rk1'));
  check('payload has NO image change digest', !asText.includes(digest('c1')));
  check('payload has no image content field', !('image' in b) && !('pixel' in b) && !('change_digest' in b));

  // A second decision keeps business cursor ordering.
  await settleEnvelope('obj-hook-1', { region: 't:2', change: 'c2', reqkey: 'rk2', site: 'station-alpha', seq: 2 });
  await waitFor(() => sinkA.received.length >= 2 ? true : null, { label: 'second callback' });
  check('second event cursor = 2', sinkA.received[1].body.cursor === 2 && sinkA.received[1].body.kind === 'decision.created');
  check('two distinct stable event ids', sinkA.received[0].body.event_id !== sinkA.received[1].body.event_id);
}

// ---------------------------------------------- 2. per-subscription cursors ---
console.log('2. 每订阅独立游标；订阅过滤生效；稳定事件标识去重');
{
  const subB = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'compliance-b', callback_url: 'http://127.0.0.1:9942/hook',
    events: ['decision.created'],
  });
  check('second subscription created', subB.status === 200);
  await settleEnvelope('obj-hook-2', { region: 't:a', change: 'x1', reqkey: 'r1', site: 'st-a', seq: 1 });
  await waitFor(() => sinkB.byEvent().size >= 1 ? true : null, { label: 'sink B callback' });
  await sleep(100);
  const aCursors = sinkA.received.map((r) => r.body.delivery_cursor);
  const bCursors = sinkB.received.map((r) => r.body.delivery_cursor);
  check('sink B got only its own events', sinkB.received.every((r) => r.body.object_key === 'obj-hook-2'));
  check('per-subscription delivery cursor starts at 1', bCursors[0] === 1, JSON.stringify(bCursors));
  check('sink A cursor independently counts all its events (>=3)', aCursors.length >= 3 && aCursors[aCursors.length - 1] === aCursors.length,
    JSON.stringify(aCursors));
}

// --------------------------------------------- 3. timeout -> backoff -> recv --
console.log('3. 响应超时：退避重试，恢复后确认游标前进');
{
  const subT = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'backoff-sub', callback_url: 'http://127.0.0.1:9944/b', events: ['decision.created'],
  });
  check('backoff sub created', subT.status === 200);
  sinkTimeout.mode = 'timeout';
  await settleEnvelope('obj-timeout', { region: 'r:1', change: 'z1', reqkey: 'rt1', site: 'st-t', seq: 1 });
  const timedRow = await waitFor(async () => {
    const cur = (await req('GET', `/api/subscriptions/${subT.json.id}`, TOK.admin)).json;
    const r = (cur.pending || []).find((p) => p.attempts >= 2);
    return r || null;
  }, { tries: 40, interval: 40, label: 'second failed attempt accrued' });
  check('attempts accrue with capped backoff while timing out', !!timedRow, JSON.stringify(timedRow));
  check('retry row records timeout reason', /timeout/.test(timedRow?.last_error || ''), timedRow?.last_error);

  // Receiver recovers BEFORE the attempt cap is reached.
  sinkTimeout.mode = 'ok';
  const evId = timedRow.event_id;
  const delivered = await waitFor(async () => {
    await sleep(60);
    const cur = (await req('GET', `/api/subscriptions/${subT.json.id}`, TOK.admin)).json;
    return cur.last_acked_cursor >= 1 ? cur : null;
  }, { tries: 60, label: 'acked cursor advanced after recovery' });
  check('acked cursor advanced to 1 after recovery', delivered && delivered.last_acked_cursor === 1,
    JSON.stringify(delivered?.last_acked_cursor));
  check('event eventually delivered (after retries) with the same stable id',
    sinkTimeout.received.some((r) => r.body.event_id === evId));
}

// ------------------------------------------- 4. ack loss -> duplicate, order --
console.log('4. 确认丢失造成重复投递：稳定标识去重、游标不倒退、不跳过未确认');
{
  const subD = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'drop-sub', callback_url: 'http://127.0.0.1:9941/d', events: ['decision.created'],
  });
  sinkA.mode = 'drop-once';
  const n0 = sinkA.received.length;
  await settleEnvelope('obj-drop', { region: 'd:1', change: 'dd1', reqkey: 'rd1', site: 'st-d', seq: 1 });
  // First POST: receiver acts, resets socket -> sender retries -> duplicate.
  const dup = await waitFor(() => {
    const rs = sinkA.received.filter((r) =>
      r.body.object_key === 'obj-drop' && r.headers['x-convergence-subscription'] === subD.json.id);
    return rs.length >= 2 ? rs : null;
  }, { tries: 60, label: 'duplicate redelivery after ack loss' });
  check('ack loss causes a redelivery', !!dup);
  if (dup) {
    check('duplicate keeps the SAME stable event id', dup[0].body.event_id === dup[1].body.event_id);
    check('duplicate keeps the SAME business cursor', dup[0].body.cursor === dup[1].body.cursor);
    check('duplicate keeps the SAME per-subscription delivery cursor',
      dup[0].body.delivery_cursor === dup[1].body.delivery_cursor,
      JSON.stringify(dup.map((d) => d.body.delivery_cursor)));
    check('receiver-side duplicate flag works (dedup key = event id)', dup[1].duplicated === true);
  }
  const cur = await req('GET', `/api/subscriptions/${subD.json.id}`, TOK.admin);
  check('confirmed cursor is 1 (never regresses)', cur.json.last_acked_cursor === 1, String(cur.json.last_acked_cursor));

  // The in-line cursor sequence for this subscription is strictly ordered and
  // never regresses, even with the duplicate redelivery in it.
  const deliveredForSub = sinkA.received.filter((r) => r.headers['x-convergence-subscription'] === subD.json.id);
  const deliveryCursors = deliveredForSub.map((r) => r.body.delivery_cursor);
  check('cursors delivered in non-decreasing order (duplicate repeats, never regresses)',
    deliveryCursors.every((c, i) => i === 0 || c >= deliveryCursors[i - 1]), JSON.stringify(deliveryCursors));
  sinkA.mode = 'ok';
}

// ------------------------------------------------ 5. key rotation boundary ----
console.log('5. 密钥更换边界：已出箱批次沿用旧版本，新批次用当前版本');
{
  const subK = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'key-sub', callback_url: 'http://127.0.0.1:9945/k', events: ['decision.created'],
    signing_secret: '0123456789abcdef0123456789abcdef',
  });
  check('custom signing secret accepted', subK.status === 200);
  // Pause so the first event stays in the outbox bound to v1.
  await req('POST', `/api/subscriptions/${subK.json.id}/pause`, TOK.admin);
  await settleEnvelope('obj-key', { region: 'k:1', change: 'kk1', reqkey: 'rkey-1', site: 'st-k', seq: 1 });
  await sleep(150);
  const rot = await req('POST', `/api/subscriptions/${subK.json.id}/rotate-key`, TOK.admin, {});
  check('rotation creates v2', rot.status === 200 && rot.json.current_key_version === 'v2');
  check('both key versions retained with fingerprints',
    rot.json.key_versions.length === 2 && rot.json.key_versions.every((k) => k.fingerprint));
  check('rotation response carries the new secret once', /^[a-f0-9]{32,}$/.test(rot.json.signing_secret || ''));
  await settleEnvelope('obj-key2', { region: 'k2:1', change: 'kk2', reqkey: 'rkey-2', site: 'st-k2', seq: 1 });
  await req('POST', `/api/subscriptions/${subK.json.id}/resume`, TOK.admin);

  const got = await waitFor(() => {
    const rows = sinkKey.received.filter((r) => r.headers['x-convergence-subscription'] === subK.json.id);
    return rows.length >= 2 ? rows : null;
  }, { tries: 80, label: 'both key-boundary events delivered' });
  if (got) {
    const byObj = Object.fromEntries(got.map((r) => [r.body.object_key, r.body]));
    check('in-boxed batch still signed with v1', byObj['obj-key']?.key_version === 'v1', JSON.stringify(Object.keys(byObj)));
    check('new batch signed with v2', byObj['obj-key2']?.key_version === 'v2');
    check('v1 envelope verifies against the OLD key', verifySignature('0123456789abcdef0123456789abcdef', byObj['obj-key']));
    check('v2 envelope verifies against the NEW key', verifySignature(rot.json.signing_secret, byObj['obj-key2']));
  }
}

// ------------------------------------------------- 6. pause backlog / order ---
console.log('6. 暂停期间积压，恢复后按业务顺序投递');
{
  const subP = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'pause-sub', callback_url: 'http://127.0.0.1:9941/p', events: ['decision.created'],
  });
  await req('POST', `/api/subscriptions/${subP.json.id}/pause`, TOK.admin);
  for (let i = 1; i <= 3; i++) {
    await settleEnvelope('obj-pause', { region: `p:${i}`, change: `pp${i}`, reqkey: `rp${i}`, site: 'st-p', seq: i });
  }
  await sleep(150);
  const whilePaused = sinkA.received.filter((r) => r.headers['x-convergence-subscription'] === subP.json.id);
  check('nothing delivered while paused', whilePaused.length === 0, String(whilePaused.length));
  const pausedView = await req('GET', `/api/subscriptions/${subP.json.id}`, TOK.admin);
  check('backlog visible (pending_count=3)', pausedView.json.pending_count === 3, String(pausedView.json.pending_count));
  check('paused status shown', pausedView.json.status === 'paused');

  await req('POST', `/api/subscriptions/${subP.json.id}/resume`, TOK.admin);
  const rows = await waitFor(() => {
    const rs = sinkA.received.filter((r) => r.headers['x-convergence-subscription'] === subP.json.id);
    return rs.length === 3 ? rs : null;
  }, { tries: 80, label: 'backlog flushed' });
  if (rows) {
    check('backlog flushed in business order', rows.map((r) => r.body.cursor).join(',') === '1,2,3',
      JSON.stringify(rows.map((r) => r.body.cursor)));
    check('delivery cursors also 1,2,3', rows.map((r) => r.body.delivery_cursor).join(',') === '1,2,3');
  }
  const after = await req('GET', `/api/subscriptions/${subP.json.id}`, TOK.admin);
  check('acked cursor 3 after flush', after.json.last_acked_cursor === 3, String(after.json.last_acked_cursor));
}

// --------------------------------------------------- 7. DLQ + manual resend ---
console.log('7. 达到重试上限进入死信；人工再发送沿用原事件标识与业务顺序');
{
  const subQ = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'dlq-sub', callback_url: 'http://127.0.0.1:9946/q', events: ['decision.created'],
  });
  sinkDlq.mode = 'fail';
  await settleEnvelope('obj-dlq', { region: 'q:1', change: 'qq1', reqkey: 'rq1', site: 'st-q', seq: 1 });
  const deadRow = await waitFor(async () => {
    const { json } = await req('GET', '/api/subscriptions/dead-letters', TOK.admin);
    const d = (json.dead_letters || []).find((x) => x.subscription_id === subQ.json.id);
    return d || null;
  }, { tries: 80, interval: 60, label: 'item entered dead letter' });
  check('item dead after 3 attempts', deadRow && deadRow.attempts === 3, JSON.stringify(deadRow?.attempts));
  check('dead reason recorded', /non_success_status:500/.test(deadRow?.dead_reason || ''), deadRow?.dead_reason);
  check('dead letter exposes object key + cursor but no secret', deadRow && deadRow.object_key === 'obj-dlq'
    && !JSON.stringify(deadRow).includes('signing_secret'));
  check('operator can read dead letters', (await req('GET', '/api/subscriptions/dead-letters', TOK.op)).status === 200);
  check('operator cannot resend', (await req('POST', `/api/subscriptions/deliveries/${deadRow.delivery_id}/resend`, TOK.op)).status === 403);
  check('inspector cannot read dead letters', (await req('GET', '/api/subscriptions/dead-letters', TOK.insp)).status === 403);

  // A second event arrives behind the dead head: must stay blocked.
  await settleEnvelope('obj-dlq', { region: 'q:2', change: 'qq2', reqkey: 'rq2', site: 'st-q', seq: 2 });
  await sleep(300);
  const blocked = sinkDlq.received.filter((r) => r.headers['x-convergence-subscription'] === subQ.json.id);
  check('dead head blocks later events (no skipping)', blocked.length === 0, String(blocked.length));

  // Human resend: receiver healthy again.
  sinkDlq.mode = 'ok';
  const resend = await req('POST', `/api/subscriptions/deliveries/${deadRow.delivery_id}/resend`, TOK.admin);
  check('resend accepted 200', resend.status === 200, JSON.stringify(resend.json));
  const flushed = await waitFor(() => {
    const rs = sinkDlq.received.filter((r) => r.headers['x-convergence-subscription'] === subQ.json.id);
    return rs.length >= 2 ? rs : null;
  }, { tries: 60, label: 'resend unlocked the line' });
  if (flushed) {
    check('resent event keeps ORIGINAL event id', flushed[0].body.event_id === deadRow.event_id);
    check('resent event keeps ORIGINAL cursor 1', flushed[0].body.cursor === 1 && flushed[0].body.delivery_cursor === 1);
    check('blocked event 2 then flows and is not skipped',
      flushed.some((r) => r.body.cursor === 2), JSON.stringify(flushed.map((r) => r.body.cursor)));
    check('order after resend is 1 then 2',
      flushed.map((r) => r.body.delivery_cursor).join(',') === '1,2',
      JSON.stringify(flushed.map((r) => r.body.delivery_cursor)));
  }
  const view = await req('GET', `/api/subscriptions/${subQ.json.id}`, TOK.admin);
  check('no dead letters remain', view.json.dead_count === 0, String(view.json.dead_count));
  check('acked cursor reached 2', view.json.last_acked_cursor === 2, String(view.json.last_acked_cursor));
}

// ----------------------------------------------------------- 8. process restart
console.log('8. 进程恢复：待发送不丢、不重复、游标不倒退');
{
  const subR = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'restart-sub', callback_url: 'http://127.0.0.1:9941/r', events: ['decision.created'],
  });
  await req('POST', `/api/subscriptions/${subR.json.id}/pause`, TOK.admin);
  await settleEnvelope('obj-restart', { region: 'rt:1', change: 'rr1', reqkey: 'rrt1', site: 'st-r', seq: 1 });
  await sleep(100);
  await stopServer();
  await sleep(150);
  const before = sinkA.received.filter((x) => x.headers['x-convergence-subscription'] === subR.json.id).length;
  await startServer();
  const subs = (await req('GET', '/api/subscriptions', TOK.admin)).json.subscriptions;
  const rebuilt = subs.find((s) => s.id === subR.json.id);
  check('subscription rebuilt from WAL after restart', !!rebuilt && rebuilt.status === 'paused');
  check('pending item survived restart', rebuilt.pending_count === 1, String(rebuilt.pending_count));
  check('key history rebuilt (still v1)', rebuilt.current_key_version === 'v1' && rebuilt.key_versions.length === 1);
  await req('POST', `/api/subscriptions/${subR.json.id}/resume`, TOK.admin);
  const got = await waitFor(() => {
    const rs = sinkA.received.filter((x) => x.headers['x-convergence-subscription'] === subR.json.id);
    return rs.length >= before + 1 ? rs : null;
  }, { tries: 80, label: 'post-restart delivery' });
  check('pending delivered after restart exactly once', got && got.length === before + 1,
    `before=${before} after=${got?.length}`);
}

// ------------------------------------------------- 9. slow callback isolation --
console.log('9. 回调缓慢不阻塞汇合请求与历史查询');
{
  const subS = await req('POST', '/api/subscriptions', TOK.admin, {
    name: 'slow-sub', callback_url: 'http://127.0.0.1:9943/slow', events: ['decision.created'],
  });
  sinkSlow.latencyMs = 250;
  const t0 = Date.now();
  const r = await settleEnvelope('obj-slow', { region: 's:1', change: 'ss1', reqkey: 'rs1', site: 'st-s', seq: 1 });
  const submitMs = Date.now() - t0;
  check('envelope submission returns immediately despite slow callback', r.status === 200 && submitMs < 500, `${submitMs}ms`);
  const t1 = Date.now();
  const hist = await req('GET', '/api/objects/obj-slow', TOK.op);
  const histMs = Date.now() - t1;
  check('history query is not blocked by the in-flight callback', hist.status === 200 && histMs < 500, `${histMs}ms`);
  const t2 = Date.now();
  const rec = await req('GET', '/api/receipts', TOK.insp);
  check('inspector history read unaffected', rec.status === 200 && Date.now() - t2 < 500);
  await waitFor(() => sinkSlow.received.length >= 1 ? true : null, { label: 'slow sink eventually got it' });
  sinkSlow.latencyMs = 0;
}

// -------------------------------------- 10. RBAC + desensitised proofs --------
console.log('10. 角色限制：管理员管理、操作员只读、监察员只读脱敏通知证明');
{
  check('operator cannot create subscriptions', (await req('POST', '/api/subscriptions', TOK.op, {
    name: 'x', callback_url: 'http://127.0.0.1:9941', events: ['decision.created'],
  })).status === 403);
  check('reviewer cannot list subscriptions', (await req('GET', '/api/subscriptions', TOK.r1)).status === 403);
  check('inspector cannot list subscriptions', (await req('GET', '/api/subscriptions', TOK.insp)).status === 403);
  check('unauthenticated rejected', (await req('GET', '/api/subscriptions')).status === 401);
  check('bad token rejected', (await req('GET', '/api/subscriptions', TOK.bogus)).status === 403);
  check('operator view has no signing secret',
    !JSON.stringify((await req('GET', '/api/subscriptions', TOK.op)).json).includes('signing_secret'));

  // Inspector proofs.
  const proofs = (await req('GET', '/api/subscriptions/events?object_key=obj-hook-1', TOK.insp)).json.proofs || [];
  check('inspector lists desensitised proofs', proofs.length >= 1, String(proofs.length));
  const p0 = proofs[0];
  check('proof has event id/key/kind/cursor/signature',
    p0.event_id && p0.object_key === 'obj-hook-1' && p0.signature && p0.delivery_cursor);
  const text = JSON.stringify(p0);
  check('proof has no callback URL / secret / name / error text',
    !text.includes('callback_url') && !text.includes('9941') && !text.includes('compliance-a'));
  check('proof has no site/request/image', !text.includes('station-alpha') && !text.includes('rk1') && !text.includes(digest('c1')));

  // Verify proof endpoint (inspector): reconstruct the exact envelope that was
  // delivered (the signed content), as a compliance auditor would.
  const deliveredEnv = sinkA.received.find((r) => r.body.event_id === p0.event_id)?.body;
  const v = await req('POST', '/api/subscriptions/events/verify', TOK.insp, deliveredEnv);
  check('inspector verifies a valid proof', v.status === 200 && v.json.ok === true, JSON.stringify(v.json));
  const tampered = { ...deliveredEnv, object_key: 'obj-forged' };
  const vt = await req('POST', '/api/subscriptions/events/verify', TOK.insp, tampered);
  check('tampered proof rejected deterministically', vt.status === 422 && vt.json.error.code === 'PROOF_SIG_INVALID',
    JSON.stringify(vt.json?.error));
  // Fetching one event by id (inspector).
  const one = await req('GET', `/api/subscriptions/events/${p0.event_id}`, TOK.insp);
  check('inspector fetches a single event proof', one.status === 200 && one.json.proofs[0].event_id === p0.event_id);
  check('operator is not offered proof verification (admin/inspector channel only)',
    (await req('POST', '/api/subscriptions/events/verify', TOK.op, deliveredEnv)).status === 403);
}

// ---------------------------- 11. conflict.closed + cold_segment.generated ----
console.log('11. 冲突闭合与冷段生成事件（同样脱敏）');
{
  const o = 'obj-conf-events';
  const dA = digest('ce-A'), dB = digest('ce-B');
  await Promise.all([
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 's1', site_seq: 1, coverage_region: 'reg',
      change_digest: dA, predecessor_digests: [], request_key: 'rce-a',
    }),
    req('POST', '/api/envelopes', TOK.op, {
      object_key: o, site: 's2', site_seq: 1, coverage_region: 'reg',
      change_digest: dB, predecessor_digests: [], request_key: 'rce-b',
    }),
  ]);
  await sleep(150);
  const cs = (await req('GET', '/api/cases', TOK.r1)).json.cases.find((c) => c.object_key === o);
  check('conflict opened', !!cs);
  await req('POST', `/api/reviews/${cs.case_id}`, TOK.r1, { choices: { reg: dA } });
  await req('POST', `/api/reviews/${cs.case_id}`, TOK.r2, { choices: { reg: dA } });

  const both = await waitFor(() => {
    const m = sinkA.byEvent();
    const kinds = [...m.values()].map((r) => r.body).filter((b) => b.object_key === o).map((b) => b.kind).sort();
    return kinds.length >= 2 ? kinds : null;
  }, { tries: 60, label: 'decision + conflict-closed events' });
  check('adjudication emits decision.created AND conflict.closed',
    both && both.join(',') === 'conflict.closed,decision.created', JSON.stringify(both));
  const closed = sinkA.received.map((r) => r.body).find((b) => b.object_key === o && b.kind === 'conflict.closed');
  check('conflict.closed carries case id and business cursor', closed && closed.detail.case_id === cs.case_id && closed.cursor === 1);
  check('conflict event has no site/request/image',
    !JSON.stringify(closed).includes('s1') && !JSON.stringify(closed).includes('rce-a') && !JSON.stringify(closed).includes(dA));

  // Cold segment.
  const cp = await req('POST', '/api/archive/checkpoints', TOK.op, { object_key: o, task_key: 'cp-events-1' });
  check('checkpoint created', cp.status === 200, JSON.stringify(cp.json?.error));
  const segId = cp.json.segment_id;
  const cold = await waitFor(() => {
    const m = sinkA.byEvent();
    return [...m.values()].map((r) => r.body).find((b) => b.kind === 'cold_segment.generated' && b.object_key === o) || null;
  }, { tries: 60, label: 'cold segment event' });
  check('cold_segment.generated emitted', !!cold, 'missing');
  if (cold) {
    check('cold cursor = boundary seq', cold.cursor === 1 && cold.cursor_name === 'boundary_receipt_seq');
    check('cold detail references segment id', cold.detail.segment_id === segId);
    check('cold event has no site/request/image content',
      !JSON.stringify(cold).includes('s1') && !JSON.stringify(cold).includes('rce-a') && !JSON.stringify(cold).includes(dA));
  }
}

// ----------------------- 12. transactional outbox crash-window repair ----------
console.log('12. 业务提交与事件入箱为同一持久化结果（崩溃窗口对账修复）');
{
  // Build a fresh decision directly through a second store instance sharing the
  // data dir while the server is stopped, then CORRUPT the atomicity by removing
  // the T_EVENT frame that accompanies the T_DECISION frame (simulating a
  // pre-outbox writer / a hand-crafted commit). Startup reconciliation must
  // detect the durable business fact without an event and re-enqueue it.
  await stopServer();
  const { digestOf } = await import(path.join(ROOT, 'src', 'crypto-util.mjs').replace(/\\/g, '/'));
  const walPath = path.join(DATA, 'converge.wal');
  const { parseFrames, WAL_MAGIC, encodeFrame } = await import(path.join(ROOT, 'src', 'wal.mjs').replace(/\\/g, '/'));
  const buf = fs.readFileSync(walPath);
  const { frames } = parseFrames(buf, WAL_MAGIC, { allowTornTail: false });
  // Find the last T_DECISION frame (type 2) and its following T_EVENT (6)/
  // T_DELIVERY (7) frames; rewrite the WAL keeping everything EXCEPT the event
  // frames of that last decision, as if the business committed without outbox.
  let lastDecisionIdx = -1;
  frames.forEach((f, i) => { if (f.type === 2) lastDecisionIdx = i; });
  const decisionFrame = frames[lastDecisionIdx];
  const decision = JSON.parse(decisionFrame.payload.toString('utf8'));
  const receiptHash = decision.receipt.hash;
  const decisionEventId = 'ev_' + digestOf({ kind: 'decision.created', fact_key: receiptHash }).slice(0, 32);
  const rebuilt = [];
  for (const f of frames) {
    if (f.type === 6) {
      const ev = JSON.parse(f.payload.toString('utf8'));
      if (ev.event_id === decisionEventId) continue; // drop its event fact
    }
    if (f.type === 7) {
      const doc = JSON.parse(f.payload.toString('utf8'));
      if (doc.action === 'enqueue' && doc.event_id === decisionEventId) continue;
    }
    rebuilt.push({ type: f.type, payload: f.payload });
  }
  const fd = fs.openSync(walPath, 'w');
  for (const f of rebuilt) {
    const out = encodeFrame(WAL_MAGIC, f.type, f.payload);
    fs.writeSync(fd, out, 0, out.length);
  }
  fs.fsyncSync(fd); fs.closeSync(fd);

  await startServer();
  // A subscription already existed that matches decision.created; the repaired
  // event must have been re-enqueued and delivered.
  const repaired = await waitFor(async () => {
    const { json } = await req('GET', `/api/subscriptions/events?object_key=${decision.node.object_key}`, TOK.admin).catch(() => ({ json: { proofs: [] } }));
    const rows = (json.proofs || []).filter((p) => p.detail?.receipt_hash === receiptHash);
    return rows.length ? rows : null;
  }, { tries: 40, label: 'reconciled event present after restart' });
  check('startup repaired the missing outbox event for a durable decision', !!repaired,
    `receipt=${receiptHash.slice(0, 12)}`);
  // Business results are intact and queryable regardless of delivery.
  const obj = await req('GET', `/api/objects/${decision.node.object_key}`, TOK.op);
  check('convergence history still queryable through the repair', obj.status === 200 && obj.json.current?.hash === receiptHash);
}

// --------------------------------------------------------------------------
console.log(`\n结果：${passed} 通过，${failed} 失败；数据目录 ${DATA}`);
await sinkA.close().catch(() => {});
await sinkB.close().catch(() => {});
await sinkSlow.close().catch(() => {});
await sinkTimeout.close().catch(() => {});
await sinkKey.close().catch(() => {});
await sinkDlq.close().catch(() => {});
await stopServer().catch(() => {});
process.exit(failed ? 1 : 0);
