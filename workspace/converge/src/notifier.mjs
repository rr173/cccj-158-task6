import crypto from 'node:crypto';
import { digestOf, sign } from './crypto-util.mjs';

/**
 * Downstream compliance event subscription, transactional outbox and reliable
 * sender.
 *
 * Durability model (everything lives in the SAME fsynced WAL as the business):
 *  - T_EVENT (6): an immutable event fact (new decision / conflict closure /
 *    cold segment). The store appends the business frame and the event + inbox
 *    frames in ONE wal.appendMany() call (one write, one fsync), so a business
 *    commit can never become durable without its notifications. Startup
 *    reconciliation re-checks every durable fact against the event registry and
 *    repairs the residual crash-after-commit window.
 *  - T_DELIVERY (7): subscription snapshots (full-document, latest wins) plus
 *    inbox lifecycle frames (enqueue / attempt / ack / dead / resend).
 *
 * Ordering model: every subscription has its own monotonic business cursor
 * assigned at enqueue time, strictly in the order facts are committed. The
 * sender only ever POSTs the lowest-cursor unconfirmed row of a subscription
 * ("head of line"); a dead row blocks later rows until a human resends it, so
 * an unconfirmed event is never skipped and the confirmed cursor never moves
 * backwards. Confirmation loss therefore causes at most a duplicate delivery
 * (deduplicated downstream by the stable event id).
 *
 * Signing-key model: every inbox row binds the subscription's CURRENT key
 * version at enqueue time. Pause / resume / rotation never touch already
 * enqueued rows: old batches keep signing with their bound version; new batches
 * use the current version. Old key material is retained (never deleted) so an
 * in-flight batch can always be re-signed after a restart.
 */

export const T_EVENT = 6;
export const T_DELIVERY = 7;

export const EVENT_KINDS = ['decision.created', 'conflict.closed', 'cold_segment.generated'];

const URL_RE = /^https?:\/\/[^\s/$.?#].[^\s]*$/i;
const NAME_RE = /^[A-Za-z0-9:._\-]{1,80}$/;
const SECRET_RE = /^[A-Za-z0-9._\-]{16,200}$/;

export class NotificationError extends Error {
  constructor(status, code, message, details = undefined) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

function randomHex(bytes) {
  return crypto.randomBytes(bytes).toString('hex');
}

function nowIso() {
  return new Date().toISOString();
}

/** Stable, deterministic event id for a fact: re-running planning after a crash
 * resolves to the same id, so reconciliation can detect "already in the inbox"
 * without guessing. */
export function eventIdFor(kind, factKey) {
  return 'ev_' + digestOf({ kind, fact_key: factKey }).slice(0, 32);
}

export class Notifier {
  constructor(config) {
    this.config = config;
    this.dcfg = config.delivery || {};

    this.events = new Map(); // event_id -> event fact
    this.eventOrder = [];    // event ids in durable commit order

    this.subscriptions = new Map(); // sub_id -> subscription doc
    this.deliveries = new Map();    // delivery_id -> row

    this._timer = null;
    this._sending = false;
    this._stopped = false;
    this._store = null; // injected by the store after construction
  }

  // ------------------------------------------------------- store integration --

  bindStore(store) {
    this._store = store;
  }

  start() {
    this._stopped = false;
    if (this.dcfg.enabled === false) return;
    if (this._timer) return;
    this._timer = setInterval(() => { this.tick().catch(() => {}); }, this.dcfg.tickMs || 100);
    if (this._timer.unref) this._timer.unref();
  }

  stop() {
    this._stopped = true;
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
  }

  /** Apply one durable T_EVENT / T_DELIVERY frame to in-memory state (replay). */
  replayFrame(type, payloadBuf) {
    const doc = JSON.parse(payloadBuf.toString('utf8'));
    if (type === T_EVENT) return this._indexEvent(doc);
    if (type === T_DELIVERY) return this._applyDelivery(doc);
  }

  _indexEvent(ev) {
    if (!this.events.has(ev.event_id)) {
      this.events.set(ev.event_id, ev);
      this.eventOrder.push(ev.event_id);
    }
  }

  _applyDelivery(doc) {
    if (doc.action === 'subscription') {
      // Full snapshot; the newest frame wins. Key history is embedded so rows
      // bound to older versions stay signable forever.
      this.subscriptions.set(doc.subscription.id, doc.subscription);
      return;
    }
    if (doc.action === 'enqueue') {
      const row = {
        delivery_id: doc.delivery_id,
        subscription_id: doc.subscription_id,
        event_id: doc.event_id,
        cursor: doc.cursor,
        key_version: doc.key_version,
        status: 'pending',
        attempts: 0,
        not_before: 0,
        last_error: null,
        last_status: null,
        created_at: doc.created_at,
        updated_at: doc.created_at,
      };
      this.deliveries.set(row.delivery_id, row);
      const sub = this.subscriptions.get(row.subscription_id);
      if (sub && sub.next_cursor < row.cursor) sub.next_cursor = row.cursor;
      return;
    }
    const row = this.deliveries.get(doc.delivery_id);
    if (!row) return; // frame for an inbox row older than any state we rebuilt (cannot normally happen)
    if (doc.action === 'attempt') {
      row.attempts = doc.attempts;
      row.not_before = doc.not_before;
      row.last_error = doc.error;
      row.last_status = doc.last_status;
      row.status = doc.status; // 'retry' | 'dead'
      row.updated_at = doc.at;
    } else if (doc.action === 'ack') {
      row.status = 'acked';
      row.attempts = doc.attempts;
      row.not_before = 0;
      row.last_error = null;
      row.last_status = 200;
      row.acked_at = doc.at;
      row.updated_at = doc.at;
      const sub = this.subscriptions.get(row.subscription_id);
      if (sub) {
        if (doc.cursor > (sub.acked_cursor || 0)) sub.acked_cursor = doc.cursor;
        if (!sub.last_ack || row.cursor >= (sub.last_ack.cursor || 0)) {
          sub.last_ack = { cursor: row.cursor, event_id: row.event_id, at: doc.at, delivery_id: row.delivery_id };
        }
      }
    } else if (doc.action === 'resend') {
      // Human replay: SAME event id, SAME business cursor (original order),
      // attempts reset, still bound to the key version the batch was created
      // with. It re-enters the head of the line.
      row.status = 'pending';
      row.attempts = 0;
      row.not_before = doc.not_before;
      row.last_error = null;
      row.last_status = null;
      row.resent_at = doc.at;
      row.updated_at = doc.at;
    }
  }

  // ------------------------------------------------------------ planning (in --
  // -------- store lock, synchronous; frames land in the SAME fsync as fact) --

  /**
   * Build the event + inbox frames for one durable fact. The store appends the
   * returned frames together with its business frame via wal.appendMany() and
   * then calls afterAppend() to mirror them into live state.
   *
   * `fact`: {kind, object_key, cursor, cursor_name, detail, occurred_at, fact_key}
   */
  plan(fact) {
    const eventId = fact.event_id || eventIdFor(fact.kind, fact.fact_key);
    if (this.events.has(eventId)) {
      return { event: this.events.get(eventId), eventFrame: null, enqueueFrames: [], planned: [] };
    }
    const event = {
      event_id: eventId,
      kind: fact.kind,
      object_key: fact.object_key,
      cursor: fact.cursor,
      cursor_name: fact.cursor_name,
      detail: fact.detail || {},
      occurred_at: fact.occurred_at || nowIso(),
    };
    const eventFrame = { type: T_EVENT, payload: JSON.stringify(event) };
    const enqueueFrames = [];
    const planned = [];
    const createdAt = nowIso();
    for (const sub of [...this.subscriptions.values()].sort((a, b) => (a.id < b.id ? -1 : 1))) {
      if (sub.status !== 'active' && sub.status !== 'paused') continue; // deleted/defunct
      if (!sub.events.includes(fact.kind)) continue;
      sub.next_cursor = (sub.next_cursor || 0) + 1;
      const deliveryId = 'dlv_' + randomHex(12);
      const doc = {
        action: 'enqueue',
        delivery_id: deliveryId,
        subscription_id: sub.id,
        event_id: eventId,
        cursor: sub.next_cursor,
        key_version: sub.current_key_version,
        created_at: createdAt,
      };
      enqueueFrames.push({ type: T_DELIVERY, payload: JSON.stringify(doc) });
      planned.push(doc);
    }
    return { event, eventFrame, enqueueFrames, planned };
  }

  /** Mirror a planned+committed fact into live state (call after appendMany). */
  afterAppend(plan) {
    if (plan.eventFrame) this._indexEvent(plan.event);
    for (const doc of plan.planned) this._applyDelivery(doc);
  }

  /**
   * Startup repair: facts durable in the WAL but missing from the event registry
   * (the residual crash window / mixed writers) get their events + inbox rows
   * appended now. With the single-fsync appendMany this path is unreachable in
   * normal operation; it exists so the "business committed but never notified"
   * gap is structurally impossible even if a commit came from an older writer.
   * Must run AFTER WAL replay and after settlement recovery.
   */
  reconcile(facts, appendMany) {
    const repaired = [];
    for (const fact of facts) {
      const plan = this.plan(fact);
      if (!plan.eventFrame) continue;
      const frames = [plan.eventFrame, ...plan.enqueueFrames];
      appendMany(frames);
      this.afterAppend(plan);
      repaired.push(plan.event.event_id);
    }
    return repaired;
  }

  // -------------------------------------------------------- subscription admin --

  _keyFingerprint(secret) {
    return 'sha256:' + digestOf({ key: secret }).slice(0, 16);
  }

  createSubscription(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new NotificationError(400, 'BAD_REQUEST', 'subscription must be a JSON object');
    }
    const name = body.name;
    if (typeof name !== 'string' || !NAME_RE.test(name)) {
      throw new NotificationError(400, 'BAD_REQUEST', 'name must be 1-80 chars [A-Za-z0-9:._-]');
    }
    const callback_url = body.callback_url;
    if (typeof callback_url !== 'string' || !URL_RE.test(callback_url) || callback_url.length > 500) {
      throw new NotificationError(400, 'BAD_REQUEST', 'callback_url must be a valid http(s) URL');
    }
    const events = this._validateEvents(body.events);
    const status = body.status === 'paused' ? 'paused' : 'active';
    const secret = body.signing_secret != null ? body.signing_secret : randomHex(32);
    if (typeof secret !== 'string' || !SECRET_RE.test(secret)) {
      throw new NotificationError(400, 'BAD_REQUEST', 'signing_secret must be 16-200 chars [A-Za-z0-9._-] (or omit to auto-generate)');
    }
    const ts = nowIso();
    const doc = {
      id: 'sub_' + randomHex(10),
      name,
      callback_url,
      events,
      status,
      current_key_version: 'v1',
      keys: { v1: { secret, created_at: ts, fingerprint: this._keyFingerprint(secret) } },
      next_cursor: 0,
      acked_cursor: 0,
      last_ack: null,
      created_at: ts,
      updated_at: ts,
    };
    const frame = { type: T_DELIVERY, payload: JSON.stringify({ action: 'subscription', subscription: doc }) };
    this._commitFrames([frame]);
    this.subscriptions.set(doc.id, doc);
    return this._adminView(doc, { include_secret: true });
  }

  updateSubscription(id, patch) {
    const sub = this._require(id);
    const next = { ...sub, keys: { ...sub.keys } };
    if (patch.name !== undefined) {
      if (typeof patch.name !== 'string' || !NAME_RE.test(patch.name)) {
        throw new NotificationError(400, 'BAD_REQUEST', 'name must be 1-80 chars [A-Za-z0-9:._-]');
      }
      next.name = patch.name;
    }
    if (patch.callback_url !== undefined) {
      if (typeof patch.callback_url !== 'string' || !URL_RE.test(patch.callback_url)) {
        throw new NotificationError(400, 'BAD_REQUEST', 'callback_url must be a valid http(s) URL');
      }
      next.callback_url = patch.callback_url;
    }
    if (patch.events !== undefined) next.events = this._validateEvents(patch.events);
    if (patch.status !== undefined) {
      if (!['active', 'paused'].includes(patch.status)) {
        throw new NotificationError(400, 'BAD_REQUEST', "status must be 'active' or 'paused'");
      }
      next.status = patch.status;
    }
    next.updated_at = nowIso();
    this._persistSubscription(next);
    return this._adminView(next);
  }

  pause(id) { return this.updateSubscription(id, { status: 'paused' }); }
  resume(id) { return this.updateSubscription(id, { status: 'active' }); }

  rotateKey(id, body = {}) {
    const sub = this._require(id);
    const next = { ...sub, keys: { ...sub.keys } };
    const prevN = Number(next.current_key_version.slice(1));
    const version = `v${prevN + 1}`;
    const secret = body.signing_secret != null ? body.signing_secret : randomHex(32);
    if (typeof secret !== 'string' || !SECRET_RE.test(secret)) {
      throw new NotificationError(400, 'BAD_REQUEST', 'signing_secret must be 16-200 chars [A-Za-z0-9._-] (or omit to auto-generate)');
    }
    const ts = nowIso();
    next.keys[version] = secret ? { secret, created_at: ts, fingerprint: this._keyFingerprint(secret) } : next.keys[version];
    next.current_key_version = version;
    next.updated_at = ts;
    this._persistSubscription(next);
    return this._adminView(next, { include_secret: true });
  }

  _validateEvents(events) {
    if (!Array.isArray(events) || !events.length) {
      throw new NotificationError(400, 'BAD_REQUEST', 'events must be a non-empty array');
    }
    const out = [...new Set(events)];
    for (const k of out) {
      if (!EVENT_KINDS.includes(k)) throw new NotificationError(400, 'BAD_REQUEST', `unknown event kind ${k}`);
    }
    return out.sort();
  }

  _require(id) {
    const sub = this.subscriptions.get(id);
    if (!sub) throw new NotificationError(404, 'SUBSCRIPTION_NOT_FOUND', 'unknown subscription');
    return sub;
  }

  _persistSubscription(doc) {
    const frame = { type: T_DELIVERY, payload: JSON.stringify({ action: 'subscription', subscription: doc }) };
    this._commitFrames([frame]);
    this.subscriptions.set(doc.id, doc);
  }

  /** Persist inbox frames under the store mutex (synchronous WAL append). */
  _commitFrames(frames) {
    if (!this._store) throw new Error('notifier not bound to a store');
    this._store.commitNotificationFrames(frames);
  }

  /**
   * Persist from the sender (HTTP work happened outside the lock). The fetch
   * itself never holds the mutex, so a slow downstream cannot block
   * convergence/history; only the quick fsync enters the critical section,
   * keeping ack/dead frames ordered against business + enqueue frames.
   */
  async _commitFramesFromSender(frames) {
    if (!this._store) throw new Error('notifier not bound to a store');
    await this._store.lock.runExclusive(() => this._store.commitNotificationFrames(frames));
  }

  // --------------------------------------------------------------- resend DLQ --

  resendDead(deliveryId) {
    const row = this.deliveries.get(deliveryId);
    if (!row) throw new NotificationError(404, 'DELIVERY_NOT_FOUND', 'unknown delivery');
    if (row.status !== 'dead') {
      throw new NotificationError(409, 'NOT_DEAD', 'only dead-letter items can be manually resent', { status: row.status });
    }
    const at = nowIso();
    const doc = { action: 'resend', delivery_id: row.delivery_id, at, not_before: 0 };
    const frame = { type: T_DELIVERY, payload: JSON.stringify(doc) };
    this._commitFrames([frame]);
    this._applyDelivery(doc);
    return this.deadView(row);
  }

  // ------------------------------------------------------------------- views --

  _rowsOf(subId) {
    return [...this.deliveries.values()]
      .filter((r) => r.subscription_id === subId)
      .sort((a, b) => a.cursor - b.cursor);
  }

  _counts(sub) {
    let pending = 0;
    let in_flight = 0;
    let dead = 0;
    let next_cursor = null;
    for (const r of this._rowsOf(sub.id)) {
      if (r.status === 'acked') continue;
      if (next_cursor === null || r.cursor < next_cursor) next_cursor = r.cursor;
      if (r.status === 'dead') dead++;
      else if (r.status === 'sending') in_flight++;
      else pending++;
    }
    return {
      pending_count: pending,      // due or backing off
      in_flight_count: in_flight,
      dead_count: dead,
      backlog_count: pending + in_flight + dead,
      next_cursor,                 // the cursor blocking the line
      last_acked_cursor: sub.acked_cursor || 0,
    };
  }

  _adminView(sub, { include_secret = false } = {}) {
    const counts = this._counts(sub);
    const view = {
      id: sub.id,
      name: sub.name,
      callback_url: sub.callback_url,
      events: sub.events,
      status: sub.status,
      created_at: sub.created_at,
      updated_at: sub.updated_at,
      current_key_version: sub.current_key_version,
      key_versions: Object.entries(sub.keys)
        .sort(([a], [b]) => (a < b ? -1 : 1))
        .map(([version, k]) => ({ version, created_at: k.created_at, fingerprint: k.fingerprint })),
      ...counts,
      last_ack: sub.last_ack || null,
    };
    if (include_secret) {
      const k = sub.keys[sub.current_key_version];
      view.signing_secret = k.secret; // returned ONCE on create/rotate
    }
    return view;
  }

  /** Operator read-only view: same operational status, never the signing key. */
  _operatorView(sub) {
    return this._adminView(sub);
  }

  listSubscriptions({ includeSecrets = false } = {}) {
    return [...this.subscriptions.values()]
      .sort((a, b) => (a.created_at < b.created_at ? -1 : 1))
      .map((s) => this._adminView(s, { include_secret: includeSecrets }));
  }

  getSubscription(id) {
    const sub = this._require(id);
    const view = this._adminView(sub);
    view.pending = this._rowsOf(id)
      .filter((r) => r.status !== 'acked')
      .slice(-50)
      .map((r) => ({
        delivery_id: r.delivery_id,
        event_id: r.event_id,
        cursor: r.cursor,
        key_version: r.key_version,
        status: r.status,
        attempts: r.attempts,
        not_before: r.not_before ? new Date(r.not_before).toISOString() : null,
        last_status: r.last_status,
        last_error: r.last_error,
        created_at: r.created_at,
      }));
    view.dead_letters = this._rowsOf(id)
      .filter((r) => r.status === 'dead')
      .map((r) => this.deadView(r));
    return view;
  }

  deadView(row) {
    const ev = this.events.get(row.event_id);
    return {
      delivery_id: row.delivery_id,
      subscription_id: row.subscription_id,
      event_id: row.event_id,
      object_key: ev?.object_key || null,
      kind: ev?.kind || null,
      cursor: row.cursor,
      key_version: row.key_version,
      attempts: row.attempts,
      dead_reason: row.last_error,
      last_status: row.last_status,
      created_at: row.created_at,
    };
  }

  listDeadLetters() {
    return [...this.deliveries.values()]
      .filter((r) => r.status === 'dead')
      .sort((a, b) => a.cursor - b.cursor)
      .map((r) => this.deadView(r));
  }

  // ---------------------------------------------------- inspector-safe proofs --

  /**
   * Desensitised notification proof. Carries the signed envelope (stable event
   * id, object key, kind, business cursor, key version, signature) and delivery
   * status, but never the callback URL, subscription name, signing secret or
   * dead-letter error text (which may quote the downstream response). No site
   * ids / request keys / image digests exist anywhere in an event by
   * construction.
   */
  proofForEvent(eventId, opts = {}) {
    const ev = this.events.get(eventId);
    if (!ev) throw new NotificationError(404, 'EVENT_NOT_FOUND', 'unknown event');
    return [...this.deliveries.values()]
      .filter((r) => r.event_id === eventId)
      .sort((a, b) => (a.subscription_id < b.subscription_id ? -1 : 1))
      .map((r) => this._proofRow(ev, r, opts));
  }

  listProofs({ objectKey = null, kind = null, limit = 200, includeDeliveryReason = false } = {}) {
    const out = [];
    for (const eventId of this.eventOrder) {
      const ev = this.events.get(eventId);
      if (objectKey && ev.object_key !== objectKey) continue;
      if (kind && ev.kind !== kind) continue;
      for (const r of this.deliveries.values()) {
        if (r.event_id !== eventId) continue;
        out.push(this._proofRow(ev, r, { includeDeliveryReason }));
        if (out.length >= limit) return out;
      }
    }
    return out;
  }

  _proofRow(ev, row, { includeDeliveryReason = false } = {}) {
    const sub = this.subscriptions.get(row.subscription_id);
    const envelope = this.buildEnvelope(ev, row, sub);
    const out = {
      event_id: ev.event_id,
      subscription_id: row.subscription_id,
      object_key: ev.object_key,
      kind: ev.kind,
      cursor: ev.cursor,
      cursor_name: ev.cursor_name,
      occurred_at: ev.occurred_at,
      detail: ev.detail,
      delivery_cursor: row.cursor,
      key_version: row.key_version,
      signature: envelope.signature,
      status: row.status,
      attempts: row.attempts,
      acked_at: row.acked_at || null,
    };
    // Dead-letter error text may quote the downstream response: admin console
    // only, never part of an inspector's desensitised proof.
    if (includeDeliveryReason) out.last_error = row.last_error;
    return out;
  }

  /** Server-side verification of a (possibly externally supplied) proof. */
  verifyProof(body) {
    const env = body && (body.envelope || body);
    if (!env || typeof env !== 'object') {
      throw new NotificationError(400, 'PROOF_MALFORMED', 'envelope must be an object');
    }
    const required = ['event_id', 'subscription_id', 'object_key', 'kind', 'cursor', 'key_version', 'signature'];
    for (const f of required) {
      if (!(f in env)) throw new NotificationError(422, 'PROOF_MALFORMED', `missing field ${f}`);
    }
    const sub = this.subscriptions.get(env.subscription_id);
    if (!sub) throw new NotificationError(422, 'PROOF_UNKNOWN_SUBSCRIPTION', 'subscription not found');
    const keyEntry = sub.keys[env.key_version];
    if (!keyEntry) throw new NotificationError(422, 'PROOF_KEY_UNKNOWN', 'key version is not (or no longer) known', { key_version: env.key_version });
    const { signature, ...content } = env;
    const expect = sign(keyEntry.secret, content);
    if (expect.length !== signature.length) {
      throw new NotificationError(422, 'PROOF_SIG_INVALID', 'notification signature is invalid');
    }
    const a = Buffer.from(expect);
    const b = Buffer.from(signature);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      throw new NotificationError(422, 'PROOF_SIG_INVALID', 'notification signature is invalid');
    }
    // The event must be one this server actually emitted.
    const known = this.events.get(env.event_id);
    if (!known) throw new NotificationError(422, 'PROOF_EVENT_UNKNOWN', 'event id is not in the durable event log');
    if (known.object_key !== env.object_key || known.kind !== env.kind || known.cursor !== env.cursor) {
      throw new NotificationError(422, 'PROOF_CONTENT_MISMATCH', 'envelope content differs from durable event');
    }
    return {
      ok: true,
      event_id: env.event_id,
      subscription_id: env.subscription_id,
      object_key: env.object_key,
      kind: env.kind,
      cursor: env.cursor,
      key_version: env.key_version,
      matched_key_fingerprint: keyEntry.fingerprint,
    };
  }

  // ----------------------------------------------------------------- sending --

  /** The exact JSON body + headers posted to the downstream callback. */
  buildEnvelope(ev, row, sub = this.subscriptions.get(row.subscription_id)) {
    const envelope = {
      event_id: ev.event_id,
      subscription_id: row.subscription_id,
      object_key: ev.object_key,
      kind: ev.kind,
      cursor: ev.cursor,
      cursor_name: ev.cursor_name,
      detail: ev.detail || {},
      occurred_at: ev.occurred_at,
      key_version: row.key_version,
      delivery_id: row.delivery_id,
      delivery_cursor: row.cursor,
    };
    const keyEntry = sub?.keys?.[row.key_version];
    const secret = keyEntry ? keyEntry.secret : '';
    return { ...envelope, signature: sign(secret, envelope) };
  }

  /**
   * One sender pass. HTTP I/O happens OUTSIDE the store mutex; a slow callback
   * can never block convergence submissions or history reads. State transitions
   * are short critical sections that only append WAL frames.
   */
  async tick() {
    if (this._stopped || this.dcfg.enabled === false || this._sending) return { sent: 0 };
    this._sending = true;
    let sent = 0;
    try {
      const due = this._pickDueRows();
      for (const item of due) {
        if (this._stopped) break;
        await this._deliver(item);
        sent++;
      }
    } finally {
      this._sending = false;
    }
    return { sent };
  }

  _pickDueRows() {
    const now = Date.now();
    const out = [];
    for (const sub of this.subscriptions.values()) {
      if (sub.status !== 'active') continue; // paused: backlog accumulates, nothing leaves
      const head = this._headRow(sub.id);
      if (!head) continue;
      if (!['pending', 'retry'].includes(head.status)) continue; // dead blocks the line
      if ((head.not_before || 0) > now) continue; // backing off
      head.status = 'sending'; // in-memory lease; not persisted (crash => safe redelivery)
      out.push({ sub, row: head, ev: this.events.get(head.event_id) });
    }
    return out;
  }

  /** Lowest-cursor unconfirmed row — the only row allowed on the wire. */
  _headRow(subId) {
    let head = null;
    for (const r of this.deliveries.values()) {
      if (r.subscription_id !== subId) continue;
      if (r.status === 'acked') continue;
      if (!head || r.cursor < head.cursor) head = r;
    }
    return head;
  }

  async _deliver({ sub, row, ev }) {
    const envelope = this.buildEnvelope(ev, row, sub);
    const body = JSON.stringify(envelope);
    const headers = {
      'content-type': 'application/json',
      'x-convergence-event': ev.event_id,
      'x-convergence-subscription': sub.id,
      'x-convergence-cursor': String(ev.cursor),
      'x-convergence-delivery-cursor': String(row.cursor),
      'x-convergence-key': row.key_version,
      'x-convergence-delivery': row.delivery_id,
      'x-convergence-attempt': String(row.attempts + 1),
      'x-convergence-signature': envelope.signature,
    };
    let outcome;
    try {
      outcome = await this._post(sub.callback_url, headers, body);
    } catch (e) {
      outcome = { ok: false, error: `${e.name || 'NetworkError'}: ${e.message}`, status: null, timedOut: e.name === 'AbortError' };
    }
    if (outcome.ok) {
      await this._persistAck(row, outcome.status || 200);
    } else {
      await this._persistFailure(row, outcome);
    }
  }

  _post(url, headers, body) {
    const timeoutMs = this.dcfg.timeoutMs || 3000;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error('delivery timeout')), timeoutMs);
    return (async () => {
      try {
        const res = await fetch(url, { method: 'POST', headers, body, signal: controller.signal });
        // Drain a small amount so the connection can free; the body is ignored.
        try { await res.arrayBuffer(); } catch { /* ignore */ }
        if (res.status >= 200 && res.status < 300) return { ok: true, status: res.status };
        return { ok: false, status: res.status, error: `non_success_status:${res.status}` };
      } finally {
        clearTimeout(timer);
      }
    })();
  }

  async _persistAck(row, status) {
    const attempts = row.attempts + 1;
    const at = nowIso();
    const doc = { action: 'ack', delivery_id: row.delivery_id, subscription_id: row.subscription_id, cursor: row.cursor, attempts, at, status };
    await this._commitFramesFromSender([{ type: T_DELIVERY, payload: JSON.stringify(doc) }]);
    this._applyDelivery(doc);
  }

  _backoffDelayMs(attempts) {
    const base = this.dcfg.backoffBaseMs ?? 200;
    const cap = this.dcfg.backoffCapMs ?? 30000;
    return Math.min(cap, base * 2 ** Math.max(0, attempts - 1));
  }

  async _persistFailure(row, outcome) {
    const attempts = row.attempts + 1;
    const max = this.dcfg.maxAttempts || 5;
    const at = nowIso();
    const dead = attempts >= max;
    const reason = outcome.timedOut
      ? `timeout_after_${this.dcfg.timeoutMs}ms`
      : outcome.error || `non_success_status:${outcome.status}`;
    const doc = dead
      ? { action: 'attempt', delivery_id: row.delivery_id, subscription_id: row.subscription_id, attempts, at, status: 'dead', last_status: outcome.status ?? null, error: reason, not_before: 0 }
      : { action: 'attempt', delivery_id: row.delivery_id, subscription_id: row.subscription_id, attempts, at, status: 'retry', last_status: outcome.status ?? null, error: reason, not_before: Date.now() + this._backoffDelayMs(attempts) };
    await this._commitFramesFromSender([{ type: T_DELIVERY, payload: JSON.stringify(doc) }]);
    this._applyDelivery(doc);
  }
}
