import path from 'node:path';

export const ROLES = ['operator', 'reviewer', 'inspector', 'admin'];

/**
 * Static role tokens. Override via environment for deployment; defaults exist so
 * the service and verification script work out of the box.
 */
export function loadConfig(env = process.env) {
  const dataDir = env.CONVERGE_DATA_DIR || path.join(process.cwd(), 'data');
  return {
    host: env.CONVERGE_HOST || '127.0.0.1',
    port: Number(env.CONVERGE_PORT || 8080),
    dataDir,
    walPath: path.join(dataDir, 'converge.wal'),
    receiptSecret: env.CONVERGE_RECEIPT_SECRET || 'dev-receipt-secret-change-me',
    // Quiet period before an object settles: coalesces concurrent reports into
    // one decision. Determinism of the result does not depend on this value.
    settleDelayMs: Number(env.CONVERGE_SETTLE_MS || 25),
    // Retention policy: an object must hold at least this many decisions in HOT
    // storage after a checkpoint. A checkpoint targeting an older boundary is
    // rejected (RETENTION_POLICY_REJECTS). 0 allows archiving the whole history.
    retainDecisions: Math.max(0, Number(env.CONVERGE_RETAIN_DECISIONS || 0)),
    // Injected crash points for recovery tests (no-op in normal operation):
    //   'after_segment'  process-exit-equivalent failure once the segment exists
    //   'after_stub'     failure once the checkpoint stub is durable, pre-trim
    crashPoint: env.CONVERGE_CRASH_POINT || null,
    // Downstream event subscription delivery tuning.
    delivery: {
      enabled: env.CONVERGE_DELIVERY_DISABLED !== '1',
      tickMs: Math.max(1, Number(env.CONVERGE_DELIVERY_TICK_MS || 100)),
      timeoutMs: Math.max(50, Number(env.CONVERGE_DELIVERY_TIMEOUT_MS || 3000)),
      maxAttempts: Math.max(1, Number(env.CONVERGE_DELIVERY_MAX_ATTEMPTS || 5)),
      // Base delay for the capped exponential backoff (delay = base*2^(n-1),
      // capped at capMs). Tests override the base to run without wall-clock waits.
      backoffBaseMs: Math.max(0, Number(env.CONVERGE_DELIVERY_BACKOFF_BASE_MS || 200)),
      backoffCapMs: Math.max(0, Number(env.CONVERGE_DELIVERY_BACKOFF_CAP_MS || 30000)),
    },
    tokens: {
      [env.OPERATOR_TOKEN || 'operator-token']: 'operator',
      [env.REVIEWER1_TOKEN || 'reviewer1-token']: 'reviewer',
      [env.REVIEWER2_TOKEN || 'reviewer2-token']: 'reviewer',
      [env.INSPECTOR_TOKEN || 'inspector-token']: 'inspector',
      [env.ADMIN_TOKEN || 'admin-token']: 'admin',
    },
    reviewerIdentities: {
      [env.REVIEWER1_TOKEN || 'reviewer1-token']: env.REVIEWER1_NAME || 'reviewer-01',
      [env.REVIEWER2_TOKEN || 'reviewer2-token']: env.REVIEWER2_NAME || 'reviewer-02',
    },
  };
}
