/**
 * Owner-level extraction on PGlite. The same scenarios run on PostgreSQL in
 * the `.pg` suite, which adds the cases that need parallel connections.
 */
import { randomUUID } from 'node:crypto';

import { PGlite } from '@electric-sql/pglite';
import { afterEach, describe, it, vi } from 'vitest';

import {
  bootExtractionHarness,
  cacheCollectorOrderScenario,
  claimIntoSameContentScenario,
  sameKeyConcurrentSourcesScenario,
  claimBudgetScenario,
  deletedSourceScenario,
  ensureStartedScenario,
  fallbackReuseScenario,
  heartbeatLossScenario,
  mediaDerivativeScenario,
  ownerCacheScenario,
  ownerClaimMidRunScenario,
  publishRollbackScenario,
  quotaFailureScenario,
  schemaCompatibilityScenario,
  supersededClaimScenario,
  twoConversationsOneExtractionScenario,
  withdrawnDonorScenario,
  type ExtractionHarness,
  type ExtractionScenarioPool,
} from './_owner-extraction-scenarios';

class PGlitePool implements ExtractionScenarioPool {
  constructor(readonly db: PGlite) {}

  async query<TRow>(text: string, params?: unknown[]) {
    return (await this.db.query(text, params)) as { rows: TRow[] };
  }

  async connect() {
    return {
      query: (text: string, params?: unknown[]) => this.db.query(text, params),
      release() {},
    };
  }

  async end() {}
}

describe('owner-level material extraction (PGlite)', () => {
  let db: PGlite | undefined;

  async function boot(env: Record<string, string> = {}): Promise<ExtractionHarness> {
    vi.stubEnv('ASSET_S3_BUCKET', '');
    for (const [name, value] of Object.entries(env)) vi.stubEnv(name, value);
    const databaseUrl = `postgres://owner-extraction-${randomUUID()}`;
    vi.stubEnv('DATABASE_URL', databaseUrl);
    db = new PGlite();
    await db.waitReady;
    return bootExtractionHarness(new PGlitePool(db), databaseUrl);
  }

  afterEach(async () => {
    vi.unstubAllEnvs();
    await db?.close();
    db = undefined;
  });

  it('starts idle and failed sources once and leaves pending, running and done ones alone', async () => {
    await ensureStartedScenario(await boot());
  });

  it('extracts a source two conversations start exactly once', async () => {
    await twoConversationsOneExtractionScenario(await boot());
  });

  it('spends the claim budget on takeovers and keeps a heartbeating lease', async () => {
    await claimBudgetScenario(await boot());
  });

  it('refuses every write of a superseded claim, across a manual restart too', async () => {
    await supersededClaimScenario(await boot());
  });

  it('never claims a deleted source and refuses a claim of one', async () => {
    await deletedSourceScenario(await boot());
  });

  it('publishes once for the account when the owner is claimed mid-run', async () => {
    await ownerClaimMidRunScenario(await boot());
  });

  it('rolls a failed publication back completely', async () => {
    await publishRollbackScenario(await boot());
  });

  it('reuses an owner s earlier result only when it still holds', async () => {
    await ownerCacheScenario(await boot());
  });

  it('stores media derivatives as owner materials of their own', async () => {
    await mediaDerivativeScenario(await boot());
  });

  it('fails a source whose outputs do not fit the asset quota, publishing nothing', async () => {
    await quotaFailureScenario(await boot({ ASSET_QUOTA_BYTES: '1000' }));
  });

  it('keeps the bootstrap rerunnable and the public view unchanged', async () => {
    await schemaCompatibilityScenario(await boot());
  });

  it('stops a run whose heartbeat finds the claim taken over before it allocates', async () => {
    await heartbeatLossScenario(await boot());
  });

  it('keeps a reused result through the donor s deletion and refuses a hit the collector took', async () => {
    await cacheCollectorOrderScenario(await boot());
  });

  it('lets two sources of the same bytes extract and publish their own results at once', async () => {
    await sameKeyConcurrentSourcesScenario(await boot());
  });

  it('keeps both results when a claim brings the same content into an account that has it', async () => {
    await claimIntoSameContentScenario(await boot());
  });

  it('reuses a document fallback s earlier result when the provider ahead of it fails', async () => {
    await fallbackReuseScenario(await boot());
  });

  it('does not reuse a done donor that no longer roots its result', async () => {
    await withdrawnDonorScenario(await boot());
  });
});
