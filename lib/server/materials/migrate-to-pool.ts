/**
 * Move owner materials uploaded before the asset pool into it.
 *
 * A ready source from before the pool records its original as an object in the
 * material byte store (`oss_key`) and has no pool pointer. One bounded pass
 * walks those rows in id order and, for each:
 *
 * 1. reads the object and checks it against the digest recorded at upload;
 * 2. allocates a pending pool entry for it under the owner's own partition;
 * 3. publishes the pointer and the `('material', id)` root in one transaction
 *    (`withMaterialRoots`), only if the row has no pointer yet -- a second
 *    migrator of the same row gives up, and its allocation expires;
 * 4. re-reads the row, and only when the pointer is there, committed, deletes
 *    the object and clears `oss_key`.
 *
 * Step 4 decides on the database's committed state, never on whether a
 * publication call returned: a publication whose outcome is uncertain deletes
 * nothing this pass, and the next pass sees what actually committed. A row
 * whose pointer committed but whose object outlived a failed delete (or a
 * crash) still has its `oss_key`, so the next pass picks it up again and goes
 * straight to step 4; the delete is idempotent.
 *
 * The pass never reads a row twice: the cursor only moves forward, and a row
 * that fails is counted and left for the next pass. Several instances may run
 * it at once: the row lock and the empty-pointer check settle each row.
 *
 * It runs only when an operator turns it on (`MATERIALS_POOL_BACKFILL=1`, see
 * `instrumentation.ts`), after every instance runs this release: deleting an
 * original while an older instance can still start would put it beyond that
 * instance's reach, and an older bootstrap drops the pointers. Not running it
 * is harmless: readers fall back to `oss_key`.
 *
 * Existing session copies are never touched; they are the sessions' own.
 */
import { createHash } from 'node:crypto';

import type { BinaryBlob } from '@openmaic/dsl';
import { AssetQuotaExceededError } from '@openmaic/storage';

import { withMaterialRoots } from '@/lib/persistence/material-roots';
import { assetPrincipalForOwner } from '@/lib/persistence/owner-assets';
import { forwardOwnerWrite } from '@/lib/persistence/owner-merges';
import { getServerPersistenceProvider } from '@/lib/persistence/server-provider';
import { getMaterialByteStore } from '@/lib/server/materials/bytes';

export interface OwnerMaterialMigrationReport {
  /** Candidate rows looked at. */
  scanned: number;
  /** Rows whose pool pointer this pass published. */
  migrated: number;
  /** Old objects deleted, the row's pointer committed. */
  oldBytesRemoved: number;
  /** The old object could not be read. */
  skippedMissing: number;
  /** No recorded digest, or the object does not match it. */
  skippedDigest: number;
  /** The owner's pool quota has no room for it. */
  skippedQuota: number;
  /** The row changed before publication: deleted, moved or already pointing. */
  skippedLost: number;
  /** Any other failure; the row is left for the next pass. */
  failed: number;
}

export interface OwnerMaterialMigrationOptions {
  batchSize?: number;
  /** Pause between batches, so a pass stays out of the way of requests. */
  pauseMs?: number;
}

interface CandidateRow extends Record<string, unknown> {
  id: string;
  owner_id: string;
  mime: string | null;
  sha256: string | null;
  oss_key: string;
  asset_id: string | null;
}

const DEFAULT_BATCH_SIZE = 20;
const DEFAULT_PAUSE_MS = 1_000;

function emptyReport(): OwnerMaterialMigrationReport {
  return {
    scanned: 0,
    migrated: 0,
    oldBytesRemoved: 0,
    skippedMissing: 0,
    skippedDigest: 0,
    skippedQuota: 0,
    skippedLost: 0,
    failed: 0,
  };
}

/** Run one bounded pass over every pre-pool source; see the module docstring. */
export async function migrateOwnerMaterialsToPool(
  options: OwnerMaterialMigrationOptions = {},
): Promise<OwnerMaterialMigrationReport> {
  const batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
  const pauseMs = options.pauseMs ?? DEFAULT_PAUSE_MS;
  const provider = await getServerPersistenceProvider(process.env.DATABASE_URL ?? '');
  const byteStore = getMaterialByteStore();
  const report = emptyReport();

  /** Steps 1-3 for a row with no pointer. `true` when this pass published one. */
  const publish = async (row: CandidateRow): Promise<boolean> => {
    let bytes: Buffer;
    try {
      bytes = await byteStore.get(row.oss_key);
    } catch {
      report.skippedMissing += 1;
      return false;
    }
    if (!row.sha256 || createHash('sha256').update(bytes).digest('hex') !== row.sha256) {
      report.skippedDigest += 1;
      return false;
    }
    const mime = row.mime ?? 'application/octet-stream';
    const part = new Uint8Array(bytes.buffer as ArrayBuffer, bytes.byteOffset, bytes.byteLength);
    const blob: BinaryBlob = new Blob([part], { type: mime });
    let assetId: string;
    try {
      // Background work: a claim of the owner since the row was listed moves
      // the allocation to the account, where the row is now.
      assetId = await provider.withTransaction(async (tx) => {
        const ownerId = await forwardOwnerWrite(tx, row.owner_id);
        return provider
          .assetStoreIn(tx)
          .put(assetPrincipalForOwner(ownerId), blob, { contentType: mime });
      });
    } catch (error) {
      if (error instanceof AssetQuotaExceededError) {
        report.skippedQuota += 1;
        return false;
      }
      throw error;
    }
    const published = await withMaterialRoots(
      provider,
      { ownerId: row.owner_id, fence: 'background', materialIds: [row.id] },
      async ({ tx, ownerId, changeRoots }) => {
        const current = await tx.query<{
          owner_id: string;
          status: string;
          deleted_at: unknown;
          asset_id: string | null;
        }>('SELECT owner_id, status, deleted_at, asset_id FROM owner_material WHERE id = $1', [
          row.id,
        ]);
        const locked = current.rows[0];
        if (
          !locked ||
          locked.owner_id !== ownerId ||
          locked.status !== 'ready' ||
          locked.deleted_at !== null ||
          locked.asset_id !== null
        ) {
          return false;
        }
        await changeRoots({ add: [{ materialId: row.id, assetIds: [assetId] }] });
        await tx.query('UPDATE owner_material SET asset_id = $2 WHERE id = $1', [row.id, assetId]);
        return true;
      },
    );
    if (published) report.migrated += 1;
    else report.skippedLost += 1;
    return published;
  };

  /** Step 4: delete the old object only once the committed row points into the pool. */
  const removeOldBytes = async (id: string): Promise<void> => {
    const committed = await provider.pool.query<{ asset_id: string | null; oss_key: string }>(
      'SELECT asset_id, oss_key FROM owner_material WHERE id = $1',
      [id],
    );
    const row = committed.rows[0];
    if (!row?.asset_id || !row.oss_key) return;
    await byteStore.delete(row.oss_key);
    const cleared = await provider.pool.query(
      `UPDATE owner_material SET oss_key = ''
        WHERE id = $1 AND asset_id IS NOT NULL AND oss_key = $2
        RETURNING id`,
      [id, row.oss_key],
    );
    if (cleared.rows.length > 0) report.oldBytesRemoved += 1;
  };

  let cursor = '';
  for (;;) {
    const batch = await provider.pool.query<CandidateRow>(
      `SELECT id, owner_id, mime, sha256, oss_key, asset_id
         FROM owner_material
        WHERE kind = 'source' AND status = 'ready' AND deleted_at IS NULL
          AND oss_key <> '' AND id > $1
        ORDER BY id
        LIMIT $2`,
      [cursor, batchSize],
    );
    if (batch.rows.length === 0) break;
    for (const row of batch.rows) {
      cursor = row.id;
      report.scanned += 1;
      try {
        // A row that already points into the pool goes straight to step 4.
        if (row.asset_id === null && !(await publish(row))) continue;
        await removeOldBytes(row.id);
      } catch (error) {
        report.failed += 1;
        console.warn(`[material-backfill] material ${row.id} left for the next pass`, error);
      }
    }
    console.info('[material-backfill] batch done', { ...report });
    if (batch.rows.length < batchSize) break;
    if (pauseMs > 0) await new Promise((resolve) => setTimeout(resolve, pauseMs));
  }
  return report;
}
