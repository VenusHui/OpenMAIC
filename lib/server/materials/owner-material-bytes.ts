/**
 * Read an owner material's original bytes, pool first.
 *
 * A material in the asset pool (`asset_id`) is read from its entry, under the
 * owner's own partition; a material from before the pool is read from the
 * material byte store by its object key (`oss_key`). Both binding a material
 * to a conversation and owner-level extraction read through here.
 *
 * ## One re-read, under the owner's fence
 *
 * The caller's record can be older than the row in two ways that matter, and
 * one re-read of the row after a failed first read covers both. The re-read
 * and the pool read run in one short transaction that first takes the record
 * owner's write fence (`forwardOwnerWrite`, the shared identity lock): a claim
 * of that owner takes the lock exclusively, so the row's owner cannot change
 * between the re-read and the read of the entry.
 *
 * - **The backfill moved it.** The backfill deletes an old object only after
 *   the row's pool pointer has committed (`./migrate-to-pool.ts`), and nothing
 *   else deletes the object of a ready row. So when the object is gone, the
 *   re-read finds the pointer.
 * - **A claim moved it.** A claim moves the row and re-keys its pool entry to
 *   the account in one transaction, so an owner read before the claim names a
 *   partition the entry is no longer in. The re-read finds the row's owner now,
 *   and the entry under it, and the fence keeps a claim from moving both again
 *   before the read. (Owner extraction keeps working for the account after a
 *   claim, as the rest of its writes do. Claims never chain: the account a
 *   claim moves into is never itself claimed.)
 *
 * Both hold for the write paths this release has; this does not repair an
 * object that is missing or damaged for any other reason.
 */
import { getServerPersistenceProvider } from '@/lib/persistence/server-provider';
import { assetPrincipalForOwner } from '@/lib/persistence/owner-assets';
import { forwardOwnerWrite } from '@/lib/persistence/owner-merges';
import type { OwnerMaterialRecord } from '@/lib/persistence/owner-materials';
import { getMaterialByteStore } from '@/lib/server/materials/bytes';

/** No stored bytes could be read for the material. */
export class OwnerMaterialBytesUnavailableError extends Error {
  constructor(readonly materialId: string) {
    super(`material ${materialId} bytes are unavailable`);
    this.name = 'OwnerMaterialBytesUnavailableError';
  }
}

type OwnerMaterialLocation = Pick<OwnerMaterialRecord, 'id' | 'ownerId' | 'assetId' | 'ossKey'>;

async function readOnce(location: OwnerMaterialLocation): Promise<Buffer | null> {
  if (location.assetId) {
    const provider = await getServerPersistenceProvider(process.env.DATABASE_URL ?? '');
    const read = await provider.assetStore.resolve(
      assetPrincipalForOwner(location.ownerId),
      location.assetId,
    );
    return read ? Buffer.from(read.bytes) : null;
  }
  if (location.ossKey) {
    try {
      return await getMaterialByteStore().get(location.ossKey);
    } catch {
      return null;
    }
  }
  return null;
}

/**
 * Re-read the row and read its pool entry, both under the fence of the
 * record's owner: the row's owner cannot change between the two statements.
 * `null` when the row has no pool pointer or its entry has no bytes.
 */
async function rereadAndReadPool(record: OwnerMaterialLocation): Promise<Buffer | null> {
  const provider = await getServerPersistenceProvider(process.env.DATABASE_URL ?? '');
  return provider.withTransaction(async (tx) => {
    await forwardOwnerWrite(tx, record.ownerId);
    const found = await tx.query<{ owner_id: string; asset_id: string | null }>(
      'SELECT owner_id, asset_id FROM owner_material WHERE id = $1',
      [record.id],
    );
    const row = found.rows[0];
    if (!row?.asset_id) return null;
    const read = await provider
      .assetStoreIn(tx)
      .resolve(assetPrincipalForOwner(row.owner_id), row.asset_id);
    return read ? Buffer.from(read.bytes) : null;
  });
}

/**
 * The material's original bytes.
 *
 * @throws OwnerMaterialBytesUnavailableError when neither the record nor the
 *   row as it is now leads to stored bytes.
 */
export async function readOwnerMaterialBytes(record: OwnerMaterialLocation): Promise<Buffer> {
  const first = await readOnce(record);
  if (first) return first;
  const second = await rereadAndReadPool(record);
  if (second) return second;
  throw new OwnerMaterialBytesUnavailableError(record.id);
}
