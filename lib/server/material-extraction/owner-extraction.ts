/**
 * Owner-level extraction of library sources: claim, extract, allocate and
 * publish, on the state in `lib/persistence/owner-material-extraction.ts`.
 *
 * Not started anywhere. `instrumentation.ts` starts only the session runner
 * (`./runner.ts`); nothing in a route, a tool or that runner calls into this
 * module. It is wired up together with its readers in Phase 2 of RFC #1716.
 *
 * ## One run
 *
 * 1. Read the source's bytes (`readSource`; today the neutral material byte
 *    store by the row's object key) and choose the extractor.
 * 2. Before each extractor runs -- the preferred one, then each document
 *    fallback in turn -- look for the owner's own earlier result under that
 *    extractor's cache key and, when there is one, publish it for this source
 *    with derivatives of its own. A hit that no longer holds under lock is a
 *    miss.
 * 3. Otherwise extract, then allocate each kept file as a pending pool entry
 *    in its own transaction, then publish everything in one more.
 *
 * Every allocation and the publication check the claim first. A claim that
 * is no longer current when it allocates stores nothing; one superseded while
 * an allocation is under way can leave that pending entry behind, which it
 * can never publish and which expires like any unpublished allocation. What
 * this does not do: cancel a provider call (none of them takes a signal) or
 * bound how long one runs. A late result is refused when it tries to publish.
 *
 * Document extraction keeps the text only, as the session chain does: images
 * a document provider returns are not stored, and the text is kept as the
 * provider wrote it, including any image references in it. Media extraction
 * keeps its transcript and its images.
 */
import { createHash, randomUUID } from 'node:crypto';

import { AssetQuotaExceededError } from '@openmaic/storage';
import { AssetRootTargetError } from '@openmaic/storage/asset/pg';

import { assetPrincipalForOwner } from '@/lib/persistence/owner-assets';
import {
  OwnerExtractionClaimLostError,
  checkOwnerExtractionClaim,
  claimNextOwnerMaterialExtraction,
  findOwnerExtractionCacheHit,
  heartbeatOwnerMaterialExtraction,
  publishOwnerMaterialExtraction,
  settleOwnerMaterialExtractionFailure,
  type OwnerExtractionClaim,
  type OwnerExtractionDerivative,
  type OwnerExtractionPublication,
  type OwnerExtractionResult,
  type PublishOutcome,
} from '@/lib/persistence/owner-material-extraction';
import { forwardOwnerWrite } from '@/lib/persistence/owner-merges';
import type { ServerPersistenceProvider } from '@/lib/persistence/server-provider';
import { getMinerUBackend } from '@/lib/pdf/pdf-providers';
import { getMaterialByteStore } from '@/lib/server/materials/bytes';
import {
  resolveASRBaseUrl,
  resolveASRModel,
  resolvePDFBaseUrl,
  resolveServerASRProviderId,
} from '@/lib/server/provider-config';

import { isTransientExtractionError, MaterialExtractionError } from './errors';
import {
  decodeMediaAssetData,
  documentExtractionFailure,
  documentFailureLine,
  documentOutcome,
  extractWithDocumentProvider,
  planSourceExtraction,
  plannedExtractor,
  runSourceExtraction,
  type ExtractorRegistryDependencies,
  type SourceExtractionOutcome,
  type SourceExtractionPlan,
} from './extract';

/**
 * How long a claim's lease lasts without a heartbeat. Longer than the session
 * chain's 10 seconds because the built-in PDF parser runs on the event loop:
 * a 30.7 MB text PDF held it for 17.7 seconds in one measurement, during
 * which no heartbeat can be sent. That is one sample, not a bound, so this is
 * a mitigation, not a guarantee; the cost is a slower takeover after a crash.
 */
export const OWNER_EXTRACTION_LEASE_TTL_MS = 60_000;
export const OWNER_EXTRACTION_HEARTBEAT_MS = 5_000;

type Persistence = Pick<ServerPersistenceProvider, 'pool' | 'withTransaction' | 'assetStoreIn'>;

export interface OwnerExtractionDependencies extends ExtractorRegistryDependencies {
  persistence: Persistence;
  /** The source's bytes. Where they live changes when uploads move into the pool. */
  readSource?: (claim: OwnerExtractionClaim) => Promise<Buffer>;
  /** The settings besides the extractor version that change its output. */
  resultOptions?: (extractorId: string) => Record<string, string>;
  now?: () => number;
  createId?: () => string;
}

/** Shared between a run and its heartbeat: set once the claim is known to be lost. */
export interface OwnerExtractionRunState {
  lost: boolean;
}

export type OwnerExtractionRunOutcome = PublishOutcome | 'reused';

/**
 * A configured endpoint as part of a cache key: scheme, host and path only.
 * Credentials and query strings are dropped, so nothing secret is stored;
 * a value that does not parse as a URL is kept only as a digest.
 */
export function endpointIdentity(baseUrl: string | undefined): string {
  if (!baseUrl) return '';
  try {
    const url = new URL(baseUrl);
    return `${url.protocol}//${url.host}${url.pathname.replace(/\/+$/, '')}`;
  } catch {
    return `sha256:${createHash('sha256').update(baseUrl).digest('hex')}`;
  }
}

/**
 * The server settings that change an extractor's output without changing its
 * version: the endpoint it calls (a self-hosted or overridden service decides
 * the model behind it), the MinerU backend, and for local media the ASR
 * provider, model and endpoint. No credentials: a key changes who pays, not
 * what comes back.
 */
export function defaultResultOptions(extractorId: string): Record<string, string> {
  if (extractorId === 'local-ffmpeg') {
    const asrProvider = resolveServerASRProviderId() ?? '';
    return {
      asrProvider,
      asrModel: (asrProvider && resolveASRModel(asrProvider)) || '',
      asrEndpoint: asrProvider ? endpointIdentity(resolveASRBaseUrl(asrProvider)) : '',
    };
  }
  const endpoint = endpointIdentity(resolvePDFBaseUrl(extractorId));
  return {
    ...(endpoint ? { endpoint } : {}),
    ...(extractorId === 'mineru' ? { backend: getMinerUBackend() } : {}),
  };
}

/**
 * The owner-scoped cache key (RFC #1716 §3): content identity, the extractor
 * that runs and the settings that change its result. The owner is not in the
 * key; a lookup only ever searches the source's own owner. A source without a
 * recorded digest has no reliable content identity and gets no key.
 */
export function ownerExtractionCacheKey(
  sha256: string | null,
  extractor: { id: string; version: string },
  options: Record<string, string>,
): string | null {
  if (!sha256) return null;
  const settings = Object.entries(options).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return createHash('sha256')
    .update(JSON.stringify([sha256, `${extractor.id}@${extractor.version}`, settings]))
    .digest('hex');
}

function isQuotaRefusal(error: unknown): boolean {
  if (error instanceof AssetQuotaExceededError) return true;
  return (
    typeof error === 'object' &&
    error !== null &&
    (error as { code?: unknown }).code === 'ASSET_QUOTA_EXCEEDED'
  );
}

/**
 * Allocate one file as a pending pool entry for the claim's current owner,
 * in a transaction of its own: the owner fence first (forwarded, as every
 * background write is), then the claim check, then the put.
 */
async function allocate(
  persistence: Persistence,
  claim: OwnerExtractionClaim,
  state: OwnerExtractionRunState,
  bytes: Buffer,
  mime: string,
): Promise<string> {
  if (state.lost) throw new OwnerExtractionClaimLostError(claim.materialId);
  const part = new Uint8Array(bytes.buffer as ArrayBuffer, bytes.byteOffset, bytes.byteLength);
  try {
    return await persistence.withTransaction(async (tx) => {
      const ownerId = await forwardOwnerWrite(tx, claim.ownerId);
      await checkOwnerExtractionClaim(tx, claim, ownerId);
      return persistence
        .assetStoreIn(tx)
        .put(assetPrincipalForOwner(ownerId), new Blob([part], { type: mime }), {
          contentType: mime,
        });
    });
  } catch (error) {
    if (error instanceof OwnerExtractionClaimLostError) throw error;
    if (isQuotaRefusal(error)) {
      throw new MaterialExtractionError(
        'the asset store has no room for this extraction; free space and start it again',
        false,
        { cause: error },
      );
    }
    // The registry reports every other failure without its cause, so a
    // passing database fault cannot be told from a lasting one: retry it,
    // within the claim budget.
    throw new MaterialExtractionError('storing an extraction output failed', true, {
      cause: error,
    });
  }
}

function sha256Hex(bytes: Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}

async function defaultReadSource(claim: OwnerExtractionClaim): Promise<Buffer> {
  if (!claim.ossKey) throw new Error(`source material ${claim.materialId} has no stored bytes`);
  return getMaterialByteStore().get(claim.ossKey);
}

/** Run one claimed source to a publication, a refusal, or a thrown failure. */
export async function runClaimedOwnerExtraction(
  claim: OwnerExtractionClaim,
  dependencies: OwnerExtractionDependencies,
  state: OwnerExtractionRunState = { lost: false },
): Promise<OwnerExtractionRunOutcome> {
  const { persistence } = dependencies;
  const now = dependencies.now ?? Date.now;
  const createId = dependencies.createId ?? randomUUID;
  const resultOptions = dependencies.resultOptions ?? defaultResultOptions;
  const bytes = await (dependencies.readSource ?? defaultReadSource)(claim);
  const mime = claim.mime ?? 'application/octet-stream';
  const plan = await planSourceExtraction({ bytes, mime }, claim.originalName, dependencies);

  /** Publish the owner's earlier result of `extractor`, if one still holds. */
  const reuse = async (extractor: {
    id: string;
    version: string;
  }): Promise<OwnerExtractionRunOutcome | undefined> => {
    const key = ownerExtractionCacheKey(claim.sha256, extractor, resultOptions(extractor.id));
    if (!key || state.lost) return undefined;
    const hit = await findOwnerExtractionCacheHit(persistence.pool, claim.materialId, key);
    if (!hit) return undefined;
    const reused = await publishReused(persistence, claim, key, hit, createId, now());
    return reused === 'miss' ? undefined : reused;
  };

  const extracted = await extractOrReuse(plan, claim.originalName, reuse);
  if (extracted.kind === 'reused') return extracted.outcome;
  const outcome = extracted.outcome;

  if (state.lost) return 'not-authorized';
  const options = resultOptions(outcome.extractor.id);
  const textBytes = Buffer.from(outcome.text, 'utf8');
  const textAssetId = await allocate(persistence, claim, state, textBytes, 'text/markdown');
  const derivatives: OwnerExtractionDerivative[] = [];
  for (const image of outcome.images) {
    const imageBytes = decodeMediaAssetData(image.data);
    const assetId = await allocate(persistence, claim, state, imageBytes, image.mimeType);
    derivatives.push({
      id: createId(),
      kind: 'image',
      assetId,
      title: image.title,
      mime: image.mimeType,
      bytes: imageBytes.byteLength,
      sha256: sha256Hex(imageBytes),
      ...(image.pageNumber === undefined ? {} : { pageNumber: image.pageNumber }),
      ...(image.timeMs === undefined ? {} : { timeMs: image.timeMs }),
    });
  }
  if (state.lost) return 'not-authorized';
  try {
    return await publishOwnerMaterialExtraction(
      persistence.withTransaction,
      claim,
      {
        cacheKey: ownerExtractionCacheKey(claim.sha256, outcome.extractor, options),
        text: { assetId: textAssetId, chars: outcome.text.length },
        extractor: { ...outcome.extractor, options },
        stats: { ...outcome.stats },
        derivatives,
      },
      now(),
    );
  } catch (error) {
    // An entry allocated above is gone or under another owner: nothing was
    // published, and a fresh claim allocates again.
    if (error instanceof AssetRootTargetError) {
      throw new MaterialExtractionError('an extraction output is no longer stored', true, {
        cause: error,
      });
    }
    throw error;
  }
}

/**
 * Walk the plan's extractors in their order, trying the owner's earlier
 * result of each one before running it: a document fallback that once
 * succeeded is reused when the providers ahead of it fail, exactly as it
 * would have been run. The failure when every extractor fails is the one the
 * session chain reports.
 */
async function extractOrReuse(
  plan: SourceExtractionPlan,
  title: string | null,
  reuse: (extractor: {
    id: string;
    version: string;
  }) => Promise<OwnerExtractionRunOutcome | undefined>,
): Promise<
  | { kind: 'reused'; outcome: OwnerExtractionRunOutcome }
  | { kind: 'extracted'; outcome: SourceExtractionOutcome }
> {
  if (plan.kind === 'media') {
    const reused = await reuse(plannedExtractor(plan));
    if (reused) return { kind: 'reused', outcome: reused };
    return { kind: 'extracted', outcome: await runSourceExtraction(plan, title) };
  }
  const errors: string[] = [];
  const failures: unknown[] = [];
  for (const provider of plan.candidates) {
    const reused = await reuse({ id: provider.id, version: provider.version });
    if (reused) return { kind: 'reused', outcome: reused };
    try {
      const artifact = await extractWithDocumentProvider(provider, plan.input);
      return { kind: 'extracted', outcome: documentOutcome(artifact, provider) };
    } catch (error) {
      errors.push(documentFailureLine(provider, error));
      failures.push(error);
    }
  }
  throw documentExtractionFailure(errors, failures);
}

/**
 * Publish an earlier result of the same owner for this source: the same
 * pool entries, rooted again under this source and derivatives of its own.
 * `miss` when the earlier result no longer holds -- its source changed or was
 * deleted, a root it held is withdrawn, or an entry it named is gone -- so
 * the caller extracts instead.
 */
async function publishReused(
  persistence: Persistence,
  claim: OwnerExtractionClaim,
  cacheKey: string,
  hit: { materialId: string; result: OwnerExtractionResult },
  createId: () => string,
  now: number,
): Promise<OwnerExtractionRunOutcome | 'miss'> {
  const publication: OwnerExtractionPublication = {
    cacheKey,
    donor: {
      materialId: hit.materialId,
      revision: hit.result.revision,
      roots: [
        { rootId: hit.materialId, assetId: hit.result.text.assetId },
        ...hit.result.derivatives.map((derivative) => ({
          rootId: derivative.id,
          assetId: derivative.assetId,
        })),
      ],
    },
    text: hit.result.text,
    extractor: hit.result.extractor,
    stats: hit.result.stats,
    derivatives: hit.result.derivatives.map((derivative) => ({ ...derivative, id: createId() })),
  };
  try {
    const outcome = await publishOwnerMaterialExtraction(
      persistence.withTransaction,
      claim,
      publication,
      now,
    );
    if (outcome === 'donor-changed') return 'miss';
    return outcome === 'published' ? 'reused' : outcome;
  } catch (error) {
    if (error instanceof AssetRootTargetError) return 'miss';
    throw error;
  }
}

export interface OwnerExtractionWorkerOptions {
  leaseTtlMs?: number;
  heartbeatIntervalMs?: number;
}

/**
 * Claim and run one source. `false` when there was nothing to claim. A
 * failure is settled under the claim's token; a claim found lost is not
 * settled, so it can never settle the claim that replaced it.
 */
export async function runNextOwnerExtraction(
  dependencies: OwnerExtractionDependencies,
  options: OwnerExtractionWorkerOptions = {},
): Promise<boolean> {
  const now = dependencies.now ?? Date.now;
  const { pool } = dependencies.persistence;
  const claim = await claimNextOwnerMaterialExtraction(pool, {
    leaseTtlMs: options.leaseTtlMs ?? OWNER_EXTRACTION_LEASE_TTL_MS,
    now: now(),
    createToken: randomUUID,
  });
  if (!claim) return false;
  const state: OwnerExtractionRunState = { lost: false };
  const heartbeat = setInterval(() => {
    heartbeatOwnerMaterialExtraction(pool, claim, now()).then(
      (current) => {
        if (!current) state.lost = true;
      },
      (error) => {
        // A heartbeat that did not land renews nothing; the lease decides.
        console.warn('[owner-extraction] heartbeat failed', error);
      },
    );
  }, options.heartbeatIntervalMs ?? OWNER_EXTRACTION_HEARTBEAT_MS);
  try {
    await runClaimedOwnerExtraction(claim, dependencies, state);
  } catch (error) {
    if (!(error instanceof OwnerExtractionClaimLostError) && !state.lost) {
      await settleOwnerMaterialExtractionFailure(pool, claim, {
        reason: error instanceof Error ? error.message : String(error),
        retryable: isTransientExtractionError(error),
      });
    }
  } finally {
    clearInterval(heartbeat);
  }
  return true;
}
