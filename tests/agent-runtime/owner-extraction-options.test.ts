/**
 * The settings the owner-level extraction cache key is built from, read from
 * the real server provider configuration: an extractor whose configured
 * endpoint, model or backend changes must get a different key, and nothing
 * secret may end up in it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// Keep a host machine's server-providers.yml out of the configuration.
vi.mock('fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('fs')>();
  const isYaml = (p: unknown) => typeof p === 'string' && p.endsWith('server-providers.yml');
  const existsSync = (p: string) => (isYaml(p) ? false : actual.existsSync(p));
  return { ...actual, default: { ...actual, existsSync }, existsSync };
});

const SHA = 'a'.repeat(64);

async function keyFor(extractorId: string, env: Record<string, string>) {
  vi.resetModules();
  for (const [name, value] of Object.entries(env)) vi.stubEnv(name, value);
  const { defaultResultOptions, ownerExtractionCacheKey } =
    await import('@/lib/server/material-extraction/owner-extraction');
  const options = defaultResultOptions(extractorId);
  return { options, key: ownerExtractionCacheKey(SHA, { id: extractorId, version: '1' }, options) };
}

describe('owner extraction cache key settings', () => {
  beforeEach(() => {
    for (const name of [
      'ASR_FUNASR_BASE_URL',
      'ASR_FUNASR_MODELS',
      'PDF_MINERU_BASE_URL',
      'PDF_MINERU_BACKEND',
    ]) {
      vi.stubEnv(name, '');
    }
  });

  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it('keys local media on the ASR endpoint, not only its provider and model', async () => {
    const a = await keyFor('local-ffmpeg', {
      ASR_FUNASR_BASE_URL: 'http://asr-a.internal:8000/v1/',
      ASR_FUNASR_MODELS: 'transcriber',
    });
    const b = await keyFor('local-ffmpeg', {
      ASR_FUNASR_BASE_URL: 'http://asr-b.internal:8000/v1',
      ASR_FUNASR_MODELS: 'transcriber',
    });
    expect(a.options).toEqual({
      asrProvider: 'funasr-asr',
      asrModel: 'transcriber',
      asrEndpoint: 'http://asr-a.internal:8000/v1',
    });
    expect(b.options.asrEndpoint).toBe('http://asr-b.internal:8000/v1');
    expect(a.key).not.toBe(b.key);

    const otherModel = await keyFor('local-ffmpeg', {
      ASR_FUNASR_BASE_URL: 'http://asr-a.internal:8000/v1',
      ASR_FUNASR_MODELS: 'transcriber-large',
    });
    expect(otherModel.key).not.toBe(a.key);
  });

  it('keys self-hosted MinerU on its endpoint and backend', async () => {
    const pipeline = await keyFor('mineru', { PDF_MINERU_BASE_URL: 'http://mineru-a:8000' });
    const vlm = await keyFor('mineru', {
      PDF_MINERU_BASE_URL: 'http://mineru-a:8000',
      PDF_MINERU_BACKEND: 'vlm',
    });
    const elsewhere = await keyFor('mineru', { PDF_MINERU_BASE_URL: 'http://mineru-b:8000' });
    expect(pipeline.options).toEqual({ endpoint: 'http://mineru-a:8000', backend: 'pipeline' });
    expect(new Set([pipeline.key, vlm.key, elsewhere.key]).size).toBe(3);
  });

  it('keeps credentials and query strings out of the endpoint', async () => {
    const { options } = await keyFor('mineru', {
      PDF_MINERU_BASE_URL: 'https://operator:s3cret@mineru.example/api?token=abc#x',
    });
    expect(options.endpoint).toBe('https://mineru.example/api');
    expect(JSON.stringify(options)).not.toMatch(/s3cret|token=abc|operator/);
  });

  it('gives an extractor without configured settings an empty set', async () => {
    expect((await keyFor('unpdf', {})).options).toEqual({});
  });
});
