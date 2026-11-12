import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { join } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { once } from 'node:events';
import { createGzip } from 'node:zlib';

export type Shard = { name: string; bytes: number; sha256: string; rows: number };
export async function hashFile(path: string) {
  const sha = createHash('sha256');
  let bytes = 0;
  for await (const chunk of createReadStream(path)) { sha.update(chunk); bytes += chunk.length; }
  return { bytes, sha256: sha.digest('hex') };
}

export const safeJson = (value: unknown) => JSON.stringify(value, (_key, item) =>
  item?.type === 'Buffer' && Array.isArray(item.data) ? { base64: Buffer.from(item.data).toString('base64') } : item);

export class NdjsonWriter {
  private current?: { stream: ReturnType<typeof createGzip>; complete: Promise<void>; path: string; name: string; rows: number; bytes: number; pending: string[]; pendingBytes: number };
  count = 0;
  readonly shards: Shard[] = [];

  constructor(private readonly directory: string, private readonly kind: string, private readonly shardBytes = 128 * 2 ** 20) {}

  async put(value: object) {
    const row = safeJson(value) + '\n';
    const bytes = Buffer.byteLength(row);
    if (bytes > 16 * 2 ** 20) throw new Error('Linha NDJSON acima do limite local de 16 MiB');
    if (this.current && this.current.bytes + bytes > this.shardBytes) await this.close();
    if (!this.current) {
      const name = `${this.kind}.ndjson.${String(this.shards.length + 1).padStart(6, '0')}.gz`;
      const path = join(this.directory, name);
      const stream = createGzip({ level: 3 });
      const complete = pipeline(stream, createWriteStream(path, { flags: 'wx' }));
      void complete.catch(() => {});
      this.current = { stream, complete, path, name, rows: 0, bytes: 0, pending: [], pendingBytes: 0 };
    }
    const current = this.current;
    current.pending.push(row);
    current.pendingBytes += bytes;
    current.rows++;
    current.bytes += bytes;
    this.count++;
    if (current.pendingBytes >= 256 * 1024) await this.flush(current);
  }

  private async flush(current: NonNullable<NdjsonWriter['current']>) {
    if (!current.pending.length) return;
    const chunk = current.pending.join('');
    current.pending = [];
    current.pendingBytes = 0;
    if (!current.stream.write(chunk)) await once(current.stream, 'drain');
  }

  async close() {
    const current = this.current;
    if (!current) return;
    await this.flush(current);
    current.stream.end();
    await current.complete;
    this.shards.push({ name: current.name, rows: current.rows, ...await hashFile(current.path) });
    this.current = undefined;
  }

  async abort() {
    if (!this.current) return;
    this.current.stream.destroy();
    await this.current.complete.catch(() => {});
  }

  manifest() {
    return { rows: this.count, bytes: this.shards.reduce((sum, shard) => sum + shard.bytes, 0), shards: this.shards };
  }
}
