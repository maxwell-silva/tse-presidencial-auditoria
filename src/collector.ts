import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, mkdtemp, open, readFile, rename, rm, stat, statfs, writeFile } from 'node:fs/promises';
import { basename, extname, join } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { crc32 } from 'node:zlib';
import yauzl from 'yauzl';
import { NdjsonWriter, hashFile } from './ndjson.js';
import { decode, decodeCsvStream } from './decoders.js';
import { digest, sidecarPattern, SourceClient, turnOf, type Resource } from './source.js';

type Entry = yauzl.Entry & { fileNameRaw: Buffer; extraFieldRaw: Buffer; fileCommentRaw: Buffer };
type Zip = yauzl.ZipFile & {
  eachEntry(): AsyncIterable<Entry>;
  openReadStreamPromise(entry: Entry): Promise<Readable>;
  readLocalFileHeaderPromise(entry: Entry): Promise<unknown>;
};
export type Stage = { name: 'downloaded' | 'verified' | 'extracted' | 'committed' | 'loaded'; resource: string; detail?: object };
type Emit = (stage: Stage) => void;
const MiB = 2 ** 20;
function openZip(path: string): Promise<Zip> {
  return new Promise((ok, fail) => yauzl.open(path, { lazyEntries: true, autoClose: false, validateEntrySizes: true },
    (error, zip) => error ? fail(error) : ok(zip as Zip)));
}
function tracked(stream: Readable, declared: number, expectedCrc?: number) {
  const sha = createHash('sha256');
  let bytes = 0;
  let crc = 0;
  let complete = false;
  return {
    async *chunks() {
      for await (const data of stream) {
        const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
        bytes += chunk.length;
        if (bytes > declared) throw new Error('Entrada excede tamanho declarado');
        sha.update(chunk);
        if (expectedCrc !== undefined) crc = crc32(chunk, crc);
        yield chunk;
      }
      if (bytes !== declared || (expectedCrc !== undefined && crc !== expectedCrc)) throw new Error('Tamanho ou CRC de entrada divergente');
      complete = true;
    },
    finish() {
      if (!complete) throw new Error('Entrada processada apenas parcialmente');
      return { bytes, sha256: sha.digest('hex') };
    },
  };
}

export async function collectResource(options: {
  resource: Resource; resources: Resource[]; client: SourceClient; root: string; limit: number; emit: Emit; snapshot: string;
}) {
  const { resource, resources, client, root, limit, emit, snapshot } = options;
  await mkdir(root, { recursive: true });
  const key = digest('v2', snapshot, resource.url).slice(0, 32);
  const final = join(root, key);
  const existing = await readFile(join(final, 'manifest.json'), 'utf8').catch((err: NodeJS.ErrnoException) => {
    if (err.code === 'ENOENT') return null;
    throw err;
  });
  if (existing) {
    const manifest = JSON.parse(existing) as Awaited<ReturnType<typeof run>>;
    if (manifest.url !== resource.url || manifest.snapshot !== snapshot) throw new Error('Manifesto incompatível');
    for (const name of ['packages', 'files', 'records'] as const) {
      for (const shard of manifest.tables[name].shards) {
        if (!new RegExp(`^${name}\\.ndjson\\.\\d{6}\\.gz$`).test(shard.name)) throw new Error('Shard inválido no manifesto');
        const actual = await hashFile(join(final, shard.name));
        if (actual.sha256 !== shard.sha256 || actual.bytes !== shard.bytes) throw new Error('NDJSON alterado');
      }
    }
    return { directory: final, ...manifest };
  }

  const work = await mkdtemp(join(root, '.partial-'));
  const input = join(work, 'source.part');
  const streams = ['packages', 'files', 'records'].map(name => new NdjsonWriter(work, name));
  async function run() {
    const [packages, files, records] = streams;
    const download = await client.download(resource, input, limit);
    emit({ name: 'downloaded', resource: resource.path, detail: { bytes: download.bytes } });
    const sidecars = resources.filter(item => sidecarPattern.test(item.url) && item.url.replace(sidecarPattern, '') === resource.url);
    const checksums = [];
    for (const item of sidecars) {
      const algorithm = item.url.match(sidecarPattern)![1].toLowerCase() as keyof typeof download.hashes;
      const sidecarPath = join(work, 'checksum.part');
      const sidecar = await client.small(item.url, sidecarPath);
      await rm(sidecarPath);
      const found = sidecar.text.toLowerCase().match(new RegExp(`\\b[a-f0-9]{${download.hashes[algorithm].length}}\\b`, 'g')) ?? [];
      if (found.length !== 1 || found[0] !== download.hashes[algorithm]) throw new Error(`Checksum TSE divergente: ${item.url}`);
      checksums.push({ algorithm, value: found[0], transfer: sidecar.metadata });
    }
    const ckanId = /^https:\/\/dadosabertos\.tse\.jus\.br\/dataset\/[^/]+\/resource\/([0-9a-f-]{36})$/.exec(resource.source)?.[1];
    let catalog = null;
    if (ckanId) {
      const catalogPath = join(work, 'catalog.part');
      const response = await client.small(`https://dadosabertos.tse.jus.br/api/3/action/resource_show?id=${ckanId}`, catalogPath);
      await rm(catalogPath);
      const body = JSON.parse(response.text);
      if (!body.success || body.result?.id !== ckanId || body.result?.url !== resource.url) throw new Error('Catálogo e inventário divergem');
      catalog = { resource: body.result, transfer: response.metadata };
    }
    emit({ name: 'verified', resource: resource.path, detail: { checksums: checksums.length } });
    const packageId = digest(resource.url, download.hashes.sha256);
    const batchId = digest('v2', snapshot, packageId);
    const common = { batch_id: batchId, package_id: packageId };
    const coverage: Record<string, number> = {};
    let number = 0;
    let accounted = 0;
    async function ensureDisk() {
      const volume = await statfs(work);
      if (volume.bavail * volume.bsize < 2 ** 30 + 64 * MiB) throw new Error('Espaço insuficiente para a extração');
    }
    async function walk(path: string, parent: string | null, depth: number) {
      if (depth > 8) throw new Error('Limite de aninhamento ZIP atingido');
      const zip = await openZip(path);
      let ordinal = 0;
      try {
        for await (const entry of zip.eachEntry()) {
          const local = await zip.readLocalFileHeaderPromise(entry);
          await item(entry.fileName, parent, ++ordinal, entry.uncompressedSize,
            await zip.openReadStreamPromise(entry), depth, { local, central: entry, container_comment: zip.comment }, entry.crc32);
        }
        if (zip.entryCount !== ordinal) throw new Error('Diretório ZIP incompleto');
        return { count: ordinal, comment: zip.comment };
      } finally { zip.close(); }
    }
    async function item(name: string, parent: string | null, ordinal: number, declared: number, stream: Readable,
      depth: number, zipMetadata: object | null, crc?: number) {
      const fileId = digest(packageId, parent, ordinal, name);
      if (++number % 128 === 0) await ensureDisk();
      accounted += declared;
      if (!Number.isSafeInteger(accounted) || accounted > 16 * limit) throw new Error('Limite total de expansão excedido');
      const source = tracked(stream, declared, crc);
      const chunks = source.chunks();
      const ext = extname(name).toLowerCase();
      const isContainer = /\.(zip|jez|logjez)$/i.test(name);
      let container = null;
      let status = 'metadados';
      let found = 0;
      const record = async (row: { kind: string; ordinal: number; payload: Record<string, unknown> }) => {
        await records.put({ ...common, file_id: fileId, ...row });
        coverage[row.kind] = (coverage[row.kind] ?? 0) + 1;
        if (++found % 16384 === 0) await ensureDisk();
      };
      try {
        if (isContainer) {
          await ensureDisk();
          const volume = await statfs(work);
          if (volume.bavail * volume.bsize < 2 ** 30 + declared) throw new Error('Espaço insuficiente para arquivo aninhado');
          const nested = join(work, fileId + '.part');
          try {
            await pipeline(Readable.from(chunks), createWriteStream(nested, { flags: 'wx' }));
            const file = await open(nested, 'r');
            const header = Buffer.alloc(4);
            try { await file.read(header); } finally { await file.close(); }
            if (header.subarray(0, 2).toString() === 'PK') {
              container = await walk(nested, fileId, depth + 1);
              status = 'container';
            } else {
              status = 'container_sem_layout';
            }
          } finally { await rm(nested, { force: true }); }
        } else if (ext === '.csv') {
          for await (const row of decodeCsvStream({ name, chunks, year: resource.year, category: resource.category })) await record(row);
          status = 'csv_processado';
        } else if (declared <= 32 * MiB) {
          const parts = [];
          for await (const chunk of chunks) parts.push(chunk);
          const bytes = Buffer.concat(parts);
          if (!zipMetadata && /^\s*(?:<!doctype html|<html)/i.test(bytes.subarray(0, 256).toString())) throw new Error('HTML recebido no lugar do arquivo');
          if (resource.expected && ext === '.json') {
            const value = JSON.parse(bytes.toString('utf8'));
            for (const [field, expected] of Object.entries(resource.expected)) {
              const ok = field === 'cargo'
                ? value.carg?.some((cargo: {cd: string}) => String(cargo.cd) === expected)
                : String(value[field] ?? '').toLowerCase() === expected.toLowerCase();
              if (!ok) throw new Error(`Identidade divergente: ${field}`);
            }
          }
          if (/\.(vsc|vscmr|sig|jws)$/i.test(name)) {
            for (let offset = 0; offset < bytes.length; offset += MiB) {
              await record({ kind: 'assinatura_bytes', ordinal: offset / MiB + 1,
                payload: { base64: bytes.subarray(offset, offset + MiB).toString('base64'), offset } });
            }
            status = 'assinatura_preservada_sem_validacao';
          } else {
            for await (const row of decode({ name, bytes, year: resource.year, category: resource.category })) await record(row);
            status = found ? 'extraido' : 'metadados_sem_registros';
          }
        } else {
          for await (const _chunk of chunks) { /* valida/hash sem materializar */ }
          status = 'metadados_limite_decoder';
        }
        const result = source.finish();
        await files.put({ ...common, file_id: fileId, parent_file_id: parent, ordinal, path: name,
          byte_length: result.bytes, sha256: result.sha256,
          metadata_json: { zip: zipMetadata, container, status, records: found, year: resource.year,
            turn: turnOf(resource), category: resource.category } });
        coverage[status] = (coverage[status] ?? 0) + 1;
      } finally { stream.destroy(); }
    }
    let outer = null;
    if (/\.zip$/i.test(new URL(resource.url).pathname)) outer = await walk(input, null, 0);
    else await item(basename(resource.path), null, 1, download.bytes, createReadStream(input), 0, null);
    emit({ name: 'extracted', resource: resource.path, detail: { files: files.count, records: records.count } });
    await packages.put({ ...common, year: resource.year, turn: turnOf(resource), category: resource.category,
      url: resource.url, source: resource.source, collected_at: download.collectedAt, sha256: download.hashes.sha256,
      byte_length: download.bytes, metadata_json: { download, checksums, catalog, outer, coverage, inventory_path: resource.path } });
    await Promise.all(streams.map(writer => writer.close()));
    const tables = { packages: packages.manifest(), files: files.manifest(), records: records.manifest() };
    return { schema: 2, url: resource.url, snapshot, batchId, packageId, tables, coverage };
  }
  try {
    const manifest = await run();
    await rm(input);
    await writeFile(join(work, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n', { flag: 'wx' });
    await rename(work, final);
    emit({ name: 'committed', resource: resource.path });
    return { directory: final, ...manifest };
  } finally {
    await Promise.all(streams.map(writer => writer.abort()));
    if (await stat(work).then(() => true, () => false)) await rm(work, { recursive: true, force: true });
  }
}
