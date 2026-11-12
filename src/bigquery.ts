import { createHash } from 'node:crypto';
import { open, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { createGunzip } from 'node:zlib';
import { GoogleAuth } from 'google-auth-library';

const api = 'https://bigquery.googleapis.com';
const mediaType = 'application/octet-stream';
const uploadChunkBytes = 8 * 1024 * 1024;
const transient = new Set([408, 429, 500, 502, 503, 504]);

type Input = {
  project: string;
  dataset: string;
  location: string;
  directory: string;
  batchId: string;
  expiresHours: number;
  gzipTransport?: boolean;
};
type Target = Pick<Input, 'project' | 'dataset' | 'location' | 'expiresHours'>;

type Field = { name: string; type: string; mode: 'REQUIRED' | 'NULLABLE' };
type Kind = 'packages' | 'files' | 'records';
type Source = { path: string; bytes: number; contentBytes: number; rows: bigint; hash: string; modifiedNs: bigint; gzip: boolean };
type Inputs = { kind: Kind; sources: Source[]; rows: bigint; hash: string };
type TableReference = { projectId: string; datasetId: string; tableId: string };
type Table = {
  expirationTime: string;
  description?: string;
  schema?: { fields?: Field[] };
  numRows?: string;
};
type Job = {
  jobReference?: { jobId: string; location?: string };
  configuration?: { load?: { destinationTable?: TableReference; writeDisposition?: string } };
  status?: { state?: string; errorResult?: { reason?: string }; errors?: unknown[] };
  statistics?: { load?: { outputRows?: string; badRecords?: string } };
};
class LocalSourceChanged extends Error {}

const required = (name: string, type = 'STRING'): Field => ({ name, type, mode: 'REQUIRED' });
const identity = [required('batch_id'), required('package_id')];
const schemas: Record<Kind, Field[]> = {
  packages: [...identity, required('year', 'INTEGER'), { name: 'turn', type: 'INTEGER', mode: 'NULLABLE' }, required('category'),
    required('url'), required('source'), required('collected_at', 'TIMESTAMP'), required('sha256'),
    required('byte_length', 'INTEGER'), required('metadata_json', 'JSON')],
  files: [...identity, required('file_id'), { name: 'parent_file_id', type: 'STRING', mode: 'NULLABLE' },
    required('ordinal', 'INTEGER'), required('path'), required('byte_length', 'INTEGER'),
    required('sha256'), required('metadata_json', 'JSON')],
  records: [...identity, required('file_id'), required('kind'), required('ordinal', 'INTEGER'), required('payload', 'JSON')],
};

async function inspect(path: string, gzip: boolean): Promise<Source> {
  const handle = await open(path, 'r');
  const digest = createHash('sha256');
  let bytes = 0;
  let contentBytes = 0;
  let lines = 0;
  let lastByte = 0;
  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile() || (gzip && (before.size <= 0n || before.size >= 1024n ** 3n))) {
      throw new Error('Entrada inválida ou parte gzip acima de 1 GiB.');
    }
    const raw = handle.createReadStream({ autoClose: false });
    const checksum = new Transform({
      transform(data: Buffer, _encoding, callback) {
        digest.update(data);
        bytes += data.length;
        callback(null, data);
      },
    });
    const count = async (stream: AsyncIterable<Buffer>) => {
      for await (const data of stream) {
        contentBytes += data.length;
        lastByte = data[data.length - 1];
        for (let offset = data.indexOf(10); offset !== -1; offset = data.indexOf(10, offset + 1)) lines++;
      }
    };
    if (gzip) await pipeline(raw, checksum, createGunzip(), count);
    else await pipeline(raw, checksum, count);
    const after = await handle.stat({ bigint: true });
    if (before.mtimeNs !== after.mtimeNs || before.size !== BigInt(bytes)) {
      throw new Error('Arquivo local mudou durante a conferência.');
    }
    if (contentBytes > 0 && lastByte !== 10) lines++;
    return { path, gzip, bytes, contentBytes, rows: BigInt(lines), hash: digest.digest('hex'), modifiedNs: before.mtimeNs };
  } finally {
    await handle.close();
  }
}

async function discover(directory: string, filenames: string[], kind: Kind): Promise<Inputs> {
  const regular = kind + '.ndjson';
  const parts = filenames.filter(file => file.startsWith(regular + '.')).sort();
  if (parts.length && (filenames.includes(regular) || parts.some((part, i) =>
    part !== `${regular}.${String(i + 1).padStart(6, '0')}.gz`))) {
    throw new Error(`Partes de ${kind} duplicadas, mistas ou fora de sequência.`);
  }
  const sources: Source[] = [];
  for (const file of parts.length ? parts : [regular]) sources.push(await inspect(join(directory, file), parts.length > 0));
  const rows = sources.reduce((total, source) => total + source.rows, 0n);
  const hash = createHash('sha256').update(JSON.stringify(sources.map(({ hash }) => hash))).digest('hex');
  return { kind, sources, rows, hash };
}

class BigQueryApi {
  private auth = new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/bigquery'] });
  private client = this.auth.getClient();

  async request(url: string, init: RequestInit = {}) {
    const parsed = new URL(url);
    if (!['bigquery.googleapis.com', 'www.googleapis.com'].includes(parsed.host) || parsed.protocol !== 'https:'
      || parsed.username || parsed.password) throw new Error('Origem inválida para sessão de upload do BigQuery.');
    const tokenHeaders = await (await this.client).getRequestHeaders(url);
    const headers = new Headers(init.headers);
    tokenHeaders.forEach((value, name) => headers.set(name, value));
    return fetch(url, { ...init, headers, redirect: 'manual', signal: AbortSignal.timeout(120_000) });
  }

  async control(url: string, init: RequestInit = {}) {
    for (let attempt = 0; ; attempt++) {
      try {
        const response = await this.request(url, init);
        if (!transient.has(response.status) || attempt >= 4) return response;
        const backoff = retryAfter(response.headers);
        await response.body?.cancel();
        await sleep(Math.max(backoff, 1000 * 2 ** attempt));
      } catch (error) {
        if (attempt >= 4 || !isNetworkFailure(error)) throw error;
        await sleep(1000 * 2 ** attempt);
      }
    }
  }
}

function isNetworkFailure(error: unknown) {
  return error instanceof TypeError || (error instanceof Error && error.name === 'TimeoutError');
}

function retryAfter(headers: Headers) {
  const value = headers.get('retry-after');
  if (!value) return 0;
  const millis = /^\d+$/.test(value) ? Number(value) * 1000 : Date.parse(value) - Date.now();
  return Number.isFinite(millis) ? Math.max(0, millis) : 0;
}

function json(body: unknown): RequestInit {
  return { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) };
}

async function read<T>(response: Response, operation: string): Promise<T> {
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`BigQuery ${operation}: HTTP ${response.status}`);
  }
  return await response.json() as T;
}

function tableUrl(datasetUrl: string, tableId: string) {
  return datasetUrl + '/tables/' + tableId;
}

async function getTable(client: BigQueryApi, datasetUrl: string, tableId: string): Promise<Table | null> {
  const response = await client.control(tableUrl(datasetUrl, tableId));
  if (response.status === 404) {
    await response.body?.cancel();
    return null;
  }
  return read<Table>(response, 'consultar tabela');
}

async function createTable(client: BigQueryApi, datasetUrl: string, reference: TableReference,
  expirationTime: string, fields: Field[], description: string): Promise<Table> {
  const response = await client.control(datasetUrl + '/tables',
    json({ tableReference: reference, expirationTime, schema: { fields }, description }));
  if (response.status === 409) {
    await response.body?.cancel();
    const existing = await getTable(client, datasetUrl, reference.tableId);
    if (existing) return existing;
    throw new Error('Tabela retornou conflito e não pôde ser confirmada.');
  }
  return read<Table>(response, 'criar tabela');
}

function assertTable(table: Table, fields: Field[], description: string, expirationTime?: string) {
  const actual = table.schema?.fields?.map(({ name, type, mode }) => [name, type, mode ?? 'NULLABLE']);
  const expected = fields.map(({ name, type, mode }) => [name, type, mode]);
  if (table.description !== description || JSON.stringify(actual) !== JSON.stringify(expected)
    || !Number.isFinite(Number(table.expirationTime)) || Number(table.expirationTime) <= Date.now()
    || (expirationTime && table.expirationTime !== expirationTime)) {
    throw new Error('Tabela existente diverge do contrato ou expirou.');
  }
}

async function getJob(client: BigQueryApi, projectUrl: string, location: string, jobId: string): Promise<Job | null> {
  const response = await client.control(`${projectUrl}/jobs/${jobId}?location=${encodeURIComponent(location)}`);
  if (response.status === 404) {
    await response.body?.cancel();
    return null;
  }
  return read<Job>(response, 'consultar job');
}

function nextOffset(response: Response, bytes: number) {
  const range = response.headers.get('range');
  const match = range?.match(/^(?:bytes=)?0-(\d+)$/i);
  const offset = match ? Number(match[1]) + 1 : 0;
  if ((range && !match) || !Number.isSafeInteger(offset) || offset < 0 || offset > bytes) {
    throw new Error('BigQuery retornou intervalo de upload inválido.');
  }
  return offset;
}

async function upload(client: BigQueryApi, session: string, source: Source, gzipTransport: boolean): Promise<void> {
  const file = await open(source.path, 'r');
  const verifyLocal = async () => {
    const current = await file.stat({ bigint: true });
    if (current.size !== BigInt(source.bytes) || current.mtimeNs !== source.modifiedNs) {
      throw new LocalSourceChanged('Arquivo local mudou durante a carga.');
    }
  };
  try {
    await verifyLocal();
    const raw = file.createReadStream({ autoClose: false });
    const bytes = gzipTransport ? source.bytes : source.contentBytes;
    const type = gzipTransport ? 'application/gzip' : mediaType;
    const transfer = (stream: AsyncIterable<Buffer>) => sendParts(client, session, stream, bytes, type);
    if (source.gzip && !gzipTransport) await pipeline(raw, createGunzip(), transfer);
    else await pipeline(raw, transfer);
    await verifyLocal();
  } finally {
    await file.close();
  }
}

async function sendParts(client: BigQueryApi, session: string, stream: AsyncIterable<Buffer>, bytes: number, type: string) {
  const buffer = Buffer.allocUnsafe(Math.min(uploadChunkBytes, bytes));
  let offset = 0;
  let buffered = 0;
  let complete = false;
  for await (const data of stream) {
    if (offset + buffered + data.length > bytes) throw new LocalSourceChanged('Entrada expandiu durante a carga.');
    for (let start = 0; start < data.length;) {
      const copied = data.copy(buffer, buffered, start, start + buffer.length - buffered);
      start += copied;
      buffered += copied;
      if (buffered === buffer.length) {
        complete = await sendPart(client, session, buffer, offset, bytes, type);
        offset += buffered;
        buffered = 0;
      }
    }
  }
  if (offset + buffered !== bytes) throw new LocalSourceChanged('Entrada truncada durante a carga.');
  if (buffered) complete = await sendPart(client, session, buffer.subarray(0, buffered), offset, bytes, type);
  if (!complete) throw new Error('Upload sem confirmação final do BigQuery.');
}

async function sendPart(client: BigQueryApi, session: string, part: Buffer, start: number, bytes: number, type: string) {
  let offset = start;
  const end = start + part.length;
  let stalls = 0;
  let probe = false;
  while (true) {
    const statusProbe = probe || offset === end;
    let response: Response | undefined;
    try {
      response = await client.request(session, { method: 'PUT', headers: {
        'content-type': type,
        'content-length': String(statusProbe ? 0 : end - offset),
        'content-range': statusProbe ? `bytes */${bytes}` : `bytes ${offset}-${end - 1}/${bytes}`,
      }, body: statusProbe ? undefined : part.subarray(offset - start) as unknown as BodyInit });
    } catch (error) {
      if (!isNetworkFailure(error)) throw error;
    }
    if (response?.ok) {
      await response.body?.cancel();
      if (end !== bytes) throw new Error('BigQuery encerrou upload antes do último bloco.');
      return true;
    }
    if (response?.status === 308) {
      const next = nextOffset(response, bytes);
      await response.body?.cancel();
      if (next < offset || next > end) throw new Error('Confirmação de upload fora de ordem.');
      if (next === end && end < bytes) return false;
      stalls = next > offset ? 0 : stalls + 1;
      offset = next;
      probe = false;
    } else if (!response || transient.has(response.status)) {
      await response?.body?.cancel();
      probe = true;
      stalls++;
    } else {
      await response.body?.cancel();
      throw new Error(`BigQuery upload: HTTP ${response.status}`);
    }
    if (stalls >= 5) throw new Error('Upload sem progresso após novas tentativas.');
    if (stalls > 0) await sleep(Math.max(1000 * 2 ** (stalls - 1), response ? retryAfter(response.headers) : 0));
  }
}

async function loadSource(client: BigQueryApi, projectUrl: string, location: string, reference: TableReference,
  source: Source, jobId: string, writeDisposition: 'WRITE_APPEND' | 'WRITE_EMPTY', gzipTransport: boolean) {
  let job = await getJob(client, projectUrl, location, jobId);
  if (!job) {
    const start = json({ jobReference: { projectId: reference.projectId, location, jobId }, configuration: { load: {
      destinationTable: reference, sourceFormat: 'NEWLINE_DELIMITED_JSON', writeDisposition,
      createDisposition: 'CREATE_NEVER', maxBadRecords: 0, ignoreUnknownValues: false,
    } } });
    start.headers = { ...start.headers, 'x-upload-content-type': gzipTransport ? 'application/gzip' : mediaType,
      'x-upload-content-length': String(gzipTransport ? source.bytes : source.contentBytes) };
    const response = await client.control(api + '/upload/bigquery/v2/projects/' + reference.projectId + '/jobs?uploadType=resumable', start);
    if (response.status === 409) {
      await response.body?.cancel();
    } else {
      const session = response.headers.get('location');
      await readStart(response);
      if (!session) throw new Error('BigQuery não retornou uma sessão de upload.');
      try {
        await upload(client, session, source, gzipTransport);
      } catch (error) {
        // A resposta final pode ter se perdido depois de o job ser aceito.
        if (error instanceof LocalSourceChanged || !await getJob(client, projectUrl, location, jobId)) throw error;
      }
    }
  }
  for (let poll = 0; poll < 720; poll++) {
    job ??= await getJob(client, projectUrl, location, jobId);
    if (job?.status?.state === 'DONE') {
      const load = job.configuration?.load;
      const target = load?.destinationTable;
      const stats = job.statistics?.load;
      if (job.status.errorResult || (job.status.errors?.length ?? 0) > 0) {
        throw new Error(`Job ${jobId} falhou (${job.status.errorResult?.reason ?? 'erros de carga'}).`);
      }
      if (job.jobReference?.jobId !== jobId || target?.projectId !== reference.projectId
        || target?.datasetId !== reference.datasetId || target?.tableId !== reference.tableId
        || load?.writeDisposition !== writeDisposition
        || BigInt(stats?.outputRows ?? -1) !== source.rows || BigInt(stats?.badRecords ?? 0) !== 0n) {
        throw new Error(`Contrato, destino ou contagem do job ${jobId} divergente.`);
      }
      return;
    }
    await sleep(5000);
    job = null;
  }
  throw new Error(`Job ${jobId} ainda sem confirmação após uma hora. Reexecute o mesmo lote para retomar.`);
}

async function readStart(response: Response) {
  await response.body?.cancel();
  if (!response.ok) throw new Error(`BigQuery iniciar upload: HTTP ${response.status}`);
}

async function validateTarget(input: Target) {
  if (!/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(input.project) || !/^\w{1,1000}$/.test(input.dataset)
    || !/^[a-z0-9-]+$/i.test(input.location)
    || !Number.isInteger(input.expiresHours) || input.expiresHours < 1 || input.expiresHours > 72) {
    throw new Error('Informe projeto, dataset, local e validade de 1 a 72 horas.');
  }
  const client = new BigQueryApi();
  const projectUrl = api + '/bigquery/v2/projects/' + input.project;
  const datasetUrl = projectUrl + '/datasets/' + input.dataset;
  const dataset = await read<{ location: string }>(await client.control(datasetUrl), 'consultar dataset');
  if (dataset.location?.toLowerCase() !== input.location.toLowerCase()) throw new Error('Local do dataset divergente.');
  return { client, projectUrl, datasetUrl, location: dataset.location };
}

/** Confirma ADC e a existência/localização do dataset antes da coleta. */
export async function preflightBigQuery(input: Target): Promise<void> {
  await validateTarget(input);
}

/** Confirma que o marcador remoto da captura ainda existe e está vigente. */
export async function verifyBigQueryMarker(input: Target, markerFullyQualified: string): Promise<string | null> {
  const [project, dataset, tableId, extra] = markerFullyQualified.split('.');
  if (project !== input.project || dataset !== input.dataset || extra !== undefined
    || !/^committed_[0-9a-f]{64}$/.test(tableId ?? '')) throw new Error('Marcador BigQuery fora do destino do lote.');
  const { client, datasetUrl } = await validateTarget(input);
  const marker = await getTable(client, datasetUrl, tableId);
  if (!marker || Number(marker.expirationTime) <= Date.now()) return null;
  const description = marker.description;
  if (!description) throw new Error('Marcador BigQuery sem metadados.');
  const metadata = JSON.parse(description) as { version?: number; tables?: { name: string }[] };
  const suffix = tableId.slice('committed_'.length);
  const tables = Object.keys(schemas).map(kind => kind + '_' + suffix);
  if (metadata.version !== 1 || JSON.stringify(metadata.tables?.map(table => table.name)) !== JSON.stringify(tables)) {
    throw new Error('Marcador BigQuery não corresponde às tabelas do lote.');
  }
  assertTable(marker, [required('reserved')], description);
  return new Date(Number(marker.expirationTime)).toISOString();
}

/**
 * Usa ADC e jobs de carga NDJSON sem bucket. As três tabelas são isoladas por
 * lote e expiram juntas. Só consulte o lote quando committed_<digest> existir.
 */
export async function loadBigQuery(input: Input): Promise<{ expiresAt: string; marker: string }> {
  if (!/^[a-z0-9_-]{8,128}$/i.test(input.batchId)) throw new Error('ID do lote inválido.');
  const names = await readdir(input.directory);
  const inputs = await Promise.all((Object.keys(schemas) as Kind[]).map(kind => discover(input.directory, names, kind)));
  if (!inputs[0].rows || !inputs[1].rows) throw new Error('Lote sem metadados de pacote ou arquivo.');
  const { client, projectUrl, datasetUrl, location } = await validateTarget(input);
  const digest = createHash('sha256').update(JSON.stringify([input.project, input.dataset, input.batchId])).digest('hex');
  const table = (prefix: string): TableReference => ({
    projectId: input.project, datasetId: input.dataset, tableId: `${prefix}_${digest}`,
  });
  const marker = table('committed');
  const markerFields = [required('reserved')];
  const description = JSON.stringify({ version: 1, batch_id: input.batchId,
    tables: inputs.map(({ kind, sources, rows, hash }) => ({ name: table(kind).tableId,
      bytes: sources.reduce((sum, source) => sum + source.bytes, 0), parts: sources.length,
      rows: String(rows), parts_sha256_digest: hash })),
  });
  const existingMarker = await getTable(client, datasetUrl, marker.tableId);
  if (existingMarker) assertTable(existingMarker, markerFields, description);
  let expiry = existingMarker?.expirationTime ?? String(Date.now() + input.expiresHours * 3_600_000);

  for (const { kind, sources, rows, hash } of inputs) {
    const reference = table(kind);
    const fields = schemas[kind];
    const info = JSON.stringify({ version: 1, batch_id: input.batchId, parts_sha256_digest: hash });
    const existing = await getTable(client, datasetUrl, reference.tableId);
    if (existingMarker && !existing) throw new Error(`Tabela ausente no lote confirmado: ${reference.tableId}`);
    const result = existing ?? await createTable(client, datasetUrl, reference, expiry, fields, info);
    if (kind === 'packages' && !existingMarker) expiry = result.expirationTime;
    assertTable(result, fields, info, expiry);
    if (!existingMarker) {
      for (const [index, source] of sources.entries()) {
        if (!source.rows) continue;
        const gzipTransport = Boolean(input.gzipTransport && source.gzip);
        await loadSource(client, projectUrl, location, reference, source,
          `tse_${kind}_${digest}_${source.hash}_${expiry}_${index}`,
          sources.length > 1 ? 'WRITE_APPEND' : 'WRITE_EMPTY', gzipTransport);
      }
    }
    const confirmed = await getTable(client, datasetUrl, reference.tableId);
    if (!confirmed || BigInt(confirmed.numRows ?? 0) !== rows) {
      throw new Error(`Contagem da tabela ${reference.tableId} diverge da carga.`);
    }
  }
  if (!existingMarker) {
    const result = await createTable(client, datasetUrl, marker, expiry, markerFields, description);
    assertTable(result, markerFields, description, expiry);
  }
  const fullName = `${input.project}.${input.dataset}.${marker.tableId}`;
  console.log(`BigQuery confirmado: ${fullName}`);
  return { expiresAt: new Date(Number(expiry)).toISOString(), marker: fullName };
}
