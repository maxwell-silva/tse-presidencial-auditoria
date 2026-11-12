import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { gzipSync } from 'node:zlib';
import { GoogleAuth } from 'google-auth-library';
import { loadBigQuery, preflightBigQuery, verifyBigQueryMarker } from '../src/bigquery.js';

test('carga BigQuery confirma lotes completos, retoma sem duplicação e rejeita sessão externa', async context => {
  const dir = await mkdtemp(join(tmpdir(), 'tse-bq-test-'));
  try {
    await writeFile(join(dir, 'packages.ndjson'), '{"turn":null}\n');
    const chunkBytes = 8 * 1024 * 1024;
    const fileInput = '{"content":"' + 'a'.repeat(chunkBytes) + '"}\n';
    await writeFile(join(dir, 'files.ndjson'), fileInput);
    const recordLine = Buffer.from('{"payload":{}}\n');
    const recordPart = gzipSync(recordLine);
    await writeFile(join(dir, 'records.ndjson.000001.gz'), recordPart);
    await writeFile(join(dir, 'records.ndjson.000002.gz'), recordPart);
    context.mock.method(GoogleAuth.prototype, 'getClient', async () => ({
      getRequestHeaders: async () => new Headers({ authorization: 'Bearer mock-adc' }),
    }));

    type FakeTable = { tableReference: { tableId: string }; expirationTime: string; schema: {
      fields: { name: string; mode: string }[] }; numRows?: string };
    type FakeJob = {
      configuration: { load: { destinationTable: { tableId: string }; writeDisposition: string; sourceFormat: string } };
      jobReference: { jobId: string }; status?: { state: string }; statistics?: { load: { outputRows: string; badRecords: string } };
    };
    const tables = new Map<string, FakeTable>();
    const jobs = new Map<string, FakeJob>();
    const sessions = new Map<string, FakeJob>();
    let uploads = 0;
    let chunks = 0;
    let rejectSession = false;
    let gzipTransport = false;
    context.mock.method(globalThis, 'fetch', async (url: string, init?: RequestInit) => {
      assert.equal(new Headers(init?.headers).get('authorization'), 'Bearer mock-adc');
      assert.equal(init?.redirect, 'manual');
      const { host, pathname } = new URL(url);
      assert.equal(host, 'bigquery.googleapis.com');
      const method = init?.method ?? 'GET';
      const target = pathname.split('/').at(-1)!;
      if (pathname.startsWith('/sessions/')) {
        const job = sessions.get(target)!;
        assert.equal(method, 'PUT');
        const destination = job.configuration.load.destinationTable.tableId;
        const headers = new Headers(init?.headers);
        assert.equal(headers.get('content-length'), String((init?.body as Uint8Array).length));
        chunks++;
        if (destination.startsWith('files_')) {
          if (headers.get('content-range') === `bytes 0-${chunkBytes - 1}/${fileInput.length}`) {
            return new Response(null, { status: 308, headers: { range: `0-${chunkBytes - 1}` } });
          }
          assert.equal(headers.get('content-range'), `bytes ${chunkBytes}-${fileInput.length - 1}/${fileInput.length}`);
        }
        if (destination.startsWith('records_')) {
          assert.equal(headers.get('content-type'), gzipTransport ? 'application/gzip' : 'application/octet-stream');
          assert.deepEqual(init?.body, gzipTransport ? recordPart : recordLine);
        }
        const table = tables.get(destination)!;
        table.numRows = String(Number(table.numRows ?? 0) + 1);
        job.status = { state: 'DONE' };
        job.statistics = { load: { outputRows: '1', badRecords: '0' } };
        jobs.set(job.jobReference.jobId, job);
        uploads++;
        return Response.json(job);
      }
      if (pathname.startsWith('/upload/')) {
        const job = JSON.parse(init?.body as string) as FakeJob;
        assert.equal(job.configuration.load.writeDisposition,
          job.configuration.load.destinationTable.tableId.startsWith('records_') ? 'WRITE_APPEND' : 'WRITE_EMPTY');
        assert.equal(job.configuration.load.sourceFormat, 'NEWLINE_DELIMITED_JSON');
        const session = String(sessions.size);
        sessions.set(session, job);
        return new Response(null, { headers: {
          location: rejectSession ? 'https://host-invalido.example/sessions/1' : `https://bigquery.googleapis.com/sessions/${session}`,
        } });
      }
      if (pathname.includes('/jobs/')) return jobs.has(target) ? Response.json(jobs.get(target)) : new Response(null, { status: 404 });
      if (pathname.endsWith('/datasets/election')) return Response.json({ location: 'us-central1' });
      if (pathname.endsWith('/tables') && method === 'POST') {
        const table = JSON.parse(init?.body as string) as FakeTable;
        const id = table.tableReference.tableId;
        if (tables.has(id)) return new Response(null, { status: 409 });
        table.numRows = '0';
        if (id.startsWith('committed_')) {
          const suffix = id.slice('committed_'.length);
          for (const prefix of ['packages_', 'files_', 'records_']) assert.ok(tables.has(prefix + suffix));
        }
        if (id.startsWith('packages_')) assert.equal(table.schema.fields.find(field => field.name === 'turn')?.mode, 'NULLABLE');
        tables.set(id, table);
        return Response.json(table);
      }
      return tables.has(target) ? Response.json(tables.get(target)) : new Response(null, { status: 404 });
    });
    const input = { project: 'tse-test', dataset: 'election', location: 'us-central1', directory: dir,
      batchId: 'pilot-idempotent', expiresHours: 72 };
    await preflightBigQuery(input);
    assert.equal(tables.size, 0);
    await assert.rejects(preflightBigQuery({ ...input, location: 'US' }), /Local do dataset divergente/);
    const receipt = await loadBigQuery(input);
    assert.equal(await verifyBigQueryMarker(input, receipt.marker), receipt.expiresAt);
    assert.equal(await verifyBigQueryMarker(input, 'tse-test.election.committed_' + '0'.repeat(64)), null);
    await assert.rejects(verifyBigQueryMarker(input, 'different.election.committed_' + '0'.repeat(64)), /fora do destino/);
    assert.equal(uploads, 4);
    assert.equal(chunks, 5);
    assert.equal(tables.size, 4);
    assert.equal(new Set([...tables.values()].map(table => table.expirationTime)).size, 1);
    assert.deepEqual(await loadBigQuery(input), receipt);
    assert.equal(uploads, 4);
    assert.equal(chunks, 5);
    assert.equal(tables.size, 4);
    gzipTransport = true;
    const direct = await loadBigQuery({ ...input, batchId: 'gzip-transport', gzipTransport: true });
    assert.notEqual(direct.marker, receipt.marker);
    assert.equal(uploads, 8);
    rejectSession = true;
    await assert.rejects(loadBigQuery({ ...input, batchId: 'other-batch' }), /Origem inválida/);
    assert.equal(uploads, 8);
    assert.equal([...tables.keys()].filter(key => key.startsWith('committed_')).length, 2);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
