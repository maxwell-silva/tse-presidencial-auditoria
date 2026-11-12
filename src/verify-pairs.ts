import { createReadStream } from 'node:fs';
import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { createGunzip } from 'node:zlib';

type Payload = Record<string, unknown>;
const [directory] = process.argv.slice(2);
if (!directory || process.argv.length !== 3) throw new Error('Informe o diretório da captura com os shards de registros.');
const files = (await readdir(directory)).filter(name => /^records\.ndjson\.\d{6}\.gz$/.test(name)).sort();
if (!files.length) throw new Error('Nenhum shard de registros na captura.');
const bu = new Map<string, number>();
const rdv = new Map<string, number>();
const rdvHeaders = new Map<string, Payload>();
const rdvCounts = new Map<string, Map<number, number>>();
const key = (data: Payload, election: number) => {
  const ids = [data.municipio, data.zona, data.secao, election];
  if (ids.some(value => typeof value !== 'number' || !Number.isSafeInteger(value))) throw new Error('Chave eleitoral incompleta.');
  return ids.join('/');
};
const integer = (value: unknown) => {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Número inválido no registro.');
  return value;
};
function increment(map: Map<string, number>, key: string, count: number) {
  map.set(key, (map.get(key) ?? 0) + count);
}
for (const name of files) {
  const stream = createReadStream(join(directory, name));
  const gzip = createGunzip();
  stream.on('error', error => gzip.destroy(error));
  const lines = createInterface({ input: stream.pipe(gzip), crlfDelay: Infinity });
  for await (const line of lines) {
    const row = JSON.parse(line) as { kind: string; file_id: string; payload: Payload };
    if (row.kind === 'bu_asn1') {
      increment(bu, key(row.payload, integer(row.payload.eleicao)), integer(row.payload.votos));
    } else if (row.kind === 'rdv_cabecalho') {
      if (rdvHeaders.has(row.file_id)) throw new Error('Mais de um cabeçalho do mesmo RDV.');
      rdvHeaders.set(row.file_id, row.payload);
    } else if (row.kind === 'rdv') {
      const counts = rdvCounts.get(row.file_id) ?? new Map<number, number>();
      const election = integer(row.payload.eleicao);
      counts.set(election, (counts.get(election) ?? 0) + 1);
      rdvCounts.set(row.file_id, counts);
    }
  }
}
for (const [fileId, elections] of rdvCounts) {
  const header = rdvHeaders.get(fileId);
  if (!header) throw new Error('Votos de RDV sem cabeçalho.');
  for (const [election, total] of elections) increment(rdv, key(header, election), total);
}
const pairs = [...bu.keys()].filter(section => bu.get(section)! > 0 && rdv.has(section));
const differences = pairs.filter(section => bu.get(section) !== rdv.get(section))
  .map(section => ({ section, bu: bu.get(section), rdv: rdv.get(section) }));
const report = { buKeys: bu.size, rdvKeys: rdv.size, matchedPositiveKeys: pairs.length, differences: differences.length,
  buOnlyKeys: [...bu.keys()].filter(section => !rdv.has(section)).length,
  rdvOnlyKeys: [...rdv.keys()].filter(section => !bu.has(section)).length, differencesSample: differences.slice(0, 20) };
console.log(JSON.stringify(report, null, 2));
if (!pairs.length || differences.length) process.exitCode = 1;
