import { spawn } from 'node:child_process';
import { once, EventEmitter } from 'node:events';
import { copyFile, link, mkdir, mkdtemp, open, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { basename, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { collectResource, type Stage } from './collector.js';
import { loadBigQuery, preflightBigQuery, verifyBigQueryMarker } from './bigquery.js';
import { digest, selectSources, SourceClient, turnOf, type Inventory, type Resource } from './source.js';

const tableNames = ['packages', 'files', 'records'] as const;
const smokeFile = 'CESP_2t_AC_241020261231.zip';

async function main() {
  const { positionals, values } = parseArgs({
    allowPositionals: true, options: {
      work: { type: 'string', default: './work' }, inventory: { type: 'string' }, resource: { type: 'string' },
      snapshot: { type: 'string' }, concurrency: { type: 'string', default: '2' }, project: { type: 'string' },
      dataset: { type: 'string' }, location: { type: 'string', default: 'us-central1' },
      'expires-hours': { type: 'string', default: '72' }, 'gzip-transport': { type: 'boolean' }, python: { type: 'string', default: 'python' },
    },
  });
  const [mode] = positionals;
  if (positionals.length !== 1 || !['inventory', 'plan', 'smoke', 'run'].includes(mode ?? '')) {
    throw new Error('Uso: npm run pipeline -- inventory|plan|smoke|run [--work PATH] [--inventory PATH]');
  }
  const work = resolve(values.work);
  const inventoryPath = resolve(values.inventory ?? join(work, '_meta', 'plano.json'));
  if (mode === 'inventory') {
    if (values.inventory) throw new Error('inventory grava em --work/_meta/plano.json');
    const script = fileURLToPath(new URL('../tools/inventario_tse.py', import.meta.url));
    const child = spawn(values.python, ['-X', 'utf8', script, '--root', work], { stdio: 'inherit' });
    const [code] = await once(child, 'close');
    if (code !== 0) throw new Error('Falha ao gerar inventário');
    return;
  }
  const inventory = JSON.parse(await readFile(inventoryPath, 'utf8')) as Inventory;
  let resources = selectSources(inventory);
  if (values.resource || mode === 'smoke') {
    const name = values.resource ?? smokeFile;
    resources = resources.filter(item => item.path === name || basename(item.path) === name);
    if (resources.length !== 1) throw new Error('Recurso precisa ser único no inventário');
  }
  const byYear: Record<string, Record<string, number>> = {};
  for (const item of resources) {
    const key = `${item.year}:T${turnOf(item) ?? 'ambos/catalogo'}`;
    byYear[key] ??= {};
    byYear[key][item.category] = (byYear[key][item.category] ?? 0) + 1;
  }
  if (mode === 'plan') {
    console.log(JSON.stringify({ resources: resources.length, byYear, gaps: inventory.gaps }, null, 2));
    return;
  }
  if (!resources.length) throw new Error('Nenhuma fonte selecionada');
  const concurrency = Number(values.concurrency);
  const expiresHours = Number(values['expires-hours']);
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 4) throw new Error('Concorrência deve ficar entre 1 e 4');
  if (!Number.isInteger(expiresHours) || expiresHours < 1 || expiresHours > 72) throw new Error('TTL deve ficar entre 1 e 72 horas');
  if (Boolean(values.project) !== Boolean(values.dataset)) throw new Error('Informe projeto e dataset em conjunto');
  if (mode === 'run' && (!values.project || !values.dataset)) throw new Error('run exige --project e --dataset com ADC');
  if (mode === 'run' && !values.snapshot) throw new Error('run exige --snapshot explícito para retomar a mesma captura');
  const snapshot = values.snapshot ?? 'smoke';
  if (!/^[\w-]{1,48}$/.test(snapshot)) throw new Error('Snapshot inválido');
  const cloud = values.project && values.dataset ? { project: values.project, dataset: values.dataset, location: values.location, expiresHours } : null;
  if (cloud) await preflightBigQuery(cloud);
  const root = join(work, 'snapshots', snapshot);
  await mkdir(root, { recursive: true });
  const lockPath = join(root, '.lock');
  const lock = await open(lockPath, 'wx');
  const events = new EventEmitter();
  events.on('stage', (event: Stage) => process.stdout.write(JSON.stringify(event) + '\n'));
  const emit = (event: Stage) => events.emit('stage', event);
  const client = new SourceClient();
  const maximum = mode === 'smoke' ? 350_000_000 : 64 * 2 ** 30;
  const bundles = bundleResources(resources);
  try {
    for (const bundle of bundles) {
      const target = cloud ? digest(cloud.project, cloud.dataset, cloud.location) : 'local';
      const checkpointId = digest('pipeline-v2', snapshot, target, bundle.map(item => item.url)).slice(0, 32);
      const checkpoint = join(root, `committed-${checkpointId}.json`);
      const previous = cloud ? await readFile(checkpoint, 'utf8').catch((err: NodeJS.ErrnoException) => {
        if (err.code === 'ENOENT') return null;
        throw err;
      }) : null;
      if (cloud && previous) {
        const receipt = JSON.parse(previous) as { marker: string; expiresAt: string };
        if (!receipt.expiresAt || Date.parse(receipt.expiresAt) <= Date.now()
          || !await verifyBigQueryMarker(cloud, receipt.marker)) {
          throw new Error('Lote ausente ou expirado. Use um novo --snapshot para coletar novamente.');
        }
        emit({ name: 'loaded', resource: checkpointId, detail: { checkpoint, reused: true } });
        continue;
      }
      let failed = false;
      const collected: Awaited<ReturnType<typeof collectResource>>[] = new Array(bundle.length);
      let next = 0;
      const workers = Array.from({ length: Math.min(concurrency, bundle.length) }, async () => {
        try {
          while (!failed && next < bundle.length) {
            const index = next++;
            collected[index] = await collectResource({ resource: bundle[index], resources: inventory.resources, client, root,
              limit: maximum, emit, snapshot });
          }
        } catch (error) { failed = true; throw error; }
      });
      const outcomes = await Promise.allSettled(workers);
      for (const result of outcomes) if (result.status === 'rejected') throw result.reason;
      if (!cloud) continue;
      const batchId = digest('bundle-v2', snapshot, collected.map(item => item.batchId));
      const batchDir = await mkdtemp(join(root, '.load-'));
      try {
        for (const table of tableNames) {
          let ordinal = 0;
          for (const item of collected) for (const shard of item.tables[table].shards) {
            const name = table + '.ndjson.' + String(++ordinal).padStart(6, '0') + '.gz';
            const from = join(item.directory, shard.name);
            const to = join(batchDir, name);
            await link(from, to).catch((err: NodeJS.ErrnoException) => {
              if (!['EPERM', 'EACCES', 'EXDEV', 'ENOTSUP', 'EINVAL', 'EOPNOTSUPP'].includes(err.code ?? '')) throw err;
              return copyFile(from, to);
            });
          }
          if (!ordinal) await writeFile(join(batchDir, table + '.ndjson'), '');
        }
        const receipt = await loadBigQuery({ ...cloud, directory: batchDir, batchId, gzipTransport: values['gzip-transport'] });
        const part = checkpoint + '.part';
        await writeFile(part, JSON.stringify({ batchId, target: cloud, loadedAt: new Date().toISOString(), ...receipt,
          resources: collected.map(({directory: _, ...manifest}) => manifest) }, null, 2) + '\n');
        await rename(part, checkpoint);
        emit({ name: 'loaded', resource: batchId, detail: { resources: collected.length, checkpoint } });
        for (const item of collected) await rm(item.directory, { recursive: true, force: true });
      } finally { await rm(batchDir, { recursive: true, force: true }); }
    }
  } finally {
    await lock.close();
    await rm(lockPath, { force: true });
  }
}

function bundleResources(items: Resource[]) {
  const groups: Resource[][] = [];
  let json: Resource[] = [];
  let small: Resource[] = [];
  for (const item of items) {
    if (/\.json$/i.test(item.path)) {
      json.push(item);
      if (json.length === 64) { groups.push(json); json = []; }
    } else if (['correspondencias', 'relatorio-totalizacao', 'historico-totalizacao'].includes(item.category)) {
      small.push(item);
      if (small.length === 8) { groups.push(small); small = []; }
    } else {
      groups.push([item]);
    }
  }
  if (json.length) groups.push(json);
  if (small.length) groups.push(small);
  // Large packages are loaded and released promptly.
  return groups;
}

main().catch(error => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
