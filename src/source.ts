import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { readFile, statfs } from 'node:fs/promises';
import { dirname } from 'node:path';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

export type Resource = { year: number; category: string; path: string; title: string; source: string; url: string; size: number | null; expected?: Record<string, string> };
export type Inventory = { resources: Resource[]; gaps?: string[] };
export const digest = (...parts: unknown[]) => createHash('sha256').update(JSON.stringify(parts)).digest('hex');
export const turnOf = (resource: Resource) => Number(/(?:^|\/)turno-([12])(?:\/|$)/.exec(resource.path)?.[1]) || null;
const allowed = new Set(['dadosabertos.tse.jus.br', 'cdn.tse.jus.br', 'resultados.tse.jus.br']);
const scope = new Set(['arquivos-transmitidos', 'boletim-urna', 'resultados', 'historico-totalizacao', 'relatorio-totalizacao',
  'correspondencias', 'logs-gedai', 'resultado-ea20', 'acompanhamento', 'catalogos', 'comparecimento-abstencao']);
export const sidecarPattern = /\.(sha1|sha256|sha512)$/i;

export function selectSources(inventory: Inventory) {
  const seen = new Set<string>();
  return inventory.resources.filter(item => {
    if (![2018, 2022, 2026].includes(item.year) || !scope.has(item.category) || sidecarPattern.test(item.url)
        || /(?:^|\/)governador(?:\/|$)/i.test(item.path)
        || /votacao_partido_munzona/i.test(item.path) || seen.has(item.url)) return false;
    seen.add(item.url);
    return true;
  });
}

export class SourceClient {
  private next = 0;
  constructor(private readonly intervalMs = 1000) {}

  private async request(url: string) {
    let current = url;
    for (let redirects = 0; ; redirects++) {
      const parsed = new URL(current);
      if (parsed.protocol !== 'https:' || !allowed.has(parsed.host) || parsed.username || parsed.password) {
        throw new Error('URL fora das origens oficiais do TSE');
      }
      const slot = Math.max(Date.now(), this.next);
      this.next = slot + this.intervalMs;
      await delay(Math.max(0, slot - Date.now()));
      const response = await fetch(current, { redirect: 'manual', signal: AbortSignal.timeout(60 * 60_000),
        headers: { 'accept-encoding': 'identity', 'user-agent': 'TSE-Presidencial-Auditoria/1.0' } });
      if (![301, 302, 303, 307, 308].includes(response.status)) return { response, finalUrl: current };
      await response.body?.cancel();
      const destination = response.headers.get('location');
      if (!destination || redirects >= 4) throw new Error('Redirecionamento inválido');
      current = new URL(destination, current).href;
    }
  }

  private async withResponse<T>(url: string, consume: (response: Response, finalUrl: string) => Promise<T>) {
    for (let attempt = 0; ; attempt++) {
      try {
        const { response, finalUrl } = await this.request(url);
        if (response.status === 200 && response.body) {
          try { return await consume(response, finalUrl); }
          finally { if (!response.body.locked) await response.body.cancel().catch(() => {}); }
        }
        await response.body?.cancel();
        if (![408, 429, 500, 502, 503, 504].includes(response.status) || attempt >= 3) {
          throw new Error(`HTTP ${response.status} do TSE para ${url}`);
        }
        const retry = response.headers.get('retry-after') ?? '';
        const ms = /^\d+$/.test(retry) ? Number(retry) * 1000 : Date.parse(retry) - Date.now();
        this.next = Math.max(this.next, Date.now() + Math.max(2000 * 2 ** attempt, ms || 0));
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (attempt >= 3 || !(error instanceof TypeError || (error instanceof Error && error.name === 'TimeoutError')
            || ['ECONNRESET','ERR_STREAM_PREMATURE_CLOSE','ETIMEDOUT'].includes(code ?? ''))) throw error;
        await delay(2000 * 2 ** attempt);
      }
    }
  }

  async download(resource: Resource, target: string, maximum: number) {
    return this.withResponse(resource.url, async (response, finalUrl) => {
      const headers = Object.fromEntries(response.headers);
      const length = Number(headers['content-length'] ?? resource.size ?? 0);
      if (!Number.isSafeInteger(length) || length < 0 || length > maximum
          || (headers['content-encoding'] && headers['content-encoding'] !== 'identity')) {
        throw new Error('Tamanho ou codificação HTTP fora do limite');
      }
      const volume = await statfs(dirname(target));
      if (volume.bsize * volume.bavail < 2 ** 30 + length) throw new Error('Espaço insuficiente para a transferência');
      const hashes = { sha1: createHash('sha1'), sha256: createHash('sha256'), sha512: createHash('sha512') };
      let bytes = 0;
      await pipeline(Readable.fromWeb(response.body as Parameters<typeof Readable.fromWeb>[0]), async function* (chunks) {
        let nextCheck = 64 * 2 ** 20;
        for await (const chunk of chunks) {
          bytes += chunk.length;
          if (bytes > maximum || (length && bytes > length)) throw new Error('Limite de download atingido');
          if (bytes >= nextCheck) {
            const disk = await statfs(dirname(target));
            if (disk.bsize * disk.bavail < 2 ** 30 + chunk.length) throw new Error('Reserva de disco atingida durante a transferência');
            nextCheck = bytes + 64 * 2 ** 20;
          }
          for (const hash of Object.values(hashes)) hash.update(chunk);
          yield chunk;
        }
      }, createWriteStream(target));
      if (!bytes || (length && length !== bytes)) throw new Error('Resposta truncada');
      return { bytes, finalUrl, headers, collectedAt: new Date().toISOString(),
        hashes: { sha1: hashes.sha1.digest('hex'), sha256: hashes.sha256.digest('hex'), sha512: hashes.sha512.digest('hex') } };
    });
  }

  async small(url: string, target: string) {
    const metadata = await this.download({url, size: null} as Resource, target, 2 ** 20);
    return { metadata, text: await readFile(target, 'utf8') };
  }
}
