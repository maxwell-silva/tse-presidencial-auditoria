import { basename, extname } from 'node:path';
import { decodeUrnaBinary } from './urna-asn1.js';

export type DecodedRecord = { kind: string; ordinal: number; payload: Record<string, unknown> };
type FileIdentity = { name: string; year: number; category: string };
type CsvInput = FileIdentity & { chunks: AsyncIterable<Uint8Array>; encoding?: 'utf-8' | 'windows-1252' };

const MAX_CSV_FIELD = 4 * 1024 * 1024;
const MAX_CSV_COLUMNS = 4096;
const MAX_CSV_RECORD = 16 * 1024 * 1024;
const CARGO_COLUMNS = new Set(['CD_CARGO', 'CD_CARGO_PERGUNTA']);

async function* csvRecords(input: CsvInput): AsyncGenerator<string[]> {
  let state: 'start' | 'plain' | 'quoted' | 'afterQuote' = 'start';
  let row: string[] = [];
  let field = '';
  let recordLength = 0;
  let skipLf = false;
  let decoder: TextDecoder | undefined;
  let first = Buffer.alloc(0);

  function append(value: string) {
    recordLength += value.length;
    if (field.length + value.length > MAX_CSV_FIELD || recordLength > MAX_CSV_RECORD) {
      throw new Error('Campo ou registro CSV acima do limite de caracteres');
    }
    field += value;
  }

  function finishField() {
    if (row.length === MAX_CSV_COLUMNS) throw new Error('Registro CSV acima do limite de colunas');
    row.push(field);
    field = '';
    state = 'start';
  }

  function* parse(text: string): Generator<string[]> {
    for (const char of text) {
      if (skipLf) {
        skipLf = false;
        if (char === '\n') continue;
      }
      if (state === 'quoted') {
        if (char === '"') state = 'afterQuote';
        else append(char);
        continue;
      }
      if (state === 'afterQuote' && char === '"') {
        append(char);
        state = 'quoted';
        continue;
      }
      if (char === ';' || char === '\r' || char === '\n') {
        finishField();
        if (char !== ';') {
          yield row;
          row = [];
          recordLength = 0;
          skipLf = char === '\r';
        }
        continue;
      }
      if (state === 'afterQuote') throw new Error('Caractere após o fechamento de aspas em CSV');
      if (char === '"') {
        if (state !== 'start') throw new Error('Aspas em campo CSV não delimitado');
        state = 'quoted';
      } else {
        append(char);
        state = 'plain';
      }
    }
  }

  for await (const chunk of input.chunks) {
    if (!decoder) {
      const need = 3 - first.length;
      first = Buffer.concat([first, Buffer.from(chunk.subarray(0, need))]);
      if (first.length < 3) continue;
      const bom = first[0] === 0xef && first[1] === 0xbb && first[2] === 0xbf;
      decoder = new TextDecoder(bom ? 'utf-8' : input.encoding ?? 'windows-1252', { fatal: true });
      yield* parse(decoder.decode(first, { stream: true }));
      yield* parse(decoder.decode(chunk.subarray(need), { stream: true }));
      continue;
    }
    yield* parse(decoder.decode(chunk, { stream: true }));
  }
  decoder ??= new TextDecoder(input.encoding ?? 'windows-1252', { fatal: true });
  yield* parse(decoder.decode(first.length < 3 ? first : undefined));
  // Estado e buffers são alterados por parse() ao percorrer os caracteres.
  if ((state as string) === 'quoted') throw new Error('Campo CSV com aspas não encerradas');
  if (row.length || (state as string) !== 'start') {
    finishField();
    yield row;
  }
}

function recordKind(category: string) {
  if (category === 'boletim-urna') return 'bu_csv';
  if (category === 'resultado-ea20' || category === 'resultados' || category === 'historico-totalizacao') return 'resultado';
  if (category === 'correspondencias') return 'correspondencia';
  return 'csv';
}

export async function* decodeCsvStream(input: CsvInput): AsyncGenerator<DecodedRecord> {
  let headers: string[] | undefined;
  let cargoColumn = -1;
  let ordinal = 0;
  for await (const row of csvRecords(input)) {
    if (!headers) {
      if (row.some(column => !column.trim()) || new Set(row).size !== row.length) {
        throw new Error('Cabeçalho CSV contém nomes vazios ou repetidos');
      }
      headers = row;
      cargoColumn = row.findIndex(column => CARGO_COLUMNS.has(column.trim().toUpperCase()));
      continue;
    }
    ordinal++;
    if (row.length !== headers.length) throw new Error(`CSV registro ${ordinal}: ${row.length} campos, esperados ${headers.length}`);
    if (cargoColumn !== -1 && !/^0*1$/.test(row[cargoColumn].trim())) continue;
    yield { kind: recordKind(input.category), ordinal, payload: Object.fromEntries(headers.map((name, index) => [name, row[index]])) };
  }
  if (!headers) throw new Error('CSV sem cabeçalho');
}

function decodeLog(bytes: Buffer): string {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } catch {
    return new TextDecoder('windows-1252').decode(bytes);
  }
}

const EVENT = /^(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.*)$/u;
const DESKTOP_EVENT = /^(\d{2}\/\d{2}\/\d{4}|\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\t([^\t]+)\t(.*)$/u;
const AUTHENTICATOR = /^(.*?)[ \t]+([0-9a-fA-F]{16,})\s*$/u;

export async function* decode(input: FileIdentity & { bytes: Buffer }): AsyncGenerator<DecodedRecord> {
  const name = basename(input.name).toLowerCase();
  if (extname(name) === '.csv') {
    async function* chunks() { yield input.bytes; }
    yield* decodeCsvStream({ ...input, chunks: chunks() });
    return;
  }
  if (extname(name) === '.json') {
    const raw = new TextDecoder('utf-8', { fatal: true }).decode(input.bytes);
    const value: unknown = JSON.parse(raw);
    // Os formatos JSON de resultados têm estruturas e níveis diferentes. Um arquivo é uma observação.
    const object = value !== null && typeof value === 'object' && !Array.isArray(value)
      ? value as Record<string, unknown> : { value };
    const match = /-c(\d{4})-/u.exec(name);
    if (input.category === 'resultado-ea20' && match && match[1] !== '0001') return;
    yield { kind: input.category === 'resultado-ea20' ? 'ea20' : 'json', ordinal: 1, payload: object };
    return;
  }
  if (name === 'logd.dat' || /\.log$/u.test(name)) {
    const lines = decodeLog(input.bytes).split(/\r\n|\r|\n/u);
    if (lines.at(-1) === '') lines.pop();
    for (const [index, line] of lines.entries()) {
      const desktop = name === 'logd.dat' ? null : DESKTOP_EVENT.exec(line);
      const event = desktop ? null : EVENT.exec(line);
      const message = desktop?.[4] ?? event?.[6] ?? line;
      const auth = (event || desktop) && AUTHENTICATOR.exec(message);
      yield { kind: 'log', ordinal: index + 1, payload: {
        data: (desktop ?? event)?.[1] ?? null, hora: (desktop ?? event)?.[2] ?? null,
        nivel: (desktop ?? event)?.[3] ?? null, urna: event?.[4] ?? null,
        aplicativo: event?.[5] ?? (desktop ? name.slice(0, -4) : null),
        mensagem: auth?.[1] ?? message, autenticador: auth?.[2] ?? null,
        formato_reconhecido: Boolean(desktop || event),
      } };
    }
    return;
  }
  yield* decodeUrnaBinary({ ...input, name });
}
