import assert from 'node:assert/strict';
import test from 'node:test';
import { decode, decodeCsvStream } from '../src/decoders.js';

async function collect<T>(iterator: AsyncIterable<T>): Promise<T[]> {
  const rows: T[] = [];
  for await (const row of iterator) rows.push(row);
  return rows;
}

test('CSV cruza fronteiras de blocos e conserva ordinal, campos e zeros à esquerda', async () => {
  const raw = Buffer.from('\ufeff"CD_CARGO";"CÓDIGO";"MENSAGEM"\r\n"3";"0002";"ignorar"\r\n"01";"0001";"ação; ""citada""\r\noutra linha"');
  async function* chunks() {
    for (const byte of raw) yield Buffer.from([byte]);
  }
  const rows = await collect(decodeCsvStream({ name: 'resultado.csv', chunks: chunks(), year: 2026, category: 'resultados' }));
  assert.deepEqual(rows, [{ kind: 'resultado', ordinal: 2, payload: {
    CD_CARGO: '01', CÓDIGO: '0001', MENSAGEM: 'ação; "citada"\r\noutra linha',
  } }]);
});

test('CSV aplica CP1252 sem BOM e rejeita registro truncado', async () => {
  const input = { name: 'bu.csv', year: 2018, category: 'boletim-urna' };
  const rows = await collect(decode({ ...input, bytes: Buffer.from('CD_CARGO_PERGUNTA;NOME\r\n1;Ação', 'latin1') }));
  assert.deepEqual(rows[0].payload, { CD_CARGO_PERGUNTA: '1', NOME: 'Ação' });
  await assert.rejects(collect(decode({ ...input, bytes: Buffer.from('A;B\n1;"incompleto') })), /aspas não encerradas/);
});

test('logs de urna e GEDAI mantêm hora de origem, mensagem, autenticador e linhas não reconhecidas', async () => {
  const input = { year: 2026, category: 'arquivos-transmitidos' };
  const rows = await collect(decode({ ...input, name: 'logd.dat', bytes: Buffer.from(
    '25/10/2026 17:01:02 INFO 123 VOTA Evento AAAAAAAAAAAAAAAA\r\nlinha incomum\r\n') }));
  assert.equal(rows.length, 2);
  assert.equal(rows[0].payload.urna, '123');
  assert.equal(rows[0].payload.autenticador, 'AAAAAAAAAAAAAAAA');
  assert.equal(rows[1].payload.mensagem, 'linha incomum');
  const desktop = await collect(decode({ ...input, name: 'gedai-ue.log', bytes: Buffer.from(
    '25/10/2026 08:40:41\tINFO\tAbertura do GEDAI\tABCD0123456789FF\n') }));
  assert.equal(desktop[0].payload.urna, null);
  assert.equal(desktop[0].payload.aplicativo, 'gedai-ue');
  assert.equal(desktop[0].payload.mensagem, 'Abertura do GEDAI');
});

test('EA20 preserva objeto de origem e aplica o código de cargo do arquivo', async () => {
  const input = { year: 2026, category: 'resultado-ea20', bytes: Buffer.from('{"carg":[{"cd":"1"}],"n":"001"}') };
  assert.deepEqual(await collect(decode({ ...input, name: 'br-c0001-e006258-u.json' })), [
    { kind: 'ea20', ordinal: 1, payload: { carg: [{ cd: '1' }], n: '001' } },
  ]);
  assert.deepEqual(await collect(decode({ ...input, name: 'br-c0003-e006258-u.json' })), []);
});

test('BER rejeita entrada de BU truncada', async () => {
  await assert.rejects(collect(decode({
    name: 'o03221rr0300000040011-bu.dat', year: 2026, category: 'arquivos-transmitidos',
    bytes: Buffer.from([0x30, 0x04, 0x02, 0x01, 0x01]),
  })), /ASN.1: conteúdo truncado/);
});
