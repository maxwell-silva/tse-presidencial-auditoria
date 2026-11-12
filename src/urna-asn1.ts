import type { DecodedRecord } from './decoders.js';

type Tlv = { tag: number; value: Buffer };
const INTEGER = 0x02;
const OCTETS = 0x04;
const ENUM = 0x0a;
const NUMERIC = 0x12;
const TEXT = 0x1b;
const SEQUENCE = 0x30;
const A0 = 0xa0;
const A1 = 0xa1;
const A2 = 0xa2;
const A3 = 0xa3;

// The published urn layouts use one-byte tags and definite-length BER. Unknown layouts fail closed.
function fields(bytes: Buffer): Tlv[] {
  let offset = 0;
  const result: Tlv[] = [];
  while (offset < bytes.length) {
    if (offset + 2 > bytes.length) throw new Error('ASN.1: cabeçalho TLV truncado');
    const tag = bytes[offset++];
    let length = bytes[offset++];
    if ((tag & 0x1f) === 0x1f) throw new Error('ASN.1: tag de múltiplos octetos');
    if (length >= 128) {
      const width = length & 127;
      if (width === 0 || width > 4 || offset + width > bytes.length) throw new Error('ASN.1: comprimento inválido');
      length = bytes.readUIntBE(offset, width);
      offset += width;
    }
    if (offset + length > bytes.length) throw new Error('ASN.1: conteúdo truncado');
    result.push({ tag, value: bytes.subarray(offset, offset + length) });
    offset += length;
  }
  return result;
}

function expect(field: Tlv | undefined, ...tags: number[]) {
  if (!field || !tags.includes(field.tag)) {
    throw new Error(`ASN.1: tag ${field?.tag?.toString(16) ?? 'ausente'}, esperada ${tags.map(tag => tag.toString(16)).join('/')}`);
  }
  return field.value;
}

const sequence = (field: Tlv | undefined, ...tags: number[]) => fields(expect(field, ...(tags.length ? tags : [SEQUENCE])));
const hex = (field: Tlv | undefined, tag = OCTETS) => expect(field, tag).toString('hex');
const text = (field: Tlv | undefined, tag = TEXT) => new TextDecoder('windows-1252').decode(expect(field, tag));

function unsigned(field: Tlv | undefined, tag = INTEGER): number {
  const value = expect(field, tag);
  if (!value.length || value.length > 6 || (value[0] & 0x80) !== 0) throw new Error('ASN.1: inteiro fora do domínio');
  return value.readUIntBE(0, value.length);
}

function root(bytes: Buffer) {
  const elements = fields(bytes);
  if (elements.length !== 1) throw new Error('ASN.1: raiz deve conter uma entidade');
  return sequence(elements[0]);
}

function header(field: Tlv) {
  const [date, election] = sequence(field);
  const identifiers: Record<number, string> = { 0x81: 'processo', 0x82: 'pleito', 0x83: 'eleicao' };
  const type = election && identifiers[election.tag];
  if (!type) throw new Error('ASN.1: identificador eleitoral desconhecido');
  return { data_geracao: text(date), tipo_identificador_eleitoral: type, identificador_eleitoral: unsigned(election, election.tag) };
}

function section(field: Tlv, tag = SEQUENCE) {
  const [area, local, secao] = sequence(field, tag);
  const [municipio, zona] = sequence(area);
  return { municipio: unsigned(municipio), zona: unsigned(zona), local: unsigned(local), secao: unsigned(secao) };
}

function urn(field: Tlv) {
  const [type, version, mapping, fileType, flash, reason] = sequence(field);
  const [identity, load] = sequence(mapping);
  const [number, loadFlash, ...other] = sequence(load);
  const loadCode = other.at(-1);
  const date = other.at(-2);
  const machine = other.length > 2 ? other[0] : undefined;
  return {
    tipo_urna: unsigned(type, ENUM), versao_votacao: text(version),
    tipo_arquivo: unsigned(fileType, ENUM), serie_flash_votacao_hex: hex(flash),
    identificacao_carga: identity.tag === A0 ? section(identity, A0) : {
      tag: identity.tag, conteudo_hex: identity.value.toString('hex'),
    },
    numero_urna: unsigned(number), serie_flash_carga_hex: hex(loadFlash),
    data_hora_carga: date?.tag === TEXT ? text(date) : null,
    codigo_carga: loadCode?.tag === TEXT ? text(loadCode) : null,
    gerador_midia_campos: machine?.tag === SEQUENCE ? sequence(machine).map(part => part.tag === TEXT ? text(part) : {
      tag: part.tag, conteudo_hex: part.value.toString('hex'),
    }) : null,
    motivo_utilizacao_sa: reason ? { tag: reason.tag, conteudo_hex: reason.value.toString('hex') } : null,
  };
}

function urnFileName(name: string) {
  const match = /^[a-z](\d{5})([a-z]{2})(\d{5})(\d{4})(\d{4})-/u.exec(name);
  if (match) return { uf: match[2].toUpperCase(), municipio: Number(match[3]), zona: Number(match[4]), secao: Number(match[5]) };
  const legacy = /^[a-z]\d{5}-(\d{5})(\d{4})(\d{4})\./u.exec(name);
  return legacy ? { uf: null, municipio: Number(legacy[1]), zona: Number(legacy[2]), secao: Number(legacy[3]) } : null;
}

function geography(name: string, identity: ReturnType<typeof section>) {
  const source = urnFileName(name);
  if (source && (['municipio', 'zona', 'secao'] as const).some(key => source[key] !== identity[key])) {
    throw new Error('ASN.1: identificação da seção diverge do nome do arquivo');
  }
  return { ...identity, uf: source?.uf ?? null };
}

function* decodeBu(bytes: Buffer, name: string, year: number): Generator<DecodedRecord> {
  const legacy = year === 2022;
  const envelope = root(bytes);
  const content = envelope.at(-1);
  if (unsigned(envelope.at(-2), ENUM) !== 1) throw new Error('ASN.1: envelope de BU cifrado ou de tipo diferente');
  const [cabecalho, fase, urna, secao, emissao, dados, ...remaining] = root(expect(content, OCTETS));
  const comparecimento = legacy ? null : unsigned(remaining.shift());
  const details = !legacy && remaining[0]?.tag === A1 ? remaining.shift() : undefined;
  const [semBiometria, biometria, biografia] = details ? sequence(details, A1).map(field => unsigned(field)) : [];
  const [abertura, encerramento, desligamento] = dados?.tag === A0 ? sequence(dados, A0) : [];
  const [junta, turma, origem] = dados?.tag === A1 ? sequence(dados, A1) : [];
  const shared = {
    ...header(cabecalho), ...geography(name, section(secao)), ...urn(urna), fase: unsigned(fase, ENUM),
    data_emissao: text(emissao), comparecimento,
    data_abertura: abertura ? text(abertura) : null, data_encerramento: encerramento ? text(encerramento) : null,
    data_desligamento_impresso: desligamento ? text(desligamento) : null,
    junta_apuradora: junta ? unsigned(junta) : null, turma_apuradora: turma ? unsigned(turma) : null,
    numero_urna_origem: origem ? unsigned(origem) : null,
    compareceram_sem_biometria: semBiometria ?? null, habilitados_por_biometria: biometria ?? null,
    habilitados_por_biografia: biografia ?? null,
    historico_codigos_carga: legacy ? null : sequence(remaining[1]).map(field => text(field)),
    historico_voto_impresso_hex: !legacy && remaining[2] ? hex(remaining[2], SEQUENCE) : null,
    integridade_entidade_hex: legacy ? hex(remaining[1]) : null,
  };
  let ordinal = 0;
  for (const election of sequence(remaining[0], legacy ? A3 : SEQUENCE)) {
    const [id, aptos, ...fields] = sequence(election);
    const [aptosSecao, aptosTte, voting, lastHash, signature] = legacy
      ? [null, null, fields[0], null, null] : fields;
    for (const group of sequence(voting)) {
      const [type, attendance, positions] = sequence(group);
      for (const position of sequence(positions)) {
        const [office, printOrder, totals] = sequence(position);
        if (office.tag !== 0x81 || unsigned(office, 0x81) !== 1) continue;
        for (const total of sequence(totals)) {
          const [voteType, count, ...rest] = sequence(total);
          const [party, candidate] = rest[0]?.tag === 0xa3 ? sequence(rest.shift(), 0xa3) : [];
          yield { kind: 'bu_asn1', ordinal: ++ordinal, payload: {
            ...shared, eleicao: unsigned(id), cargo: 1, tipo_cargo: unsigned(type, ENUM),
            aptos: unsigned(aptos), aptos_secao: aptosSecao ? unsigned(aptosSecao) : null,
            aptos_tte: aptosTte ? unsigned(aptosTte) : null,
            comparecimento_cargo: unsigned(attendance), ordem_impressao: unsigned(printOrder),
            tipo_voto: unsigned(voteType, 0x81), votos: unsigned(count, 0x82),
            partido: party ? unsigned(party) : null, votavel: candidate ? unsigned(candidate) : null,
            ordem_geracao_hash: legacy ? null : unsigned(rest[0]),
            hash_votavel_hex: hex(rest[legacy ? 0 : 1]),
            ultimo_hash_votos_hex: lastHash ? hex(lastHash) : null,
            assinatura_ultimo_hash_hex: signature ? hex(signature) : null,
          } };
        }
      }
    }
  }
}

function* decodeRdv(bytes: Buffer, name: string, year: number): Generator<DecodedRecord> {
  const legacy = year === 2022;
  const [cabecalho, urna, entity] = root(bytes);
  const [pleito, fase, secao, ...fields] = sequence(entity);
  const history = legacy ? null : fields[0];
  const elections = fields[legacy ? 0 : 1];
  const info = {
    ...header(cabecalho), ...urn(urna), ...geography(name, section(secao)),
    pleito: unsigned(pleito), fase: unsigned(fase, ENUM),
    historico_codigos_carga: history ? sequence(history).map(field => text(field)) : null,
  };
  let ordinal = 0;
  let headerWritten = false;
  for (const election of sequence(elections, legacy ? A0 : A1, legacy ? A1 : A2)) {
    const [id, ...rest] = sequence(election);
    const [type, origin] = elections.tag === (legacy ? A1 : A2) ? rest : [];
    for (const office of sequence(rest.at(-1))) {
      const [code, choices, votes] = sequence(office);
      if (code.tag !== 0x81 || unsigned(code, 0x81) !== 1) continue;
      if (!headerWritten) {
        yield { kind: 'rdv_cabecalho', ordinal: 1, payload: info };
        headerWritten = true;
      }
      for (const [index, vote] of sequence(votes).entries()) {
        const [voteType, typing] = sequence(vote);
        yield { kind: 'rdv', ordinal: ++ordinal, payload: {
          eleicao: unsigned(id), cargo: 1, escolhas: unsigned(choices),
          origem_cedula: origin ? unsigned(origin, ENUM) : null, tipo_cedula: type ? unsigned(type, ENUM) : null,
          posicao_no_cargo: index + 1, tipo_voto: unsigned(voteType, ENUM),
          digitacao: typing ? text(typing, NUMERIC) : null,
        } };
      }
    }
  }
}

export function* decodeUrnaBinary(input: { name: string; bytes: Buffer; year: number }): Generator<DecodedRecord> {
  if (/\.vsc(?:mr)?$/u.test(input.name)) {
    // Container signed bytes are queryable for independent validation. This pipeline never verifies the signature.
    yield { kind: 'assinatura', ordinal: 1, payload: { formato: 'vsc', conteudo_base64: input.bytes.toString('base64') } };
    return;
  }
  if (input.year !== 2022 && input.year !== 2026) return;
  if (/(?:-imgbu\.dat|\.imgbu)$/u.test(input.name)) {
    yield { kind: 'imagem_bu', ordinal: 1, payload: { formato: 'envelope_imagem_bu', conteudo_base64: input.bytes.toString('base64') } };
    return;
  }
  if (/(?:-bu(?:sa)?\.dat|\.bu)$/u.test(input.name)) yield* decodeBu(input.bytes, input.name, input.year);
  if (/(?:-rdv(?:red)?\.dat|\.rdv)$/u.test(input.name)) yield* decodeRdv(input.bytes, input.name, input.year);
}
