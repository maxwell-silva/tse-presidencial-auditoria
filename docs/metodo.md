# Método de coleta e transformação

## Escopo e proveniência

- Presidente, pleitos de 2018, 2022 e 2026, turnos 1 e 2. Pacotes de urna, seções e correspondências são compartilhados por cargo. O escopo de metadados abrange todas as entradas observadas nesses pacotes. Linhas com coluna de cargo em dados tabulares e votos com cargo no RDV são filtrados para Presidente quando o formato permite. Logs são de eventos de seção.
- O inventário parte do catálogo público CKAN e da configuração EA11, mais EA12 para municípios. Inclui resultados e BU por seção, correspondências, comparecimento, pacotes de urna de 2022 e 2026, GEDAI quando publicado, EA16 e resultados EA20 de Presidente de 2026. O EA20 histórico, logs/RDV brutos de 2018 e alguns arquivos avulsos ficam como lacunas quando a fonte pública não os oferece. Os ZIPs de transmissão de 2026 listam seções na situação “Totalizado”. Arquivos por legenda são excluídos do conjunto presidencial.
- A seleção é uma lista de recursos por URL, com trilha para o catálogo ou para a configuração oficial. Famílias de resultado podem ter granularidade ou conteúdo sobreposto. Antes de somar, escolha uma família e uma versão. Registre as exceções e compare com as outras famílias por chave, incluindo exterior quando aplicável.
- Fontes oficiais: [Portal do TSE](https://dadosabertos.tse.jus.br/), [pacotes de 2022](https://dadosabertos.tse.jus.br/dataset/resultados-2022-arquivos-transmitidos-para-totalizacao), [pacotes de 2026](https://dadosabertos.tse.jus.br/dataset/resultados-2026-arquivos-transmitidos-para-totalizacao), [divulgação 2026](https://www.tse.jus.br/eleicoes/informacoes-tecnicas-sobre-a-divulgacao-de-resultados), [layout de logs](https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/log-do-ecossistema).

## Etapas e eventos

| Evento | Trabalho realizado | Evidência |
| --- | --- | --- |
| `downloaded` | Streaming HTTP para arquivo temporário com backpressure. Aceita HTTPS de hosts oficiais, limita redirects, bytes e concorrência. | URL final, cabeçalhos HTTP, hora de coleta, quantidade de bytes e hashes. |
| `verified` | Confere tamanho recebido, checksum publicado se houver e identidade do recurso CKAN quando disponível. | Status do checksum, registro CKAN e hashes observados. |
| `extracted` | Lê o diretório central ZIP/ZIP64 e valida CRC e tamanho de cada entrada. Abre contêineres aninhados e transforma cada conteúdo suportado. | IDs de arquivo e de pai, ordinal, cabeçalhos ZIP, dados extraídos e status dos formatos. |
| `committed` | Fecha as partes NDJSON/GZIP de até 128 MiB de texto por parte, com buffer de escrita de 256 KiB, calcula hashes das saídas e renomeia o diretório temporário após a validação. | Manifesto de contagens e hashes. A partir daqui o ZIP bruto já foi descartado. |
| `loaded` | Cria cargas em lote direto para tabelas nativas do BigQuery, verifica a conclusão e grava um recibo local. | Jobs determinísticos, contagens, expiração e marcador remoto de lote completo. Os NDJSONs temporários podem ser liberados. |

O emissor é local e os estágios aguardam a etapa anterior. A fila limita os downloads e permite concorrência entre fontes pequenas. ZIPs de grande volume são tratados individualmente para limitar o disco. O mesmo cliente HTTP agenda no máximo uma nova solicitação por segundo, com retry limitado e respeito a `Retry-After`. O pipeline usa um arquivo ZIP temporário para ler o diretório central e descarta o arquivo após validação. ZIPs aninhados são temporários individuais. CSVs passam diretamente para o decoder de streaming. Um arquivo grande fora dos layouts suportados recebe status explícito de cobertura. O BigQuery recebe os NDJSONs sem bucket intermediário.

## Verificações antes de interpretar

1. Fixar recorte, versão, pleito, turno, UF/município/zona/seção e família de fonte. Registrar denominador e valores ausentes.
2. Comparar presença de arquivos e entradas, contagens, correspondência entre seção e urna, totais por candidato e diferenças com o resultado publicado.
3. Tratar separadamente datas internas, relógio da urna, timestamp da entrada ZIP, `Last-Modified` HTTP e atualização no catálogo. A fonte pública pode não expor criação/edição ou versões anteriores.
4. Preservar ordem e posição dos registros na fonte. Em análises de padrões, declarar a expectativa nula e os estratos antes de executar. Dependência operacional e testes múltiplos podem produzir aparentes anomalias. Uma diferença precisa ser reproduzida e cotejada.
5. CRC e hashes conferem integridade de bytes. Assinaturas preservadas exigem as chaves e procedimentos de verificação do TSE para validação criptográfica. O pipeline registra essa pendência.
