# Formatos e regras de transformação

O campo `ordinal` começa em 1 para cada tipo de registro dentro de um arquivo. Registros de CSV filtrados preservam o ordinal do registro de origem, excluindo a linha de cabeçalho. `file_id` associa cada registro à linha de metadados do arquivo individual.

## Resultados, boletins e correspondências

- CSV com separador ponto e vírgula, aspas duplas, aspas duplicadas dentro de campos e registros que podem incluir quebras de linha. O leitor processa blocos e mantém um registro por vez. Em 2018, preserva aspas internas sem escape quando o caractere seguinte indica continuação do campo, variante observada no boletim de Alagoas. A quantidade de colunas continua validada. BOM UTF-8 tem prioridade. Sem BOM, o padrão é Windows-1252, a codificação usada para interpretar os CSVs oficiais de BU examinados. `decodeCsvStream` aceita uma codificação explícita quando o leiaute da fonte especifica outra.
- Os campos conservam nomes, ordem de leitura e valores textuais de origem, incluindo zeros à esquerda, espaços de preenchimento, casas decimais com vírgula e marcadores como `#NULO#`. Nomes de coluna de payload JSON podem ser exibidos em outra ordem pela plataforma de consulta. A contagem de campos de cada registro precisa corresponder à do cabeçalho.
- Quando o cabeçalho contém `CD_CARGO` ou `CD_CARGO_PERGUNTA`, seleciona o código 1. Espaços externos e zeros à esquerda são ignorados somente na comparação. Arquivos sem campo de cargo são preservados no contexto publicado. Para EA20, o código do cargo no nome do arquivo filtra `c0001`. Cada JSON emitido preserva a estrutura interna em um registro.
- O histórico de totalização conserva o horário publicado, os totais acumulados e o recorte de cada arquivo. A publicação de um novo arquivo ou o horário de atualização HTTP não define a hora em que um voto foi registrado.

Fontes: [Boletins 2018](https://dadosabertos.tse.jus.br/dataset/resultados-2018-boletim-de-urna), [Boletins 2022](https://dadosabertos.tse.jus.br/dataset/resultados-2022-boletim-de-urna), [Boletins 2026](https://dadosabertos.tse.jus.br/dataset/resultados-2026-boletim-de-urna), [Resultados de 2026](https://dadosabertos.tse.jus.br/dataset/resultados-2026), [Divulgação de resultados EA20](https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/divulgacao-de-resultados).

## Arquivos de urna

Os pacotes oficiais de arquivos transmitidos de 2022 e 2026 são a fonte de BU, RDV, logs, envelopes de imagem e assinaturas. O catálogo de 2026 descreve os arquivos na situação “Totalizado”. Nenhum pacote bruto equivalente de 2018 foi localizado no inventário.

| Tipo | Transformação |
| --- | --- |
| BU binário | Leitor próprio ASN.1/BER de comprimentos definidos, com leiautes separados para 2022 e 2026. Produz `bu_asn1` por tipo de voto e votável do cargo 1, com seção, presença, aptos, horários publicados e campos de hash disponíveis. O tipo de voto é o código do leiaute BU. Campos ausentes no leiaute ficam nulos. |
| RDV | Produz um `rdv_cabecalho` com a identificação e os metadados do conteúdo e um `rdv` para cada entrada de voto presidencial, ligado pelo `file_id`. Conserva tipo de voto, digitação e posição no cargo. O tipo de voto segue a enumeração do RDV, diferente da do BU. O RDV do SA preserva origem e tipo de cédula. A posição é a ordem publicada no registro e não um timestamp de votação. |
| Log da urna | Extrai data, hora, severidade, número da máquina, aplicativo, mensagem e autenticador quando reconhecidos. Mantém a posição física da linha. Linhas fora do padrão permanecem consultáveis no campo mensagem com `formato_reconhecido=false`. |
| Log GEDAI | Reconhece timestamp, severidade e mensagem separados por tabulação. O aplicativo vem do nome do arquivo e o campo de número da urna fica nulo. A codificação de logs é UTF-8 estrito quando possível e Windows-1252 em caso contrário. |
| Assinatura e imagem de BU | Arquivos VSC e VSCMR têm conteúdo Base64 em partes numeradas no coletor. Envelopes da imagem de BU têm uma linha Base64. A ingestão calcula hashes do arquivo e não certifica assinaturas, chaves, certificados ou a cadeia de hashes da votação. |
| PDF e tipos sem decodificador | O coletor guarda metadados individuais, tamanho, hash e status. A extração de texto ou tabelas do PDF não está implementada. |

O leitor BER valida tags, limites dos campos e identificação da seção onde o nome de arquivo segue o padrão conhecido. Se encontrar um leiaute inesperado, o parser falha de forma explícita. O código foi implementado a partir das estruturas publicadas e ajustado às amostras binárias reais. Na execução de 12/11/2026, o servidor de documentação devolveu HTTP 403 no terminal para os ZIPs de especificação. Para o leiaute 2026, foi consultada a cópia pública dos esquemas oficiais [BU](https://github.com/ArvorCo/PNAD/blob/main/apuracao/scripts/secoes/bu.asn1) e [RDV](https://github.com/ArvorCo/PNAD/blob/main/apuracao/scripts/secoes/rdv.asn1). O leiaute legado foi verificado com os próprios arquivos oficiais de 2022. A cobertura das amostras não certifica variantes ainda não encontradas.

Fontes: [Documentação técnica de 2022](https://www.tse.jus.br/eleicoes/eleicoes-2022/documentacao-tecnica-do-software-da-urna-eletronica), [Documentação técnica de 2026](https://www.tse.jus.br/eleicoes/eleicoes-2026/eleicoes-2026), [Pacotes de urna de 2022](https://dadosabertos.tse.jus.br/dataset/resultados-2022-arquivos-transmitidos-para-totalizacao), [Pacotes de urna de 2026](https://dadosabertos.tse.jus.br/dataset/resultados-2026-arquivos-transmitidos-para-totalizacao).

## Conferências locais

- CSV de BU de RR no 1º turno. Os arquivos oficiais de 2018, 2022 e 2026 produziram respectivamente 12.635, 8.807 e 10.588 registros presidenciais.
- Pacote de urna RR, 2º turno de 2026. A coleta ponta a ponta registrou 9.133 entradas e 12.643.332 registros, incluindo 12.324.846 linhas de log. Foram lidos 1.519 BU, 1.519 RDV e 1.519 VSC. Em 1.519 pares positivos, a soma dos quantitativos presidenciais do BU coincidiu com a contagem de registros presidenciais do RDV.
- Pacote de urna do exterior, 1º turno de 2022. Foram lidos 915 BU, 960 RDV e 915 VSCMR. Em 861 pares positivos com BU e RDV, as contagens coincidiram. Os demais arquivos ficam fora dessa comparação de pares positivos.
- Testes locais sintéticos cobrem CSV particionado byte a byte, acentuação, aspas, quebra de linha, filtro presidencial, logs de urna e GEDAI, EA20 e rejeição de BER truncado.


Para reproduzir a comparação de contagem de votos em um pacote coletado, passe a pasta que contém o `manifest.json` e os shards. O comando informa os pares positivos, divergências e chaves presentes em apenas uma das fontes:

```powershell
node node_modules/tsx/dist/cli.mjs src/verify-pairs.ts CAMINHO_DA_CAPTURA
```
