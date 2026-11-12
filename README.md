# Auditoria de dados das eleições presidenciais brasileiras

Coleta reproduzível de dados públicos do TSE das eleições de **2018, 2022 e 2026**, primeiro e segundo turnos. O objetivo é permitir que outra pessoa reconstrua os cruzamentos entre arquivos da urna, boletins e resultados de totalização.

## Reproduzir

Requisitos: Node.js 22.12+, npm, Python 3.10+ para renovar o catálogo. Use um diretório temporário fora deste checkout se o volume for grande. Um inventário anterior pode ser usado com `--inventory`.

```powershell
npm ci
npm run check
npm test
npm run pipeline -- inventory --work E:/tse-auditoria
npm run pipeline -- plan --work E:/tse-auditoria
npm run pipeline -- smoke --work E:/tse-auditoria
```

O smoke padrão baixa uma correspondência oficial de 2026, valida o checksum publicado e escreve partes NDJSON comprimidas localmente. `--resource` aceita o caminho completo no inventário ou um nome de arquivo único. O smoke limita o download a 350 MB. Confira o [método](docs/metodo.md) o [contrato das tabelas](docs/tabelas.md) e os [formatos e conferências](docs/formatos.md).

## Carga no BigQuery

Use um projeto com faturamento/API BigQuery habilitados e um dataset existente em sua região. A identidade ADC precisa poder criar jobs no projeto e criar/gravar tabelas no dataset. Para ambiente local, instale a CLI do Google Cloud, execute `gcloud auth application-default login` e configure o projeto de quota se solicitado. Para automação, use ADC de identidade de workload. [Autenticação oficial](https://docs.cloud.google.com/bigquery/docs/authentication/getting-started)

```powershell
npm run pipeline -- smoke --work E:/tse-auditoria --project MEU_PROJETO --dataset MEU_DATASET --location us-central1 --expires-hours 72
npm run pipeline -- run --work E:/tse-auditoria --snapshot analise-01 --project MEU_PROJETO --dataset MEU_DATASET --location us-central1 --expires-hours 72
```

O segundo comando inicia a coleta integral do inventário. Comece com o smoke para conferir permissões, contagem de linhas, expiração e custo de consulta. O transporte padrão envia JSON descompactado em fluxo. O flag `--gzip-transport` testa envio comprimido, sujeito à validação em um smoke real antes de um lote amplo. Reutilize o mesmo snapshot para retomar uma execução interrompida antes da expiração. Use um novo snapshot para uma coleta independente. A duração das tabelas é contada por lote. O tempo de ingestão do acervo integral ainda não foi medido; valide a vazão com um pacote representativo antes de programar a janela. Para comparar tudo ao mesmo tempo, programe a análise dentro da janela em que os primeiros lotes ainda estão disponíveis.

Os exemplos assumem PowerShell. Se o npm remover flags ao encaminhar argumentos, use `node node_modules/tsx/dist/cli.mjs` com os mesmos argumentos a partir de `smoke`, `run`, `inventory` ou `plan`.

Este repositório contém código, metodologia e testes. Resultados científicos e sua revisão devem citar a versão do código, o inventário, os recibos e as consultas empregadas.
