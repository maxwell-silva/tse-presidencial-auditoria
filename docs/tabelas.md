# Contrato das tabelas

Cada lote publica tabelas com sufixo derivado de um identificador determinístico. Use as tabelas de lotes cujo marcador `committed_` existe e ainda está dentro do TTL.

| Tabela | Grão | Identificadores e conteúdo |
| --- | --- | --- |
| `packages_` | Uma resposta HTTP primária. | `batch_id`, `package_id`, ano, turno quando explícito, categoria, URL, fonte, hora da coleta, tamanho e SHA-256. O JSON de metadados contém os cabeçalhos, catálogo, checksum e cobertura. |
| `files_` | Uma entrada, inclusive em contêiner aninhado. Arquivos avulsos também ganham uma linha. | `file_id`, `package_id`, `parent_file_id`, ordinal, caminho, tamanho e SHA-256. O JSON inclui campos ZIP locais/centrais, CRC quando presente, atributos, dados de entrada e status do decoder. |
| `records_` | Um registro de conteúdo extraído. | `file_id`, `package_id`, `kind`, ordinal e `payload` JSON. Cada layout conserva os campos de origem aplicáveis. Assinaturas são representadas como partes Base64 com offset. |
| `committed_` | Marcador de que as cargas daquele lote foram confirmadas. | Expiração e manifesto descritos pelo carregador. |

As colunas `batch_id` nas linhas identificam a captura individual. O sufixo das tabelas identifica um lote de carga que pode agrupar várias capturas. Cruze pelo `file_id` e `package_id`; use o ordinal no arquivo para reconstruir ordem. Os votos `rdv` se juntam ao contexto `rdv_cabecalho` por `file_id`. Use `JSON_VALUE` e conversões explícitas ao projetar colunas. Nomes e tipos do payload dependem do layout original e do decoder. Use o registro de formatos para conferir o significado antes de comparar anos.

A camada de arquivos inclui metadados individuais mesmo quando o conteúdo está fora do decodificador. Use os status e a cobertura para reportar a diferença entre arquivo encontrado e arquivo convertido. Datas originais permanecem em sua representação de origem. Registre qualquer normalização de fuso na consulta que a aplicar.



Para descobrir quais lotes estão completos, consulte o catálogo do dataset:

```sql
SELECT table_name AS marcador, SUBSTR(table_name, LENGTH('committed_') + 1) AS sufixo
FROM `PROJETO.DATASET.INFORMATION_SCHEMA.TABLES`
WHERE STARTS_WITH(table_name, 'committed_');
```

As tabelas correspondentes são `packages_<sufixo>`, `files_<sufixo>` e `records_<sufixo>`. Para vários lotes, use uma lista explícita de sufixos confirmados. As tabelas de carga usam registros de lotes com expiração; consulte o manifesto antes de combinar versões.
