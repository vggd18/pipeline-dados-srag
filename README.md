# Desafio de Engenharia de Dados - GGMA (Sec. Saúde Recife)

Este repositório contém a minha solução para o desafio de Engenharia de Dados Júnior da Secretaria de Saúde do Recife. O projeto consiste em um pipeline ETL (Extract, Transform, Load) que processa dados de Síndrome Respiratória Aguda Grave (SRAG) do OpenDataSUS.

O pipeline é construído com **Polars** e **DuckDB**, utilizando uma abordagem "Lazy" (preguiçosa) para garantir alta performance e baixo uso de memória, permitindo o processamento de datasets maiores que a RAM disponível.

## 🚀 Tecnologias Utilizadas

  * **Python 3.10+**
  * **Polars:** Para extração e transformação de dados em alta performance (Lazy Mode).
  * **DuckDB:** Como banco de dados SQL analítico (OLAP) open-source.
  * **s3fs:** Para permitir que o Polars leia (`scan_parquet`) arquivos diretamente do S3.
  * **PyArrow:** Como a "ponte" de interoperabilidade zero-copy entre Polars e DuckDB.

## 🏃 Como Executar o Projeto

1.  Clone este repositório:

    ```bash
    git clone https://github.com/vggd18/pipeline-dados-srag.git
    cd pipeline-dados-srag
    ```

2.  Crie um ambiente virtual (recomendado) e instale as dependências:

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # No Windows: .\venv\Scripts\activate
    pip install -r requirements.txt
    ```

3.  Execute o pipeline ETL:
    ```bash
    python etl_pipeline.py
    ```

    O script irá baixar os dados, processá-los e carregar o resultado no arquivo `data/srag.duckdb`.

4.  (Opcional) Para verificar o banco de dados, você pode usar a CLI do DuckDB:

    ```bash
    duckdb data/srag.duckdb
    ```

    E então rodar suas consultas SQL (ex: `SELECT * FROM srag LIMIT 5;`).

-----

## 📋 Relatório do Desafio

Esta seção cumpre o requisito de "breve relatório" do processo seletivo.

### 1\. Dados

  * **Descrição dos Dados:**
    Os dados escolhidos foram os registros de Síndrome Respiratória Aguda Grave (SRAG) **hospitalizados**, disponibilizados pelo Ministério da Saúde via OpenDataSUS. Este conjunto de dados inclui informações demográficas, sintomas, fatores de risco, dados de vacinação (COVID-19 e Influenza), internação, uso de UTI e resultados laboratoriais.

  * **Fonte(s) de Dados:**

      * **Página de Recurso:** [Dataset Open Data SUS](https://opendatasus.saude.gov.br/dataset/srag-2021-a-2024)
      * **Link Direto (Parquet 2024):** [Link Direto Parquet](https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2024/INFLUD24-26-06-2025.parquet)
      * **Dicionário de Dados:** [Dicionario de Dados 2019 a 2025](https://opendatasus.saude.gov.br/dataset/39a4995f-4a6e-440f-8c8f-b00c81fae0d0/resource/3135ac9c-2019-4989-a893-2ed50ebd8e68/download/dicionario-de-dados-2019-a-2025.pdf)

  * **Justificativa da Escolha:**
    Escolhi este dataset por três motivos:

    1.  **Relevância:** Sendo uma vaga para a Secretaria de Saúde do Recife, utilizar dados públicos de saúde (SRAG/COVID-19) é diretamente relevante para o domínio de negócio da organização.
    2.  **Riqueza:** O dataset possui 194 colunas, o que permitiu demonstrar uma variedade de técnicas de transformação (tipagem, sanitização, mapeamento semântico, etc.).
    3.  **Escopo:** Conforme a orientação do desafio de focar em "qualidade ao invés de quantidade", optei por utilizar apenas os dados de 2024. Este escopo (+250.000 registros) é robusto o suficiente para provar a eficiência do pipeline, permitindo uma entrega polida e bem documentada.

### 2\. Pipeline (Extração e Transformação)

  * **Extração (E):** A extração é feita em Python usando o modo "Lazy" do Polars. O método `pl.scan_parquet()` é usado para "escanear" o arquivo `.parquet` diretamente do S3. Isso não carrega dados na memória, apenas lê o esquema e prepara um plano de execução.

  * **Transformação (T):** Todas as etapas de transformação são encadeadas em um único plano "Lazy", o que garante o mínimo uso de memória e máxima otimização:

    1.  **Renomeação:** Todas as 194 colunas foram convertidas para `snake_case` (minúsculas) para padronização.
    2.  **Tipagem Robusta:**
          * **Datas:** Colunas de data (ex: `dt_notific`) foram convertidas de string para `pl.Date`, usando `str.to_datetime(strict=False)` para garantir que formatos mistos ou nulos não quebrassem o pipeline.
          * **Números:** Colunas numéricas (ex: `nu_idade_n`) foram convertidas para `pl.Int32(strict=False)`.
          * **Identificadores:** Colunas que são códigos (ex: `co_mun_not`, `nu_notific`) foram mantidas como `String` para preservar zeros à esquerda.
    3.  **Mapeamento Semântico:** Para melhorar a legibilidade para o analista, os dados foram "polidos":
          * **Booleanos:** Colunas que usam o padrão `1-Sim`, `2-Não`, `9-Ignorado` (ex: `febre`, `uti`) foram convertidas para o tipo `Boolean` (`True`, `False`, `None`).
          * **(WIP) Dicionário de Dados:** Colunas categóricas serão mapeadas de seus códigos para valores legíveis.
    4.  **Sanitização:** Todas as colunas `String` restantes passaram por `.str.strip_chars().str.to_lowercase()` para remover espaços e padronizar o case.
    5.  **Filtragem de Integridade:** O dataset foi filtrado para manter apenas registros onde `hospital == True`, alinhando os dados com a definição oficial de "casos hospitalizados".
    6.  **Deduplicação:** O DataFrame final foi deduplicado pela chave primária `nu_notific`.

  * **Carregamento (L):** O carregamento no banco SQL DuckDB é feito de forma nativa e otimizada:

    1.  Uma conexão direta com o banco (`duckdb.connect()`) é aberta.
    2.  O plano "Lazy" do Polars (`df_final_lazy`) é "registrado" (`con.register()`) no DuckDB. Esta é uma operação *zero-copy* que usa o Apache Arrow para compartilhar os dados.
    3.  O DuckDB executa o plano e realiza a carga em massa com `CREATE OR REPLACE TABLE srag AS SELECT ...`, o que é ordens de magnitude mais rápido do que métodos de inserção tradicionais.
