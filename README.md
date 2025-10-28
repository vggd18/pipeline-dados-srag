# Desafio de Engenharia de Dados - GGMA (Sec. Saúde Recife)

Este repositório contém a minha solução para o desafio de Engenharia de Dados da Secretaria de Saúde do Recife. O projeto consiste em um pipeline ETL (Extract, Transform, Load) que processa dados de Síndrome Respiratória Aguda Grave (SRAG) do OpenDataSUS.

## 🚀 Tecnologias Utilizadas

* **Python 3.10+**
* **Polars:** Para extração e transformação de dados em alta performance.
* **DuckDB:** Como banco de dados SQL analítico (OLAP) open-source.
* **s3fs:** Para permitir a leitura direta dos arquivos Parquet do bucket S3.

## 🏃 Como Executar o Projeto

1.  Clone este repositório:
    ```bash
    git clone [URL-DO-SEU-REPOSITÓRIO-GHUB]
    cd [NOME-DO-SEU-REPOSITÓRIO]
    ```

2.  Crie um ambiente virtual (recomendado) e instale as dependências:
    ```bash
    python -m venv venv
    source venv/bin/activate  # No Windows: .\venv\Scripts\activate
    pip install -r requirements.txt
    ```

3.  Execute o pipeline ETL:
    ```bash
    python etl_pipeline.py
    ```

---

## 📋 Relatório do Desafio
Esta seção cumpre o requisito de relatório do processo seletivo.

### 1. Dados

***Descrição dos Dados:**
    Os dados escolhidos foram os registros de Síndrome Respiratória Aguda Grave (SRAG) hospitalizados, disponibilizados pelo Ministério da Saúde através do OpenDataSUS. Este conjunto de dados inclui informações demográficas, sintomas, fatores de risco, dados de vacinação (COVID-19 e Influenza), internação, uso de UTI e resultados laboratoriais.

***Fonte(s) de Dados:**
    * **Página de Recurso:** `https://opendatasus.saude.gov.br/dataset/srag-2021-a-2024`
    * **Link Direto (Parquet 2024):** `https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2024/INFLUD24-26-06-2025.parquet`
    * **Dicionário de Dados:** `https://opendatasus.saude.gov.br/dataset/39a4995f-4a6e-440f-8c8f-b00c81fae0d0/resource/3135ac9c-2019-4989-a893-2ed50ebd8e68/download/dicionario-de-dados-2019-a-2025.pdf`

***Justificativa da Escolha:**
    Escolhi este dataset por três motivos:
    1.  **Relevância:** Sendo uma vaga para a Secretaria de Saúde do Recife, utilizar dados públicos de saúde (SRAG/COVID-19) é diretamente relevante para o domínio de negócio da organização.
    2.  **Riqueza:** O dataset possui 194 colunas, o que permitiu demonstrar uma variedade de técnicas de transformação (tipagem de data, mapeamento de dicionário, sanitização de strings, tratamento de booleanos).
    3.  **Escopo:** Conforme a orientação do desafio de focar em "qualidade ao invés de quantidade", optei por utilizar apenas os dados de 2024. Este escopo (+260.000 registros) é robusto o suficiente para provar a eficiência do pipeline, sem adicionar complexidade desnecessária no download de múltiplos arquivos anuais.

###2. Pipeline (Extração e Transformação)

* **Extração (E):** A extração é feita em Python usando a biblioteca Polars. O método `pl.read_parquet()` é usado para ler o arquivo `.parquet` diretamente do bucket S3 do OpenDataSUS, carregando-o em um DataFrame em memória.

* **Transformação (T):** Esta foi a etapa mais complexa, focada em limpeza, normalização e enriquecimento semântico:
    1.  **Renomeação:** Todas as colunas foram convertidas para `snake_case` (minúsculas) para padronização.
    2.  **Tipagem Robusta:**
        * **Datas:** Colunas de data (ex: `dt_notific`) foram convertidas de string para `pl.Date`, usando `str.to_datetime(strict=False)` para garantir que formatos mistos (date e datetime) ou nulos não quebrassem o pipeline.
        * **Números:** Colunas numéricas (ex: `nu_idade_n`) foram convertidas para `pl.Int32(strict=False)`.
        * **Identificadores:** Colunas que são códigos (ex: `co_mun_not`, `nu_notific`) foram mantidas como `String` para preservar zeros à esquerda.
    3.  **Mapeamento Semântico (Qualidade):**
        * **Booleanos:** Colunas que usam o padrão `1-Sim`, `2-Não`, `9-Ignorado` (ex: `febre`, `uti`, `cardiopati`) foram convertidas para o tipo `Boolean` (`True`, `False`, `None`).
        * **Dicionário de Dados:** Colunas categóricas (ex: `evolucao`, `cs_sexo`) foram mapeadas de seus códigos (ex: `1`, `2`) para valores legíveis (ex: `"Cura"`, `"Óbito"`), melhorando a legibilidade para o analista.
    4.  **Sanitização:** Todas as colunas `String` restantes passaram por `.str.strip_chars().str.to_lowercase()` para remover espaços e padronizar o case.
    5.  **Deduplicação:** O DataFrame final foi deduplicado pela chave primária `nu_notific`.
