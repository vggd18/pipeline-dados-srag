# Desafio de Engenharia de Dados - GGMA (Sec. Saúde Recife)

Este repositório contém a minha solução para o desafio de Engenharia de Dados Júnior da Secretaria de Saúde do Recife. O projeto consiste em um pipeline ETL (Extract, Transform, Load) completo, construído para ser robusto, eficiente, escalável e observável.

O pipeline extrai dados de Síndrome Respiratória Aguda Grave (SRAG) do OpenDataSUS, aplica um rigoroso processo de transformação modularizado e os carrega em um banco de dados analítico local, pronto para as consultas SQL solicitadas.

## 🚀 Justificativa da Stack de Tecnologias

A escolha da stack foi um pilar central deste projeto, alinhada à filosofia do desafio de priorizar "qualidade ao invés de complexidade". A stack escolhida foi **Polars + DuckDB**.

### Por que Polars + DuckDB?

Esta é a "combinação perfeita" para este escopo.

* **Polars** é um framework de DataFrame *multi-thread* escrito em Rust. Ele oferece performance muito superior ao Pandas tradicional e possui um poderoso "Lazy Mode" que permite processar datasets maiores que a RAM disponível, como foi implementado neste projeto.
* **DuckDB** é um banco de dados SQL **analítico (OLAP)**, e não transacional (OLTP). Ao contrário do Postgres (OLTP row-based), o DuckDB é *column-based*, otimizado para a velocidade de consultas analíticas (`GROUP BY`, `AVG`, etc.).

Juntos, eles se comunicam via **Apache Arrow** (`pyarrow`), permitindo transferências de dados *zero-copy* (sem cópia) da memória do Polars para o DuckDB, resultando na carga mais rápida possível.

### Por que NÃO as Alternativas?

* **Por que não Pandas?** Pandas é (majoritariamente) *single-thread* e não possui um otimizador de consultas "Lazy" tão robusto. Para um dataset com +250 mil linhas e 194 colunas, ele estaria no seu limite de performance e memória, sendo uma escolha tecnicamente inferior.
* **Por que não Spark?** Spark é uma "bala de canhão para matar uma mosca". É um framework de computação *distribuída*, projetado para clusters e terabytes de dados. Instanciar uma sessão Spark (com sua JVM) para ler um único arquivo Parquet seria um exemplo de *over-engineering* (complexidade desnecessária).
* **Por que não Postgres ou SQLite?** Optei por não usar bancos de dados tradicionais como Postgres ou SQLite por dois motivos. Primeiro, eles são bancos **OLTP** (transacionais), otimizados para operações linha a linha e ineficientes para as consultas analíticas (`GROUP BY`) do desafio. Segundo, o Postgres exigiria a configuração de um ambiente de servidor (provavelmente via Docker), o que desviaria o foco do **tratamento dos dados** para a **arquitetura de infraestrutura**. O **DuckDB (OLAP)** foi a escolha correta por ser *serverless*, *column-based* e otimizado para a performance analítica que o desafio pedia, permitindo foco total na qualidade dos dados.

## 🏃 Como Executar o Projeto

1.  Clone este repositório:
    ```bash
    git clone https://github.com/vggd18/pipeline-dados-srag.git
    cd pipeline-dados-srag
    ```

2.  Crie um ambiente virtual e instale as dependências:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # No Windows: .\venv\Scripts\activate
    pip install -r requirements.txt
    ```

3.  **Configuração:** Copie o arquivo de configuração de exemplo para criar seu arquivo `.env` local:
    ```bash
    cp .env.example .env
    ```
    *(Os valores padrão no `.env` devem funcionar para este desafio).*

4.  Execute o pipeline ETL completo:
    ```bash
    python etl_pipeline.py
    ```
    O script irá (1) baixar os dados, (2) processá-los em modo *Lazy* e (3) carregar o resultado no arquivo `data/srag.duckdb`. A saída no terminal mostrará os logs de cada etapa e suas durações.

## 🧪 Testando a Qualidade e as Consultas

Para garantir a robustez e facilitar a validação, o projeto inclui:

1.  **Teste Pós-ETL:** A execução do `python etl_pipeline.py` inclui uma etapa final (`test_database()`) que verifica se a tabela `srag` foi criada com sucesso e loga a contagem de linhas e colunas carregadas.

2.  **Script de Execução de Consultas:** Para validar as consultas SQL analíticas, você pode usar o script dedicado:

    ```bash
    python test_queries.py
    ```
    Este script se conectará ao banco `data/srag.duckdb` (criado pelo ETL) e imprimirá os resultados das consultas encontradas na pasta `/sql/`.

---

## 📋 Relatório do Desafio

Esta seção cumpre o requisito de "breve relatório" do processo seletivo.

### 1. Dados

* **Descrição dos Dados:** Os dados escolhidos foram os registros de Síndrome Respiratória Aguda Grave (SRAG) **hospitalizados**, disponibilizados pelo Ministério da Saúde via OpenDataSUS.
* **Fonte(s) de Dados:**
    * [Página de Recursos (OpenDataSUS)](https://opendatasus.saude.gov.br/dataset/srag-2021-a-2024): Página principal do dataset SRAG.
    * [Arquivo de Dados (Parquet 2024)](https://opendatasus.saude.gov.br/gl/dataset/srag-2021-a-2024/resource/0df883b2-4c71-4e7d-ab77-1767e6b956c5): Link direto para o arquivo `.parquet` de 2024 utilizado neste projeto.
    * [Dicionário de Dados (PDF)](https://opendatasus.saude.gov.br/dataset/srag-2021-a-2024/resource/3135ac9c-2019-4989-a893-2ed50ebd8e68): Documento oficial descrevendo todas as colunas do dataset.
* **Justificativa da Escolha:**
    1.  **Relevância:** Sendo a vaga para a Secretaria de Saúde do Recife, utilizar dados de saúde pública (SRAG) é diretamente relevante para o domínio da organização.
    2.  **Escopo:** Conforme a orientação do desafio de focar em "qualidade" , optei por utilizar apenas os dados de 2024. Este escopo (+250.000 registros) é robusto o suficiente para provar a eficiência do pipeline (especialmente a otimização de memória) sem adicionar complexidade desnecessária.

### 2. Pipeline (Extração e Transformação)


O pipeline foi construído seguindo boas práticas de engenharia, com foco em **Modularidade**, **Observabilidade** e **Eficiência de Memória** (Lazy Mode).

* **Configuração Externa:** Todas as constantes (URLs, caminhos, logs) são gerenciadas em um arquivo `config.py` e carregadas via `.env` (usando `python-dotenv`). Isso permite fácil reconfiguração sem alterar o código do pipeline e segue a boa prática de separar configuração de código.

* **Observabilidade:** Um decorator `@log_step` foi implementado para registrar o início, fim e **duração (em segundos)** de cada etapa principal. Logs detalhados (ex: contagem de registros e colunas, tamanho do banco) são registrados durante a execução para permitir o rastreamento do progresso e resultados.

* **Extração (E):** A função `extract()` usa `pl.scan_parquet()` para escanear o arquivo do S3 sem carregá-lo na memória, apenas lendo o esquema.

* **Transformação (T):** Esta é a etapa central e foi modularizada em uma sequência de funções atômicas que operam no `LazyFrame` (plano de execução):
    1.  **`rename_columns()`**: Padroniza todas as colunas para `snake_case` minúsculo.
    2.  **`convert_data_types()`**: Converte colunas de data (`str` -> `Date`), aplica tipagem booleana (`1/2/9` -> `True/False/None`) e converte colunas de alta cardinalidade para `Categorical`.
    3.  **`map_categorical_codes()`**: Realiza o polimento semântico, mapeando códigos crípticos (ex: `classi_fin = '5'`) para labels legíveis (ex: `"SRAG por covid-19"`).
    4.  **`clean_and_deduplicate()`**: Aplica a lógica de negócio central: deduplica pela chave primária (`nu_notific`), sanitiza colunas string (`strip/lowercase`) e filtra o dataset para manter apenas casos válidos (`hospital = True`).
    5.  **`get_valid_columns()`**: Como etapa final de qualidade, esta função remove colunas com nulidade > 70%, mas aplica um **trade-off** estratégico para manter colunas de comorbidade (ex: `renal`, `obesidade`) que são analiticamente importantes, mesmo que esparsas.

* **Carregamento (L):** O carregamento no banco SQL DuckDB é feito de forma nativa e otimizada:
    1.  Uma conexão direta com o banco (`duckdb.connect()`) é aberta.
    2.  O plano "Lazy" final do Polars (`df_final`) é "registrado" (`con.register()`) no DuckDB, usando Apache Arrow para uma transferência *zero-copy*.
    3.  O DuckDB executa o plano completo e realiza a carga em massa com `CREATE OR REPLACE TABLE srag AS SELECT ...`.

### 3. Consultas SQL

As consultas foram projetadas para demonstrar duas abordagens analíticas diferentes: uma análise "Micro" (focada em dados de Recife) e uma "Macro" (focada em padrões epidemiológicos).

* **Consulta 1: `sql/consulta_1.sql` - Análise Micro: Perfil de Gravidade por Faixa Etária em Recife**
    * **Descrição:** Esta consulta filtra os casos de SRAG por COVID-19 (2024) para residentes do Recife (código `261160`). Ela agrupa os pacientes em faixas etárias (`pediatrico`, `adulto`, `idoso`) e calcula o total de casos, a taxa de UTI e a taxa de letalidade para cada grupo.
    * **Justificativa Analítica:** Sendo a vaga para a Sec. de Saúde do Recife, esta consulta gera um recorte de **alta relevância local**. Ela responde: "Qual foi o impacto da COVID-19 em *nossos* munícipios?". O insight (baseado nos 73 casos) foi claro: o número de casos graves foi baixo em 2024, e a letalidade se concentrou de forma extrema no grupo 'idoso' (42.8%), um dado valioso para a vigilância epidemiológica local.

* **Consulta 2: `sql/consulta_2.sql` - Análise Macro: Impacto do Acúmulo de Comorbidades na Gravidade (Brasil)**
    * **Descrição:** Esta consulta usa uma **Common Table Expression (CTE)** para uma análise de risco avançada. O CTE primeiro calcula o *número total de comorbidades* (diabetes, cardiopatia, etc.) para cada paciente de COVID-19 em *todo o dataset*. A consulta principal agrupa os pacientes por essa contagem (0, 1, 2, 3+) e calcula a `taxa_uti` e a `taxa_letalidade` para cada grupo de risco.
    * **Justificativa Analítica:** Ao contrário da Consulta 1, uma análise de comorbidades no recorte de Recife (N=73) seria estatisticamente fraca. Por isso, esta consulta muda para uma abordagem **"Macro" (nível nacional)** para identificar um padrão epidemiológico robusto. O insight foi poderoso e claro: **o risco aumenta linearmente com o número de comorbidades**. A taxa de letalidade saltou de **10.9%** (para 0 comorbidades) para **27.3%** (para 3+ comorbidades). Esta consulta demonstra o valor do pipeline ETL (que limpou +10 colunas booleanas) e a capacidade de ajustar o escopo da análise (Micro vs. Macro) para gerar insights estatisticamente válidos.