# Desafio de Engenharia de Dados - GGMA (Sec. Saúde Recife)

Este repositório contém a minha solução para o desafio de Engenharia de Dados Júnior da Secretaria de Saúde do Recife. O projeto consiste em um pipeline ETL (Extract, Transform, Load) completo, construído para ser robusto, eficiente e escalável.

O pipeline extrai dados de Síndrome Respiratória Aguda Grave (SRAG) do OpenDataSUS, aplica um rigoroso processo de transformação e limpeza, e os carrega em um banco de dados analítico local, pronto para as consultas SQL solicitadas.

## 🚀 Justificativa da Stack de Tecnologias

A escolha da stack foi um pilar central deste projeto, alinhada à filosofia do desafio de priorizar "qualidade ao invés de complexidade" . A stack escolhida foi **Polars + DuckDB**.

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

3.  Execute o pipeline ETL completo:
    ```bash
    python etl_pipeline.py
    ```
    O script irá (1) baixar os dados, (2) processá-los em modo *Lazy* e (3) carregar o resultado no arquivo `data/srag.duckdb`.

4.  (Opcional) Verifique o banco de dados via CLI:
    ```bash
    duckdb data/srag.duckdb
    ```
    E então rode suas consultas.

## 🧪 Testando a Qualidade e as Consultas

Para garantir a robustez e facilitar a validação, o projeto inclui:

1.  **Teste Pós-ETL:** A execução do `python etl_pipeline.py` inclui uma etapa final (`test_database()`) que verifica se a tabela `srag` foi criada com sucesso no banco `data/srag.duckdb`.

2.  **Script de Execução de Consultas:** Como alternativa à CLI do DuckDB, você pode executar as consultas SQL analíticas (`sql/*.sql`) diretamente através de um script Python dedicado:

    ```bash
    python test_queries.py
    ```
    Este script se conectará ao banco `data/srag.duckdb` (que deve ter sido criado pelo `etl_pipeline.py` primeiro) e imprimirá os resultados das consultas encontradas na pasta `/sql/`. Isso garante que as consultas possam ser validadas independentemente da configuração do ambiente do avaliador.
    
---

## 📋 Relatório do Desafio

Esta seção cumpre o requisito de "breve relatório" do processo seletivo.

**Nota sobre a Implementação:** O pipeline foi implementado seguindo boas práticas de engenharia, como a separação de responsabilidades em funções (`extract`, `transform`, `load`), o uso do módulo `logging` para rastreabilidade e a otimização de memória através do "Lazy Mode" do Polars.

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

O pipeline foi construído usando o **"Lazy Mode"** (modo preguiçoso) do Polars para garantir o mínimo uso de memória (resolvendo o erro de `killed` por OOM) e máxima performance. Nenhuma etapa é executada até o `con.register()` final.

* **Extração (E):** A extração é feita com `pl.scan_parquet(URL_PATH)`. Isso apenas "escaneia" o esquema do arquivo no S3 (usando `s3fs`) sem carregá-lo na memória.

* **Transformação (T):** Todas as transformações são encadeadas em um único plano de execução "Lazy":
    1.  **Renomeação:** Todas as 194 colunas foram convertidas para `snake_case` (minúsculas) para padronização SQL.
    2.  **Tipagem Robusta:** As colunas foram convertidas para seus tipos corretos (ex: `Date`, `Int32`) usando `strict=False`. Isso garante que dados sujos (ex: uma data mal formatada) sejam convertidos para `NULL` em vez de quebrar o pipeline.
    3.  **Mapeamento Semântico (Polimento):** Colunas-chave (`cs_sexo`, `evolucao`, `classi_fin`, etc.) foram mapeadas de códigos crípticos (ex: `"1"`, `"2"`, `"9"`) para valores legíveis (ex: `"Cura"`, `"Óbito"`, `None`). Isso é crucial para a usabilidade do analista.
    4.  **Mapeamento Booleano:** Dezenas de colunas (ex: `diabetes`, `cardiopati`) que usavam o padrão `1/2/9` foram convertidas para `True/False/None`, permitindo análises SQL complexas (como a Consulta 2).
    5.  **Sanitização:** Todas as colunas `String` restantes (como `co_mun_res`) foram sanitizadas (`.str.strip_chars().str.to_lowercase()`) para padronizar o case e remover espaços.
    6.  **Deduplicação:** Os registros foram deduplicados pela chave primária `nu_notific`.
    7.  **Filtragem de Integridade:** O dataset foi filtrado para `hospital = True`, alinhando os dados com a definição oficial de "casos hospitalizados".
    8.  **Filtragem de Colunas por Nulidade (com Trade-off):** Como etapa final da transformação, foi implementada uma lógica para remover colunas com mais de 70% de valores nulos. No entanto, foi feito um **trade-off estratégico**: colunas consideradas de alta importância analítica (especificamente `puerpera`, `hematologi`, `sind_down`, `hepatica`, `renal`, `obesidade`), mesmo que acima do threshold de 70%, foram **explicitamente mantidas** no dataset final. Essa decisão prioriza a retenção de informações potencialmente valiosas para a análise de comorbidades, balanceando a limpeza de dados com as necessidades do negócio.

* **Carregamento (L):** O carregamento no DuckDB é feito de forma nativa e otimizada:
    1.  O plano "Lazy" do Polars (`df_final_lazy`) é "registrado" no DuckDB (`con.register()`).
    2.  Esta operação é *zero-copy* (via `pyarrow`), "emprestando" os dados na memória sem copiá-los.
    3.  O DuckDB executa o plano e realiza a carga em massa com `CREATE OR REPLACE TABLE srag AS SELECT ...`, o que é ordens de magnitude mais rápido do que métodos de inserção tradicionais.

### 3. Consultas SQL

As consultas foram projetadas para demonstrar duas abordagens analíticas diferentes: uma análise "Micro" (focada em dados de Recife) e uma "Macro" (focada em padrões epidemiológicos).

* **Consulta 1: `sql/consulta_1.sql` - Análise Micro: Perfil de Gravidade por Faixa Etária em Recife**
    * **Descrição:** Esta consulta filtra os casos de SRAG por COVID-19 (2024) para residentes do Recife (código `261160`). Ela agrupa os pacientes em faixas etárias (`pediatrico`, `adulto`, `idoso`) e calcula o total de casos, a taxa de UTI e a taxa de letalidade para cada grupo.
    * **Justificativa Analítica:** Sendo a vaga para a Sec. de Saúde do Recife, esta consulta gera um recorte de **alta relevância local**. Ela responde: "Qual foi o impacto da COVID-19 em *nossos* munícipios?". O insight (baseado nos 73 casos) foi claro: o número de casos graves foi baixo em 2024, e a letalidade se concentrou de forma extrema no grupo 'idoso' (42.8%), um dado valioso para a vigilância epidemiológica local.

* **Consulta 2: `sql/consulta_2.sql` - Análise Macro: Impacto do Acúmulo de Comorbidades na Gravidade (Brasil)**
    * **Descrição:** Esta consulta usa uma **Common Table Expression (CTE)** para uma análise de risco avançada. O CTE primeiro calcula o *número total de comorbidades* (diabetes, cardiopatia, etc.) para cada paciente de COVID-19 em *todo o dataset*. A consulta principal agrupa os pacientes por essa contagem (0, 1, 2, 3+) e calcula a `taxa_uti` e a `taxa_letalidade` para cada grupo de risco.
    * **Justificativa Analítica:** Ao contrário da Consulta 1, uma análise de comorbidades no recorte de Recife (N=73) seria estatisticamente fraca. Por isso, esta consulta muda para uma abordagem **"Macro" (nível nacional)** para identificar um padrão epidemiológico robusto. O insight foi poderoso e claro: **o risco aumenta linearmente com o número de comorbidades**. A taxa de letalidade saltou de **10.9%** (para 0 comorbidades) para **27.3%** (para 3+ comorbidades). Esta consulta demonstra o valor do pipeline ETL (que limpou +10 colunas booleanas) e a capacidade de ajustar o escopo da análise (Micro vs. Macro) para gerar insights estatisticamente válidos.