import polars as pl
import polars.selectors as cs
from polars import col
import duckdb
import os


##### Extract #####
URL_PATH = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2024/INFLUD24-26-06-2025.parquet"
df_lazy = pl.scan_parquet(URL_PATH)

##### Transform #####

## Cleaning & Quality

boolean_columns = ["estrang", "pov_ct", "nosocomial", "febre", "tosse", "garganta", 
                   "dispneia", "desc_resp", "saturacao", "diarreia", "vomito", "dor_abd", 
                   "fadiga", "perd_olft", "perd_pala", "outro_sin", "fator_risc", 
                   "puerpera", "cardiopati", "hematologi", "sind_down", "hepatica", "asma", 
                   "diabetes", "neurologic","pneumopati", "imunodepre","renal", "obesidade", 
                   "tabag", "out_morbi", "vacina_cov", "vacina", "mae_vac", "m_amamenta", 
                   "antiviral", "trat_cov", "hospital", "uti", "amostra", "pos_an_flu", 
                   "pos_an_out", "surto_sg", "co_detec", "vg_reinf"]

float_columns = ["obes_imc"]

sexo_map = {
  "1": "masculino",
  "2": "feminino",
  "9": None
}

raca_map = {
  "1": "branca",
  "2": "preta",
  "3": "amarela",
  "4": "parda",
  "5": "indígena",
  "9": None
}

escolaridade_map = {
  "0": "Sem escolaridade/Analfabeto",
  "1": "Fundamental 1º ciclo (1ª a 5ª série)",
  "2": "Fundamental 2º ciclo (6ª a 9ª série)",
  "3": "Médio (1º ao 3º ano)",
  "4": "Superior",
  "5": "Não se aplica",
  "9": None
}

evolucao_map = {
  "1": "Cura",
  "2": "Óbito",
  "3": "Óbito por outras causas",
  "9": None
}

classi_fin_map = {
  "1":"SRAG por influenza",
  "2":"SRAG por outro vírus respiratório" ,
  "3":"SRAG por outro agente  etiológico",
  "4":"SRAG não especificado",
  "5":"SRAG por covid-19"
}

col_names = df_lazy.collect_schema().names()
df_final_lazy = df_lazy.rename({ col: col.lower() for col in col_names}) \
  .with_columns([
    (cs.starts_with("dt_") | cs.starts_with("dose_") | cs.starts_with("dos_")).str.to_datetime().dt.date(),
    (cs.starts_with("sg_") | cs.starts_with("fab_")).cast(pl.Categorical),
    pl.col("cs_sexo").replace(sexo_map, default=None).cast(pl.Categorical),
    pl.col("cs_raca").replace(raca_map, default=None).cast(pl.Categorical),
    pl.col("cs_escol_n").replace(escolaridade_map, default=None).cast(pl.Categorical),
    pl.col("evolucao").replace(evolucao_map, default=None).cast(pl.Categorical),
    pl.col("classi_fin").replace(classi_fin_map, default=None).cast(pl.Categorical),
    pl.col("nu_idade_n").cast(pl.Int32),
    pl.col(float_columns).cast(pl.Float64, strict=False),
    *[pl.when(pl.col(col_name) == "1").then(True)
        .when(pl.col(col_name) == "2").then(False)
        .otherwise(None).cast(pl.Boolean).alias(col_name)
    for col_name in boolean_columns]
  ]) \
  .unique("nu_notific") \
  .with_columns(cs.string().str.strip_chars().str.to_lowercase()) \
  .filter(pl.col("hospital") == True)

## Structuring & Modeling

### Todo...

## Enrichment & Integration

### Todo...

##### Load #####
DB_DIR = "data"
DB_PATH = "data/srag.duckdb"

os.makedirs(DB_DIR, exist_ok=True)

con = duckdb.connect(database=DB_PATH)
con.register('lazy_srag', df_final_lazy)
con.execute("CREATE OR REPLACE TABLE srag AS SELECT * FROM lazy_srag")
con.close()