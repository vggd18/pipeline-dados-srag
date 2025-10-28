import polars as pl
import polars.selectors as cs
from polars import col
import duckdb


##### Extract #####
URL_PATH = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2024/INFLUD24-26-06-2025.parquet"
df_raw = pl.read_parquet(URL_PATH)

##### Transform #####

## Cleaning & Quality

df_renamed = df_raw.rename({ col: col.lower() for col in df_raw.columns})

boolean_columns = ["estrang", "pov_ct", "nosocomial", "febre", "tosse", "garganta", "dispneia", 
                   "desc_resp", "saturacao", "diarreia", "vomito", "dor_abd", "fadiga", "perd_olft", 
                   "perd_pala", "outro_sin", "fator_risc", "puerpera", "cardiopati", "hematologi", 
                   "sind_down", "hepatica", "asma", "diabetes", "neurologic","pneumopati", "imunodepre",
                   "renal", "obesidade", "tabag", "out_morbi", "vacina_cov", "vacina", "mae_vac", 
                   "m_amamenta", "antiviral", "trat_cov", "hospital", "uti", "amostra", "pos_an_flu", 
                   "pos_an_out", "surto_sg", "co_detec", "vg_reinf"]

float_columns = ["obes_imc"]

df_typed = df_renamed.with_columns([
  (cs.starts_with("dt_") | cs.starts_with("dose_") | cs.starts_with("dos_"))
    .str.to_datetime(strict=False)
    .dt.date(),
  (cs.starts_with("sg_") | cs.starts_with("fab_")).cast(pl.Categorical, strict=False),
  pl.col("nu_idade_n").cast(pl.Int32, strict=False),
  pl.col(float_columns).cast(pl.Float64, strict=False),
  *[pl.when(pl.col(col_name) == "1").then(True)
      .when(pl.col(col_name) == "2").then(False)
      .otherwise(None).cast(pl.Boolean).alias(col_name)
    for col_name in boolean_columns]
])

df_unduplicated = df_typed.unique("nu_notific")

df_sanitized = df_unduplicated.with_columns(cs.string().str.strip_chars().str.to_lowercase())

df_final = df_sanitized.filter(pl.col("hospital") == True)

## Structuring & Modeling

### Todo...

## Enrichment & Integration

### Todo...

##### Load #####

DB_PATH = "data/srag.duckdb"

con = duckdb.connect(database=DB_PATH)
con.register('temp_srag', df_final) 
con.execute("CREATE OR REPLACE TABLE srag AS SELECT * FROM temp_srag")
con.close()