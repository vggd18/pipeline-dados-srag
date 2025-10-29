import polars as pl
import polars.selectors as cs
from polars import col
import duckdb
import os
import logging

##### Extract #####
URL_PATH = "https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SRAG/2024/INFLUD24-26-06-2025.parquet"
DB_DIR = "data"
DB_PATH = "data/srag.duckdb"

def extract(url: str) -> pl.LazyFrame:
  """
  Extrai um arquivo Parquet do S3 e retorna um Lazyframe
  """
  logging.info(f"Iniciando extração de {url}...")
  df_lazy = pl.scan_parquet(url)
  logging.info("Extração Finalizada")

  return df_lazy

def transform(df_lazy: pl.LazyFrame) -> pl.LazyFrame:
  """
  Etapa de transformação, limpeza e mapeamento.
  """
  logging.info("Aplicando séries de transformações de dados...")

  ##### Definições da Transformação #####
  boolean_columns = ["estrang", "pov_ct", "nosocomial", "febre", "tosse", "garganta", 
                    "dispneia", "desc_resp", "saturacao", "diarreia", "vomito", "dor_abd", 
                    "fadiga", "perd_olft", "perd_pala", "outro_sin", "fator_risc", 
                    "puerpera", "cardiopati", "hematologi", "sind_down", "hepatica", "asma", 
                    "diabetes", "neurologic","pneumopati", "imunodepre","renal", "obesidade", 
                    "tabag", "out_morbi", "vacina_cov", "vacina", "mae_vac", "m_amamenta", 
                    "antiviral", "trat_cov", "hospital", "uti", "amostra", "pos_an_flu", 
                    "pos_an_out", "surto_sg", "co_detec", "vg_reinf"]
  float_columns = ["obes_imc"]
  sexo_map = {"1": "masculino", "2": "feminino", "9": None}
  raca_map = {"1": "branca", "2": "preta", "3": "amarela", "4": "parda", "5": "indígena", "9": None}
  escolaridade_map = {
    "0": "Sem escolaridade/Analfabeto", "1": "Fundamental 1º ciclo (1ª a 5ª série)",
    "2": "Fundamental 2º ciclo (6ª a 9ª série)", "3": "Médio (1º ao 3º ano)",
    "4": "Superior", "5": "Não se aplica", "9": None
  }
  evolucao_map = {"1": "Cura", "2": "Óbito", "3": "Óbito por outras causas", "9": None}
  classi_fin_map = {
    "1":"SRAG por influenza", "2":"SRAG por outro vírus respiratório" ,
    "3":"SRAG por outro agente  etiológico", "4":"SRAG não especificado",
    "5":"SRAG por covid-19"
  }

  ##### Lógica da Transformação #####
  col_names = df_lazy.collect_schema().names()
  df_final_lazy = df_lazy.rename({ col: col.lower() for col in col_names}) \
    .with_columns([
      (cs.starts_with("dt_") | cs.starts_with("dose_") | cs.starts_with("dos_")).str.to_datetime(strict=False).dt.date(),
      (cs.starts_with("sg_") | cs.starts_with("fab_")).cast(pl.Categorical),
      pl.col("cs_sexo").replace_strict(sexo_map, default=None).cast(pl.Categorical),
      pl.col("cs_raca").replace_strict(raca_map, default=None).cast(pl.Categorical),
      pl.col("cs_escol_n").replace_strict(escolaridade_map, default=None).cast(pl.Categorical),
      pl.col("evolucao").replace_strict(evolucao_map, default=None).cast(pl.Categorical),
      pl.col("classi_fin").replace_strict(classi_fin_map, default=None).cast(pl.Categorical),
      pl.col("nu_idade_n").cast(pl.Int32, strict=False),
      pl.col(float_columns).cast(pl.Float64, strict=False),
      *[pl.when(pl.col(col_name) == "1").then(True)
          .when(pl.col(col_name) == "2").then(False)
          .otherwise(None).cast(pl.Boolean).alias(col_name)
      for col_name in boolean_columns]
    ]) \
    .unique("nu_notific") \
    .with_columns(cs.string().str.strip_chars().str.to_lowercase()) \
    .filter(pl.col("hospital") == True)

  logging.info("Transformações aplicadas.")

  return df_final_lazy

def load(df_final_lazy: pl.LazyFrame, db_path: str, db_dir: str):
  """
  Carrega o lazyframe no banco DuckDB
  """
  logging.info(f"Executando o pipeline em {db_path}")
  os.makedirs(db_dir, exist_ok=True)

  con = duckdb.connect(database=db_path)
  con.register('lazy_srag', df_final_lazy)
  con.execute("CREATE OR REPLACE TABLE srag AS SELECT * FROM lazy_srag")
  con.close()

  logging.info("Carregamento concluído!")


def main():
  """
  Orquestrador do Pipeline de Extração, Tratamento e Carregamento (ETL)
  """
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
  
  logging.info("Pipeline ETL iniciado.")

  df_lazy = extract(URL_PATH)
  df_final_lazy = transform(df_lazy)
  load(df_final_lazy, DB_PATH, DB_DIR)
  logging.info("Pipeline ETL concluído com sucesso.")

if __name__ == "__main__":
  main()