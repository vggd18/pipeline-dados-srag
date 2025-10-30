import polars as pl
import polars.selectors as cs
import duckdb
import os
import logging
import time
from functools import wraps
from config import URL_PATH, DB_DIR, DB_PATH, LOG_LEVEL, LOG_FORMAT, validate_config

validate_config()

def log_step(step_name: str):
    """Decorator para logar início, fim e duração de cada etapa."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logging.info(f"-> {step_name}...")
            start_time = time.time()
            
            result = func(*args, **kwargs)
            
            duration = time.time() - start_time
            logging.info(f"{step_name} concluído ({duration:.2f}s)")
            
            return result
        return wrapper
    return decorator

@log_step("Extraindo dados do S3")
def extract(url: str) -> pl.LazyFrame:
  """
  Extrai um arquivo Parquet do S3 e retorna um Lazyframe
  """
  df_lazy = pl.scan_parquet(url)
  schema = df_lazy.collect_schema()
  logging.debug(f"Schema escaneado: {len(schema)} colunas")

  return df_lazy

@log_step("Padronizando nomes de colunas")
def rename_columns(df: pl.LazyFrame) -> pl.LazyFrame:
  """Padroniza nomes de colunas para snake_case lowercase."""
  col_names = df.collect_schema().names()

  return df.rename({col: col.lower() for col in col_names})

@log_step("Convertendo tipos de dados")
def convert_data_types(df: pl.LazyFrame) -> pl.LazyFrame:
    """Converte colunas para tipos apropriados."""
    boolean_columns = [
        "estrang", "pov_ct", "nosocomial", "febre", "tosse", "garganta", 
        "dispneia", "desc_resp", "saturacao", "diarreia", "vomito", "dor_abd", 
        "fadiga", "perd_olft", "perd_pala", "outro_sin", "fator_risc", 
        "puerpera", "cardiopati", "hematologi", "sind_down", "hepatica", "asma", 
        "diabetes", "neurologic", "pneumopati", "imunodepre", "renal", "obesidade", 
        "out_morbi", "vacina_cov", "vacina", "mae_vac", "antiviral", "trat_cov", 
        "hospital", "uti", "amostra", "surto_sg", "co_detec"
    ]
    float_columns = ["obes_imc"]

    return df.with_columns([
      (cs.starts_with("dt_") | cs.starts_with("dose_") | cs.starts_with("dos_"))
        .str.to_datetime(strict=False).dt.date(),
      (cs.starts_with("sg_") | cs.starts_with("fab_")).cast(pl.Categorical),
      pl.col("nu_idade_n").cast(pl.Int32, strict=False),
      pl.col(float_columns).cast(pl.Float64, strict=False),
      *[pl.when(pl.col(col) == "1").then(True)
          .when(pl.col(col) == "2").then(False)
          .otherwise(None).cast(pl.Boolean).alias(col)
        for col in boolean_columns]
    ])

@log_step("Mapeando códigos categóricos")
def map_categorical_codes(df: pl.LazyFrame) -> pl.LazyFrame:
  """
  Mapeia códigos numéricos para labels legíveis.
  """
  sexo_map = {"1": "masculino", "2": "feminino", "9": None}
  raca_map = {
    "1": "branca", "2": "preta", "3": "amarela", 
    "4": "parda", "5": "indígena", "9": None
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
    "1": "SRAG por influenza", 
    "2": "SRAG por outro vírus respiratório",
    "3": "SRAG por outro agente etiológico", 
    "4": "SRAG não especificado",
    "5": "SRAG por covid-19"
  }

  return df.with_columns([
    pl.col("cs_sexo").replace_strict(sexo_map, default=None).cast(pl.Categorical),
    pl.col("cs_raca").replace_strict(raca_map, default=None).cast(pl.Categorical),
    pl.col("cs_escol_n").replace_strict(escolaridade_map, default=None).cast(pl.Categorical),
    pl.col("evolucao").replace_strict(evolucao_map, default=None).cast(pl.Categorical),
    pl.col("classi_fin").replace_strict(classi_fin_map, default=None).cast(pl.Categorical)
  ])

@log_step("Limpando e deduplicando dados")
def clean_and_deduplicate(df: pl.LazyFrame) -> pl.LazyFrame:
  """Aplica limpeza e deduplicação."""
  return (df
    .unique("nu_notific")
    .with_columns(cs.string().str.strip_chars().str.to_lowercase())
    .filter(pl.col("hospital") == True)
  )


def get_valid_columns(df: pl.LazyFrame, null_threshold: float = 70.0) -> list[str]:
    """
    Identifica colunas válidas baseado em % de nulidade.
    """
    logging.info(f"Analisando nulidade (threshold: {null_threshold}%)...")
    
    stats = df.select([
      pl.all().is_null().sum(),
      pl.len().alias("__total_rows__")
    ]).collect()
    
    total_rows = stats["__total_rows__"][0]
    
    critical_columns = [
      'puerpera', 'hematologi', 'sind_down', 
      'hepatica', 'renal', 'obesidade'
    ]
    
    cols_to_keep = []
    cols_dropped = []
    df_columns = df.collect_schema().names()

    if total_rows == 0:
      logging.warning("DataFrame vazio, mantendo todas as colunas.")
      return df_columns
    
    for col_name in df_columns:
      if col_name == "__total_rows__":
        continue
      
      if col_name in critical_columns:
        cols_to_keep.append(col_name)
        continue
      
      null_count = stats[col_name][0]
      null_pct = (null_count / total_rows) * 100
      
      if null_pct <= null_threshold:
        cols_to_keep.append(col_name)
      else:
        cols_dropped.append(f"{col_name} ({null_pct:.1f}%)")
    
    if cols_dropped:
      logging.warning(
        f"Removidas {len(cols_dropped)} colunas com nulidade >{null_threshold}%: "
        f"{', '.join(cols_dropped[:5])}"
        + (f" (+{len(cols_dropped)-5} outras)" if len(cols_dropped) > 5 else "")
      )
  
    logging.info(f"{len(cols_to_keep)}/{len(df_columns)} colunas mantidas.")
    
    return cols_to_keep

def transform(df_lazy: pl.LazyFrame) -> pl.LazyFrame:
  """
  Pipeline de transformação completo.
  
  Etapas:
  1. Padronização de nomes (snake_case)
  2. Conversão de tipos (dates, booleans, categoricals)
  3. Mapeamento semântico (códigos -> labels)
  4. Deduplicação e limpeza
  5. Remoção de colunas com alta nulidade
  """
  logging.info("=" * 60)
  logging.info("🔄 PIPELINE DE TRANSFORMAÇÕES INICIADO")
  logging.info("=" * 60)

  pipeline_start = time.time()

    
  df_renamed = rename_columns(df_lazy)
  df_typed = convert_data_types(df_renamed)
  df_mapped = map_categorical_codes(df_typed)
  df_cleaned = clean_and_deduplicate(df_mapped)
  
  columns_to_keep = get_valid_columns(df_cleaned, null_threshold=70.0)
  df_final = df_cleaned.select(columns_to_keep)
  
  logging.info("Pipeline de transformações concluído.")

  pipeline_duration = time.time() - pipeline_start
  
  logging.info("=" * 60)
  logging.info(f"PIPELINE CONCLUÍDO EM {pipeline_duration:.2f}s")
  logging.info("=" * 60)

  return df_final

@log_step("Carregando dados no DuckDB")
def load(df_final: pl.LazyFrame, db_path: str, db_dir: str):
  """
  Carrega o lazyframe no banco DuckDB
  """
  logging.info(f"Executando o pipeline em {db_path}")
  os.makedirs(db_dir, exist_ok=True)

  con = duckdb.connect(database=db_path)
  con.register('lazy_srag', df_final)
  con.execute("CREATE OR REPLACE TABLE srag AS SELECT * FROM lazy_srag")

  record_count = con.execute("SELECT COUNT(*) FROM srag").fetchone()[0]
  db_size_mb = os.path.getsize(db_path) / 1024 / 1024

  logging.info(f"   -> {record_count:,} registros carregados")
  logging.info(f"   -> Tamanho do banco: {db_size_mb:.2f} MB")

  con.close()

  logging.info("Carregamento concluído!")

def test_database(db_path: str):
  """
  Executa um teste simples para saber se a tabela foi carregada.
  """
  logging.info("Testando integridade do banco de dados...")

  try:
    con = duckdb.connect(database=db_path)
    count = con.execute("SELECT COUNT(*) FROM srag").fetchone()[0]
    columns = con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'srag'").fetchone()[0]  

    logging.info(f"Tabela 'srag' encontrada")
    logging.info(f"{count:,} registros")
    logging.info(f"{columns} colunas")

    con.close()
  except Exception as e:
    logging.error(f"Erro ao testar o banco de dados: {e}")

def main():
  """
  Orquestrador do Pipeline de Extração, Tratamento e Carregamento (ETL)
  """
  logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
  
  logging.info("PIPELINE ETL SRAG INICIADO")
  logging.info(f"   Fonte: {URL_PATH}")
  logging.info(f"   Destino: {DB_PATH}")
  logging.info("")

  pipeline_start = time.time()

  try:
    df_lazy = extract(URL_PATH)
    df_final = transform(df_lazy)
    load(df_final, DB_PATH, DB_DIR)
    test_database(DB_PATH)
    logging.info("Pipeline ETL concluído com sucesso.")
  except Exception as e:
    logging.critical(f"Pipeline falhou. Erro: {e}", exc_info=True)

  total_duration = time.time() - pipeline_start

  logging.info("")
  logging.info("=" * 60)
  logging.info(f"🎉 PIPELINE ETL CONCLUÍDO COM SUCESSO")
  logging.info(f"⏱️  Tempo total: {total_duration:.2f}s")
  logging.info("=" * 60)

if __name__ == "__main__":
  if not URL_PATH or not DB_PATH:
    logging.critical("Erro: Variáveis SOURCE_URL ou DB_PATH não encontradas no .env ou ambiente.")
    exit(1)

  main()