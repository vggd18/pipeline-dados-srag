import pytest
import duckdb
import logging
from pathlib import Path
from config import DB_PATH, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

@pytest.fixture(scope="module")
def db_connection():
  """
  Conecta ao DuckDB ANTES de rodar os testes e fecha a conexão DEPOIS que todos os testes terminarem.
  """
  logging.info(f"Conectando ao banco de dados: {DB_PATH}")
  if not Path(DB_PATH).exists():
    logging.error(f"Banco {DB_PATH} não encontrado. Execute o ETL primeiro.")
    pytest.skip(f"Banco de dados {DB_PATH} não encontrado.")
  
  con = duckdb.connect(database=str(DB_PATH), read_only=True)
  yield con
  
  logging.info(f"Fechando conexão com o banco.")
  con.close()


def test_primary_key_is_unique(db_connection):
  """Teste de Unicidade: Garante que não há duplicatas na chave primária (nu_notific)."""
  logging.info("Testando: Unicidade da Chave Primária...")
  
  query = "SELECT COUNT(nu_notific) - COUNT(DISTINCT nu_notific) FROM srag"
  duplicates = db_connection.execute(query).fetchone()[0]
  
  assert duplicates == 0, f"Teste de Unicidade falhou: Encontradas {duplicates} duplicatas em nu_notific"
  logging.info("-> Teste de Unicidade: OK")

def test_primary_key_is_not_null(db_connection):
  """Teste de Completude: Garante que a chave primária (nu_notific) não é nula."""
  logging.info("Testando: Completude da Chave Primária (Não Nula)...")
  
  query = "SELECT COUNT(*) FROM srag WHERE nu_notific IS NULL"
  null_count = db_connection.execute(query).fetchone()[0]
  
  assert null_count == 0, f"Teste de Completude (PK) falhou: Encontrados {null_count} nulos em nu_notific"
  logging.info("-> Teste de Completude (PK): OK")

def test_hospital_filter_applied(db_connection):
  """Teste de Validade: Garante que o filtro hospital=True foi aplicado no ETL."""
  logging.info("Testando: Validade do Filtro (hospital = True)...")
  
  query = "SELECT COUNT(*) FROM srag WHERE hospital != TRUE"
  invalid_count = db_connection.execute(query).fetchone()[0]
  
  assert invalid_count == 0, f"Teste de Validade (hospital) falhou: Encontrados {invalid_count} registros onde hospital != True"
  logging.info("-> Teste de Validade (hospital): OK")

def test_evolucao_values_are_valid(db_connection):
  """Teste de Validade: Garante que 'evolucao' contém apenas valores mapeados ou nulos."""
  logging.info("Testando: Validade dos Valores (evolucao)...")
  
  query = """
    SELECT DISTINCT evolucao 
    FROM srag 
    WHERE evolucao NOT IN ('Cura', 'Óbito', 'Óbito por outras causas') 
    AND evolucao IS NOT NULL
  """
  invalid_values = db_connection.execute(query).fetchall()
  
  assert len(invalid_values) == 0, f"Teste de Validade (evolucao) falhou: Valores inválidos encontrados: {invalid_values}"
  logging.info("-> Teste de Validade (evolucao): OK")

def test_classi_fin_values_are_valid(db_connection):
  """Teste de Validade: Garante que 'classi_fin' contém apenas valores mapeados ou nulos."""
  logging.info("Testando: Validade dos Valores (classi_fin)...")
  
  valid_values = [
    'SRAG por influenza',
    'SRAG por outro vírus respiratório',
    'SRAG por outro agente etiológico',
    'SRAG por covid-19',
    'SRAG não especificado'
  ]
  
  placeholders = ','.join(['?' for _ in valid_values])
  
  query = f"""
    SELECT DISTINCT classi_fin 
    FROM srag 
    WHERE classi_fin NOT IN ({placeholders})
    AND classi_fin IS NOT NULL
  """
  invalid_values = db_connection.execute(query, valid_values).fetchall()
  
  assert len(invalid_values) == 0, f"Teste de Validade (classi_fin) falhou: Valores inválidos encontrados: {invalid_values}"
  logging.info("-> Teste de Validade (classi_fin): OK")