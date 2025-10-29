import duckdb
import logging
import sys
import os

DB_PATH = "data/srag.duckdb"
SQL_DIR = "sql"
QUERY_FILES = ["consulta_1.sql", "consulta_2.sql"]

def run_query(con: duckdb.DuckDBPyConnection, sql_file: str):
    """
    Lê um arquivo .sql, executa a consulta e imprime o resultado.
    """
    logging.info(f"Executando {sql_file}...")
    file_path = os.path.join(SQL_DIR, sql_file)
    try:
        with open(file_path, 'r') as f:
            sql_query = f.read()
            logging.info(f"SQL:\n{sql_query}\n")
            
            result = con.execute(sql_query).pl() 
            
            print(f"Resultado de {sql_file}:")
            print(result)
            print("-" * (len(sql_file) + 20))

    except FileNotFoundError:
        logging.error(f"ERRO: Arquivo SQL não encontrado: {file_path}")
    except Exception as e:
        logging.error(f"ERRO ao executar {sql_file}: {e}")

def main():
    """
    Conecta ao banco e executa as consultas.
    """
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    if not os.path.exists(DB_PATH):
        logging.error(f"ERRO: Banco de dados não encontrado em {DB_PATH}.")
        logging.error("Por favor, execute o pipeline ETL (python etl_pipeline.py) primeiro.")
        sys.exit(1)

    try:
        con = duckdb.connect(database=DB_PATH, read_only=True)
        logging.info(f"Conectado a {DB_PATH}")
        
        for sql_file in QUERY_FILES:
            run_query(con, sql_file)
            
        con.close()
        logging.info("Todas as consultas foram executadas.")
        
    except Exception as e:
        logging.critical(f"Falha ao conectar ou executar consultas: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()