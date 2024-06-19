import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

from datetime import datetime

load_dotenv()

def conectar_banco():
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con):
    con.execute('''
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
        )
    ''')

def registrar_arquivo(con, nome_arquivo):
     con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
            VALUES (?, ?)
        """, (nome_arquivo, datetime.now()))

def arquivo_processados(con):
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

def baixar_aquivos_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True) 
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos_csv(diretorio):
    arquivo_csv = []
    if os.path.exists(diretorio):
        todos_arquivos = os.listdir(diretorio)
        for arquivo in todos_arquivos:
            if arquivo.endswith(".csv"):
                caminho_completo = os.path.join(diretorio, arquivo)
                arquivo_csv.append(caminho_completo)
    else:
        print(f"Diretório '{diretorio}' não encontrado.")
    return arquivo_csv

def ler_csv(caminho_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_arquivo)
    print(dataframe_duckdb)
    print(type(dataframe_duckdb))
    return dataframe_duckdb

def transformar(df: DuckDBPyRelation) -> DataFrame:
    df_transformando = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    return df_transformando

def salvar_no_postgree(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DATABASE_URL)
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)

def pipeline():
    url_pasta = "https://drive.google.com/drive/folders/1ZEHJwuR5Z4qKVx2JZdh1KMOsMqqDObh6"
    diretorio_local = './pasta_gdown'

    baixar_aquivos_drive(url_pasta, diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)
    con = conectar_banco()
    inicializar_tabela(con)
    processados = arquivo_processados(con)

    logs = []
    for caminho_do_arquivo in lista_de_arquivos:
        nome_arquivo = os.path.basename(caminho_do_arquivo)
        if nome_arquivo not in processados:
            df = ler_csv(caminho_do_arquivo)
            df_transformado = transformar(df)
            salvar_no_postgree(df_transformado, "vendas_calculado")
            registrar_arquivo(con, nome_arquivo)
            print(f"Arquivo {nome_arquivo} processado e salvo.")
            logs.append(f"Arquivo {nome_arquivo} processado e salvo.")
        else:
            print(f"Arquivo {nome_arquivo} já foi processado antes.")
            logs.append(f"Arquivo {nome_arquivo} já foi processado antes.")
    return logs

if __name__ == "__main__":
    logs = pipeline()
    for log in logs:
        print(log)