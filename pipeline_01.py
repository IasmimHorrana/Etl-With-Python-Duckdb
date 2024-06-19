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

def conectar_banco(): #cria (se não existir) /conecta ao banco de dados duckdb 
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con):
    con.execute('''
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
        )
    ''')


def registrar_arquivo(con, nome_arquivo): #Registra um novo arquivo no db com horario atual
     con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
            VALUES (?, ?)
        """,(nome_arquivo, datetime.now()))
        
def arquivo_processados(con): #Retorna um set com os nomes de todos os arquivos ja processados    
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchal())

        
def baixar_aquivos_drive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True) #criar uma pasta.
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False) #informa a url e ele baixa os arquivos.

#Função para listar os arquivos csv no diretorio especifico.
def listar_arquivos_csv(diretorio):
    arquivo_csv = []
    todos_arquivos = os.listdir(diretorio)
    for arquivo in todos_arquivos:
        if arquivo.endswith(".csv"):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivo_csv.append(caminho_completo)
    return arquivo_csv

#Função para ler um CSV e retornar um DataFrame duckdb.
def ler_csv(caminho_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_arquivo)
    print(dataframe_duckdb)
    print(type(dataframe_duckdb))
    return dataframe_duckdb

# Função para adicionar uma coluna de total de vendas
def transformar(df: DuckDBPyRelation) -> DataFrame:
    #executar a consulta sql que inclui a nova coluna, operando sobre a tabela virtual
    df_transformando = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    #remover o registro da tabela virtual para limpeza
    return df_transformando 

#Função para converter o duckdb em Pandas e salvar dataframe no PostgreeSQL
def salvar_no_postgree(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL") #EX: 'postgreensql://user:password@localhost:5432/data
    engine = create_engine(DATABASE_URL)
    #Salvar o Dataframe no PostgreeSQL
    df_duckdb.to_sql(tabela, con=engine, if_exists = 'append', index=False)

#RUN TIME

if __name__ == "__main__":
    url_pasta = "https://drive.google.com/drive/folders/1ZEHJwuR5Z4qKVx2JZdh1KMOsMqqDObh6"
    diretorio_local = './pasta_gdown'
    # baixar_aquivos_drive(url_pasta, diretorio_local)
    #arquivos = listar_arquivos_csv(diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)
    #ler_csv(arquivos)
    #NOVA FUNÇÃO ABAIXO
    con = conectar_banco()
    inicializar_tabela(con)
    processados = arquivo_processados(con)

    for caminho_do_arquivo in lista_de_arquivos:
        nome_arquivo = os.path.basename(caminho_do_arquivo)
        if nome_arquivo not in processados:
            df = ler_csv(caminho_do_arquivo)
            df_transformado = transformar(df)
            salvar_no_postgree(df_transformado, "vendas_calculado")
            registrar_arquivo(con, nome_arquivo)
            print(f"Arquivo {nome_arquivo} processado e salvo.")
        else:
            print(f"Arquivo {nome_arquivo} já foi processado antes.")

