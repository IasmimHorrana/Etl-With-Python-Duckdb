<h1 align="center">ETL com Python e DuckDB</h1>

<div align="center">
    <img src="img/arquitetura.png" alt="Fluxograma ETL">
</div>


Estudo de pipeline ETL usando Python e DuckDB do zero ao deploy. Quem estiver interessado pelo conteúdo > [Clique para assistir o workshop aberto do Luciano Galvão.](https://www.youtube.com/watch?v=eXXImkz-vMs)


<h1 align="center"> Diagrama do Fluxo</h1>

<div align="center">
    <img src="img/fluxograma.png" alt="Fluxograma ETL">
</div>

<h1 align="center">Funcionalidades</h1>

### Carregar Variáveis de Ambiente

Utiliza a biblioteca python-dotenv para carregar variáveis de ambiente a partir de um arquivo .env.

### Baixar Arquivos do Google Drive
- Função: baixar_aquivos_drive
- Cria um diretório local se ele não existir.
- Utiliza a biblioteca gdown para baixar arquivos de uma pasta no Google Drive para o diretório local especificado.
### Listar Arquivos CSV
- Função: listar_arquivos_csv
- Lista todos os arquivos com extensão .csv dentro de um diretório especificado.
### Ler Arquivo CSV
- Função: ler_csv
- Lê um arquivo CSV e retorna um DataFrame do DuckDB.
- Exibe o conteúdo do DataFrame e o seu tipo para verificação.
### Transformar os Dados
- Função: transformar
- Recebe um DataFrame do DuckDB.
- Executa uma consulta SQL no DuckDB para adicionar uma nova coluna chamada total_vendas, que é o resultado da multiplicação das colunas quantidade e valor.
- Retorna um DataFrame Pandas transformado.
### Salvar no PostgreSQL
- Função: salvar_no_postgree
- Utiliza a biblioteca SQLAlchemy para criar uma conexão com o banco de dados PostgreSQL usando uma URL obtida das variáveis de ambiente.
- Salva o DataFrame transformado no PostgreSQL na tabela especificada, adicionando os dados se a tabela já existir (if_exists='append').

<h1 align="center"> Utilização </h1>

### Pré-requisitos:

- Git
- Python 3.12.1 (recomendado: usar pyenv para gerenciamento de versão)
- Poetry (gerenciador de pacotes e ambiente virtual)
- PostgreSQL
- Variáveis de ambiente configuradas em um arquivo .env

### Instalação das Dependências 
 ```
  $ poetry add gdown streamlit psycopg2-binary python-dotenv sqlalchemy
```

### Passos:

1. **Clone o repositório**
 ```
    $ git clone https://github.com/IasmimHorrana/etl-with-python-duckdb.git
    $ cd etl-with-python-duckdb
```
2. **Crie e ative um ambiente virtual**
 ```
pyenv local 3.12.1
poetry shell
```
3. **Execute o pipeline:**
 ```
python script.py
```
4. **Configurar variáveis de ambiente**
<p> Crie um arquivo .env na raiz do projeto e configure as variáveis de ambiente necessárias. Com base no código fornecido, você precisará da variável DATABASE_URL para conectar ao PostgreSQL. O conteúdo do .env deve ser algo assim: </p>

```
DATABASE_URL=postgresql://user:password@localhost:5432/nome_do_banco
```

### Estrutura do Código
Aqui está uma visão geral do que cada função no script faz:

- conectar_banco(): Conecta ao banco de dados DuckDB.
- inicializar_tabela(con): Cria a tabela historico_arquivos se não existir.
- registrar_arquivo(con, nome_arquivo): Insere um registro na tabela historico_arquivos.
- arquivo_processados(con): Retorna um conjunto de arquivos que já foram processados.
- baixar_aquivos_drive(url_pasta, diretorio_local): Baixa arquivos de uma pasta do Google Drive para um diretório local.
- listar_arquivos_csv(diretorio): Lista todos os arquivos CSV em um diretório.
- ler_csv(caminho_arquivo): Lê um arquivo CSV e retorna um DataFrame DuckDB.
- transformar(df: DuckDBPyRelation) -> DataFrame: Transforma os dados adicionando uma coluna total_vendas.
- salvar_no_postgree(df_duckdb, tabela): Salva o DataFrame transformado no PostgreSQL.
- pipeline(): Orquestra todo o processo ETL.

### Fim.


   



