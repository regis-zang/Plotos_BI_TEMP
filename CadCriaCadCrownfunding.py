# -*- coding: utf-8 -*-
"""
Editor Spyder

Este é um arquivo de script temporário.
"""
import pandas as pd
import sqlite3

# Definir as variáveis fornecidas
tbl_destino_carga = 'cvm_cad_crowfunding'

# Dicionário de dados baseado no arquivo meta "meta_cad_crowdfunding"
data_dictionary = {
    'CNPJ': {'description': 'Cadastro Nacional de Pessoas Jurídicas', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},
    'DENOM_SOCIAL': {'description': 'Nome Social', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'DENOM_COMERC': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 50},
    'DT_REG': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'date'},
    'SIT': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},
    'DT_INI_SIT': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'date'}, 
    'WEBSITE': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 255},
    'EMAIL': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 255},
    'TP_ENDER': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},
    'LOGRADOURO ': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 255},
    'COMPL': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20},   
    'BAIRRO': {'description': 'Bairro', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 100},
    'MUN': {'description': 'Bairro', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 100},  
    'UF': {'description': 'Bairro', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 2},
    'CEP': {'description': 'CEP', 'domain': 'Numérico', 'data_type': 'numeric', 'precision': 8, 'scale': 0},
    'DDD': {'description': 'CEP', 'domain': 'Numérico', 'data_type': 'varchar', 'size': 3},
    'TEL': {'description': 'CEP', 'domain': 'Numérico', 'data_type': 'varchar', 'size': 10}
    # Adicione os demais campos do dicionário aqui
}

# Dados de exemplo para demonstração
data = {
    'BAIRRO': ['Bairro1', 'Bairro2'],
    'CEP': [12345678, 87654321],
    'CNPJ': ['12345678901234', '98765432109876'],
    'DENOM_SOCIAL': ['12345678901234', '98765432109876'],
    'DENOM_COMERC': ['12345678901234', '98765432109876'],
    'DT_REG': ['2018-01-10', '2018-01-10'], 
    'SIT': ['Em Funcionamento Normal', 'Em Funcionamento Normal'],
    'DT_INI_SIT': ['2018-01-10', '2018-01-10'], 
    'WEBSITE': ['https://myfirstipo.com/', 'https://myfirstipo.com/'],
    'EMAIL': ['melito@economiacriativa.com', 'melito@economiacriativa.com'],
    'TP_ENDER': ['12345678901234', '98765432109876'],
    'LOGRADOURO': ['Rua Dr. Guilherme Bannitz, 126', 'Rua Dr. Guilherme Bannitz, 126'],
    'COMPL': ['12345678901234', '98765432109876'],
    'MUN': ['SÃO PAULO', 'SÃO PAULO'],   
    'UF': ['SP', 'SP'],   
    'DDD': ['11', '11'],  
    'TEL': ['123456789', '987654321'],
    # Adicione os demais dados de exemplo de acordo com o dicionário de dados
}

df = pd.DataFrame(data)

# Conexão com o banco de dados SQLite3 e carregamento dos dados
conn = sqlite3.connect('crowfunding.db')

# Criar a tabela no banco de dados com base no dicionário de dados
create_table_query = f"CREATE TABLE IF NOT EXISTS {tbl_destino_carga} ("

for column, props in data_dictionary.items():
    sql_type = props['data_type']
    if props['data_type'] == 'numeric':
        sql_type += f"({props['precision']},{props['scale']})"
    elif props['data_type'] == 'varchar' or props['data_type'] == 'char':
        sql_type += f"({props['size']})"

    create_table_query += f"{column} {sql_type}, "

# Remover a última vírgula e espaço extra
create_table_query = create_table_query[:-2] + ")"
conn.execute(create_table_query)

# Carregar os dados do DataFrame na tabela
df.to_sql(tbl_destino_carga, conn, if_exists='replace', index=False)

# Exemplo de consulta para verificar se os dados foram carregados corretamente
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {tbl_destino_carga}")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
