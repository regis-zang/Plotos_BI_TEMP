# -*- coding: utf-8 -*-
"""
Editor Spyder

Este é um arquivo de script temporário.
"""
import pandas as pd
import sqlite3
# Definir as variáveis fornecidas
tbl_destino_carga = 'cvm_cad_crowfunding_adm_resp'

# Dicionário de dados baseado no arquivo meta "meta_cad_crowdfunding"
data_dictionary = {
    'ADM_RESP': {'description': 'Nome do Administrador Responsável', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 100},
    'CNPJ': {'description': 'Cadastro Nacional de Pessoas Jurídicas', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 20}
    # Adicione os demais campos do novo dicionário aqui conforme necessário
}

# Dados de exemplo para demonstração
data = {
    'ADM_RESP': ['Admin1', 'Admin2'],
    'CNPJ': ['12345678901234', '98765432109876']
    # Adicione os demais dados de exemplo de acordo com o novo dicionário de dados
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