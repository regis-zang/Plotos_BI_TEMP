# -*- coding: utf-8 -*-
"""
Fonte de dados:https://dados.cvm.gov.br/dataset/agente_fiduc-cad
Campo: AGENTE_FIDUC - Tipo Dados: varchar - Tamanho   : 100
Campo: DT_CANCEL - Domínio   : AAAA-MM-DD -  Tipo Dados: date - Tamanho   : 10
Campo: DT_INI_SIT - Domínio   : AAAA-MM-DD - Tipo Dados: date - Tamanho   : 10
Campo: DT_REG - Domínio   : AAAA-MM-DD - Tipo Dados: date - Tamanho   : 10
Campo: SIT - Tipo Dados: varchar - Tamanho   : 40

"""
import pandas as pd
import sqlite3
# Definir as variáveis fornecidas
tbl_destino_carga = 'cvm_cad_agente_fiduc_pf'






# Dicionário de dados baseado no arquivo meta "meta_cad_crowdfunding"
data_dictionary = {
    'AGENTE_FIDUC': {'description': 'Agentes Fiduciários', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 100},
    'DT_CANCEL': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'date'},
    'DT_INI_SIT': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'date'},
    'DT_REG': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'date'},  
    'SIT': {'description': 'Nome Comercial', 'domain': 'Alfanumérico', 'data_type': 'varchar', 'size': 40},
    # Adicione os demais campos do novo dicionário aqui conforme necessário
}

# Dados de exemplo para demonstração
data = {
    'AGENTE_FIDUC': ['CARLOS ALBERTO DA ROCHA LIMA', 'JOSÉ EDUARDO MENDES GONÇALVES'],
    'DT_CANCEL': ['2018-01-10', '2018-01-10'], 
    'DT_INI_SIT': ['2018-01-10', '2018-01-10'], 
    'DT_REG': ['2018-01-10', '2018-01-10'], 
    'SIT': ['CANCELADA', 'ATIVO'],
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
#old df.to_sql('cvm_cad_crowfunding_socios', conn, if_exists='replace', index=False)
df.to_sql(tbl_destino_carga, conn, if_exists='replace', index=False)

# Exemplo de consulta para verificar se os dados foram carregados corretamente
cursor = conn.cursor()
# old cursor.execute('SELECT * FROM cvm_cad_crowfunding_socios')
cursor.execute(f"SELECT * FROM {tbl_destino_carga}")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
