# -*- coding: utf-8 -*-
"""
Created on Fri May 10 19:14:55 2024

@author: celre
"""
import pandas as pd
import sqlite3

# Definir as variáveis fornecidas
var_folder_name = 'cad_crowdfunding'
diretorio = r"C:\Fiap\Proj_Python_IA\BaseCVM\Crowdfunding\cad_crowdfunding"
nome_arquivo_csv = 'cad_crowdfunding_socios.csv'
tbl_destino_carga = 'cvm_cad_crowfunding_socios'
tbl_destino_carga = 'cvm_cad_crowfunding_socios'

# Caminho completo do arquivo CSV
caminho_arquivo_csv = f"{diretorio}\\{nome_arquivo_csv}"

# Carregar os dados do arquivo CSV com encoding latin1
df_csv = pd.read_csv(caminho_arquivo_csv, delimiter=';', encoding='latin1')

# Conexão com o banco de dados SQLite3
conn = sqlite3.connect('crowfunding.db')

# Carregar os dados do DataFrame do arquivo CSV na tabela 'cvm_cad_crowfunding'
# old df_csv.to_sql('cvm_cad_crowfunding_socios', conn, if_exists='replace', index=False)
df_csv.to_sql(tbl_destino_carga, conn, if_exists='replace', index=False)
# Exemplo de consulta para verificar se os dados foram carregados corretamente
cursor = conn.cursor()
# old cursor.execute('SELECT * FROM cvm_cad_crowfunding_socios')
cursor.execute(f"SELECT * FROM {tbl_destino_carga}")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()


