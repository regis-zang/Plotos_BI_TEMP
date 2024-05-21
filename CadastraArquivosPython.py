# -*- coding: utf-8 -*-
"""
Created on Tue May 21 19:05:54 2024

@author: celre
"""
import mysql.connector
import os
from datetime import datetime

# Conectar ao banco de dados
conn = mysql.connector.connect(
    host='mectsuru.com',
    database='mectsu42_db_catalogo',
    user='mectsu42_dadm',
    password='S3gur@2022'
)
cursor = conn.cursor()

# Conectar ao banco de dados (já mostrado anteriormente)

# Inserir informações de um arquivo Python na tabela arquivos_python
insert_query = '''
    INSERT INTO arquivos_python (id_categoria, codigo_py, data_criacao, data_ultima_alteracao)
    VALUES (%s, %s, %s, %s)
'''
"""
Campo Categoria é para classificar motivos do codigo:
    
"""

id_categoria = 5
codigo_py = 'ETL_Carga_DadosCadCrowfunding.py'
data_criacao = datetime.fromtimestamp(os.path.getctime(codigo_py))
data_ultima_alteracao = datetime.fromtimestamp(os.path.getmtime(codigo_py))
cursor.execute(insert_query, (id_categoria, codigo_py, data_criacao, data_ultima_alteracao))

# Inserir informações de fonte objetivo e execução de código
insert_fonte_query = '''
    INSERT INTO fonte_objetivo (fonte, url_origem, destino_pasta)
    VALUES (%s, %s, %s)
'''
fonte = 'Base CVM'
url_origem = 'https://dados.cvm.gov.br/dataset/crowdfunding-cad'
destino_pasta = '/caminho/para/pasta'
cursor.execute(insert_fonte_query, (fonte, url_origem, destino_pasta))

insert_execucao_query = '''
    INSERT INTO execucao_codigo (id_proximo_codigo, tipo_execucao, status_codigo)
    VALUES (%s, %s, %s)
'''
id_proximo_codigo = 123
tipo_execucao = 'Sequence'
status_codigo = 'Ativo'
cursor.execute(insert_execucao_query, (id_proximo_codigo, tipo_execucao, status_codigo))


# Commit e fechar conexão
conn.commit()
cursor.close()
conn.close()
