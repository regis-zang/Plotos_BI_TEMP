# -*- coding: utf-8 -*-
"""
Script: temp.py
Descrição: Este script faz XYZ.
Autor: Regis Zang
Data: DD/MM/AAAA
Versão: 1.0
Licença: MIT
Requisitos: biblioteca_x, biblioteca_y
"""
import mysql.connector
from datetime import datetime

# Conectar ao banco de dados
conn = mysql.connector.connect(
    host='mectsuru.com',
    database='mectsu42_db_catalogo',
    user='mectsu42_dadm',
    password='S3gur@2022'
)
# Inserir uma nova execução de atividade
cursor = conn.cursor()
insert_query = '''
    INSERT INTO execucoes_atividades (id_usuario, codigo_py, status_exe, data_hora_inicio, data_hora_fim, num_linhas)
    VALUES (%s, %s, %s, %s, %s, %s)
'''
id_usuario = 1
codigo_py = 'CadCriaCadCrownfunding.py'
status_exe = 'Exectutado'
data_hora_inicio = datetime.now()
data_hora_fim = datetime.now()
num_linhas = 200
cursor.execute(insert_query, (id_usuario, codigo_py, status_exe, data_hora_inicio, data_hora_fim, num_linhas))

# Commit e fechar conexão
conn.commit()
cursor.close()
conn.close()


