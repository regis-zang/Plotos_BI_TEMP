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
from datetime import date, datetime

# Conectar ao banco de dados
conn = mysql.connector.connect(
    host='mectsuru.com',
    database='mectsu42_db_catalogo',
    user='mectsu42_dadm',
    password='S3gur@2022'
)

# Inserir um novo usuário
cursor = conn.cursor()
insert_query = '''
    INSERT INTO usuarios (usuario, senha, status, ultima_execucao, ultimo_acesso)
    VALUES (%s, %s, %s, %s, %s)
'''
usuario = 'usrJc'
senha = 'P@ssw0rd!'
status = 'ativo'
ultima_execucao = date.today()
ultimo_acesso = datetime.now()
cursor.execute(insert_query, (usuario, senha, status, ultima_execucao, ultimo_acesso))

# Commit e fechar conexão
conn.commit()
cursor.close()
conn.close()

