# -*- coding: utf-8 -*-
"""
Created on Wed May 15 20:59:48 2024
mectsu42_abacus
host = 'mectsuru.com'
host = '108.167.188.170"
	108.167.188.170
@author: celre
"""
import mysql.connector

# Informações de conexão
host = "108.167.188.170"
database = "mectsu42_abacus"
user = "mectsu42_abadm"
password = "banco@2075"

# Conectar ao banco de dados
try:
    conn = mysql.connector.connect(host=host, database=database, user=user, password=password)
    if conn.is_connected():
        print('Conexão bem-sucedida ao banco de dados MySQL')
        
    # Exemplo de consulta
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM tabela_dummy')
    for row in cursor.fetchall():
        print(row)

except mysql.connector.Error as e:
    print(f'Erro ao conectar ao banco de dados MySQL: {e}')

finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
        print('Conexão ao banco de dados MySQL encerrada')