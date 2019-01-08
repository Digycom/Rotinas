'''
'''

import psycopg2
import os
import datetime
from xml.dom import minidom
import time

def create_log():
    now = datetime.datetime.now()
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)

    if len(month) == 1:
        month = '0' + month
    if len(day) == 1:
        day = '0' + day

    hour = str(now.hour)
    minute = str(now.minute)
    file_name = r'C:/log_restore/LOG_' + day + '-' + month + '-' + year + '--' + hour + '-' + minute + '.txt'
    file = open(file_name, 'w+')

    return file

def create_connection():
    try:
        conn = psycopg2.connect(host="localhost", database="", port="5432", user="postgres", password="159357")
    except:
        print("Falha ao conectar com o database")
    return conn

def drop_database(create_connection, dbname):
    print("Deletando o banco de dados " + dbname + "...")
    conn = create_connection()
    
    query = """DROP DATABASE IF EXISTS """ + dbname + """;"""
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(query)
    conn.close()
    print("Banco de dados deletado.")

def create_database(create_connection, dbname):
    print("Criando o banco de dados " + dbname + "...")
    conn = create_connection()
    
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    query = """CREATE DATABASE """ + dbname + """ WITH ENCODING='UTF8' OWNER=postgres;"""
    cur = conn.cursor()
    cur.execute(query)
    conn.close()
    print("Banco de dados criado.")
    
def restore_database(dbname, backup_file_name):
    print("Restaurando o backup do arquivo " + backup_file_name + "...")
    cmd = """pg_restore.exe --host localhost --port 5432 --username postgres --dbname """ + dbname + """ D:/BKP_TOPCAR/""" + backup_file_name
    returned_value = os.system(cmd)
    print('returned value:', returned_value)

def prepare_backup_name(dbname):
    now = datetime.datetime.now()
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)

    if len(month) == 1:
        month = '0' + month
    if len(day) == 1:
        day = '0' + day

    name = dbname + "__" + year + "_" + month + "_" + day + ".backup"

    return name

print("Iniciando o processo")

# Passando o arquivo xml por nome
myxml = minidom.parse('all_db.xml')
dbname = myxml.getElementsByTagName('dbname')

# Chamando a função que cria o arquivo de log
file = create_log()

print("Iniciando rotina de Restore")
for elem in dbname:
    print("=================================================")
    print("Iniciando operação no banco " + elem.attributes['name'].value)

    start = time.time()

    dbname = str(elem.attributes['name'].value)
    backup_file_name = prepare_backup_name(dbname)

    # Iniciando as rotinas
    drop_database(create_connection, dbname)
    create_database(create_connection, dbname)
    restore_database(dbname, backup_file_name)

    end = time.time()

    file.write("Operação do banco " + elem.attributes['name'].value + " finalizada em " + str(end - start) + "\n")
    print("Operação finalizada em " + str(end - start))