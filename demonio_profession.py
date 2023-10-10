
import calendar
import json
import pandas as pd
import psycopg2
import requests
import config
import smtplib

from datetime import date, datetime, time, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#datos fecha para crear
date_format = "%Y-%m-%d %H:%M:%S.0"
now = datetime.today()
#fin datos fecha para crear
conexion_scc = psycopg2.connect(host=config.host,database=config.database_scc,user=config.user_scc,password=config.password_scc)
base_carrera = conexion_scc.cursor()

#Datos tabla base estudiante
base_carrera.execute("SELECT facultad,nombre_carrera,codigo_carrera FROM base_carrera")
row_base_carrera = base_carrera.fetchone()
acc = 0
while row_base_carrera is not None:
    faculty_name = row_base_carrera[0]
    profession_name = row_base_carrera[1]
    profession_code = row_base_carrera[2]
    profession_state = 'Activa'
    #SCC
    #comprobamos si la carrera existe en SCC
    scc_profession = conexion_scc.cursor()
    scc_profession.execute("SELECT count(*) FROM profession_profession WHERE profession_name = %s AND profession_code = %s",(profession_name,profession_code,))
    scc_profession_count = scc_profession.fetchone()
    scc_profession_count_conv = int(''.join(map(str, scc_profession_count))) #convierto la tupla en un numero
    if scc_profession_count_conv <= 0:
        profession_save = conexion_scc.cursor()
        profession_save.execute("INSERT INTO profession_profession(profession_name,profession_code,state,created,updated,faculty_name) VALUES(%s,%s,%s,%s,%s,%s)",(profession_name,profession_code,profession_state,now,now,faculty_name))
        conexion_scc.commit()   
        acc += 1     
    #SGR
    #SGT
 
    row_base_carrera = base_carrera.fetchone()

#dejo todos los registros anteriors bloqueados
records_update = conexion_scc.cursor()
records_update.execute("UPDATE administrator_records SET records_state = 'Bloqueado' WHERE records_type = 'Profession'")
conexion_scc.commit()   
#inserto registro
records_save = conexion_scc.cursor()
records_save.execute("INSERT INTO administrator_records(records_numbers,records_type,records_state,created,updated) VALUES(%s,%s,%s,%s,%s)",(acc,'Profession','Activo',now,now))
conexion_scc.commit()   
conexion_scc.close()


