
import calendar
import json
import pandas as pd
import psycopg2
import requests
import config
import smtplib

from datetime import date, datetime, time, timedelta

#datos fecha para crear
date_format = "%Y-%m-%d %H:%M:%S.0"
now = datetime.today().date()
#fin datos fecha para crear
conexion_sgr = psycopg2.connect(host=config.host,database=config.database_sgr,user=config.user_sgr,password=config.password_sgr)
base_schedule = conexion_sgr.cursor()

#Inicia reservas
records_update_init = conexion_sgr.cursor()
records_update_init.execute("UPDATE schedule_schedule SET state = 'Iniciada' WHERE reservation_init = now and state = 'Aceptada'")
#Finaliza reservas
records_update_final = conexion_sgr.cursor()
records_update_final.execute("UPDATE schedule_schedule SET state = 'Finalizada' WHERE reservation_fin < now")
conexion_sgr.commit()   
conexion_sgr.close()


