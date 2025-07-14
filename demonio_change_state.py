
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
now = datetime.today()
#fin datos fecha para crear
conexion_sgr = psycopg2.connect(host=config.host,database=config.database_sgr,user=config.user_sgr,password=config.password_sgr)
base_schedule = conexion_sgr.cursor()

#dejo todos los registros anteriors a la fecha actual como Finalizada
records_update = conexion_sgr.cursor()
records_update.execute("UPDATE schedule_schedule SET state = 'Finalizada' WHERE reservation_fin < '2025-07-14'")
conexion_sgr.commit()   
conexion_sgr.close()


