
import calendar
import json
from datetime import date, datetime, time, timedelta
from math import asin, cos, radians, sin, sqrt

import psycopg2
import requests

import config

#confuguramos id de empresa
param_cia = config.param_perez_lillo
token = config.token_perez_lillo
username = config.username_perez_lillo
password = config.pass_perez_lillo

#definimos fecha y hora a consultar

date_format = "%Y-%m-%d %H:%M:%S.0"
time_format = "%H:%M:%S"
now = datetime.today() - timedelta(seconds=60)
now_save = now
since_time = now.strftime(time_format) 
now = datetime.today().date() 
since_date = now.strftime(date_format) 

#Fin definimos fecha y hora a consultar
conexion = psycopg2.connect(host=config.host,database=config.database_sgt,user=config.user_sgt,password=config.password_sgt)
transport = conexion.cursor()
transport.execute("SELECT date,departure,state,company_id,travels_id,vehicle_id,patent,driver_id,latitude,length,next_geo_register,id,state FROM transport_transport WHERE date = 'now' AND company_id = %s AND (state='Viaje no iniciado' OR state='En viaje')",(param_cia,))
#Recorremos los resultados y los mostramos
#test endpoint

#patent = 'JRKP65' #cambiar
#url = 'http://192.168.0.1/webservice?token='+token+'&user='+username+'&pass='+password+'&vehicle_no='+patent+'&format=json'
#url = 'http://18.229.217.73/webservice?token='+token+'&user='+username+'&pass='+password+'&vehicle_no='+patent+'&format=json'
#response = requests.post(url)
#print(response)
#if response.status_code == 200:
#    data_decode = response.content.decode('ISO-8859-1')
#    data = json.loads(data_decode)
#    longitude = data['root']['VehicleData'][0]['Longitude']
#    Latitude = data['root']['VehicleData'][0]['Latitude']
#fin test endpoint

row = transport.fetchone()
while row is not None:
    process = 0
    departure = datetime(2000, 1, 1,hour=row[1].hour, minute=row[1].minute, second=row[1].second) 
    factor_before = (timedelta(seconds=3600))
    factor_after = (timedelta(seconds=14400))
    departure_before = departure - factor_before
    departure_after = departure + factor_after
    if now_save.time() >= departure_before.time() and now_save.time() <= departure_after.time():
        process = 1
    if process == 1:
        vehicle_id = row[5]
        driver_id = row[7]
        travels_id = row[4]
        vehicle = conexion.cursor()
        vehicle.execute("SELECT patent FROM vehicle_vehicle WHERE vehicle_vehicle.id=%s",(vehicle_id,))
        row_v = vehicle.fetchone()  
        a = 0
        while row_v is not None:
            patent = str(row_v[0])
            row_v = vehicle.fetchone()
        #url = 'http://192.168.0.1/webservice?token='+token+'&user='+username+'&pass='+password+'&vehicle_no='+patent+'&format=json'
        url = 'http://18.229.217.73/webservice?token='+token+'&user='+username+'&pass='+password+'&vehicle_no='+patent+'&format=json'
        response = requests.post(url)
        if response.status_code == 200:
            data_decode = response.content.decode('ISO-8859-1')
            data = json.loads(data_decode)
            length = data['root']['VehicleData'][0]['Longitude']
            latitude = data['root']['VehicleData'][0]['Latitude']   
            gps_save = conexion.cursor()
            gps_save.execute("INSERT INTO trip_gps(latitude,length,created,updated,company_id,driver_id,travels_id,vehicle_id,state) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",(latitude,length,now_save,now_save,param_cia,driver_id,travels_id,vehicle_id,row[12]))
            conexion.commit()
            #inicio geocerca        
            length_place = row[9]
            latitude_place = row[8]
            lon1, lat1, lon2, lat2 = map(radians, [length, latitude, length_place, latitude_place]) 
            dlon = lon2 - lon1 
            dlat = lat2 - lat1 
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
            c = 2 * asin(sqrt(a)) 
            # Radius of earth in kilometers is 6371 
            km = 6371 * c 
            if row[10] == 'No' and km <= 0.5:
                transport_update = conexion.cursor()
                transport_update.execute("UPDATE transport_transport SET next_geo_register = %s, next_geo_time = %s, next_geo_difference = %s WHERE id = %s",('Si',since_time,km,row[11]))
                conexion.commit()
            #Fin geocerca        
        else:
            print('Hubo un error')
    row = transport.fetchone()
conexion.close()


