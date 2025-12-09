import config
import psycopg2
from datetime import date, datetime, time, timedelta

#datos fecha para revisar
date_format = "%Y-%m-%d %H:%M:%S.0"
now = datetime.today().date()
now_str = now.strftime('%Y-%m-%d')
#fin datos fecha para revisar

#inicia conexion
conexion_sgt = psycopg2.connect(host=config.host,database=config.database_sgt,user=config.user_sgt,password=config.password_sgt)
#fin conexion
#comprobamos si hay pasajeros en estado Viaje no iniciado
find_state = 'Viaje no iniciado'
list_no_valid_count = conexion_sgt.cursor()
sql = """
    SELECT COUNT(*)
    FROM trip_passenger
    WHERE state = %s
      AND travels_id IN (
          SELECT id FROM trip_travels WHERE date = %s
      )
    """
list_no_valid_count.execute(sql,(find_state,now_str))
list_no_valid_count_ = list_no_valid_count.fetchone()
list_no_valid_count_conv = int(''.join(map(str, list_no_valid_count_))) #convierto la tupla en un numero
if list_no_valid_count_conv >= 0:
    sql_list = """
        SELECT id,student_rut,student_name,student_profession,destination,state,student_id,travels_id
        FROM trip_passenger
        WHERE state = %s
        AND travels_id IN (
            SELECT id FROM trip_travels WHERE date = %s
        )    
    """
    list_no_valid = conexion_sgt.cursor()
    list_no_valid.execute(sql_list,(find_state,now_str))   
    row_list_no_valid = list_no_valid.fetchone() 
    while row_list_no_valid is not None:    
        #data travel
        sql_travel = """
            SELECT id,company_id,service_id,name,type_trip,date,departure
            FROM trip_travels
            WHERE id = %s  
        """        
        data_travel = conexion_sgt.cursor()
        data_travel.execute(sql_travel,(row_list_no_valid[7],)) 
        data_travel_find = data_travel.fetchone() 
        #data student
        sql_student = """
            SELECT id,user_id,rut,name,last_name,campus,condition,profession
            FROM student_student
            WHERE id = %s  
        """        
        data_student = conexion_sgt.cursor()
        data_student.execute(sql_student,(row_list_no_valid[6],)) 
        data_student_find = data_student.fetchone() 
        #data user
        sql_user = """
            SELECT first_name,last_name,email
            FROM auth_user
            WHERE id = %s  
        """        
        data_user = conexion_sgt.cursor()
        data_user.execute(sql_user,(data_student_find[1],)) 
        data_user_find = data_user.fetchone() 
        if data_travel_find[4] == 'IDA':
            #data company
            sql_company = """
                SELECT id,name,rut,email,no_valid
                FROM company_Company
                WHERE id = %s  
            """        
            data_company = conexion_sgt.cursor()
            data_company.execute(sql_company,(data_travel_find[1],)) 
            data_company_find = data_company.fetchone()     
            if data_company_find[4] == 1:
                #inserto registro en Novalid
                no_valid_save = conexion_sgt.cursor()
                no_valid_save.execute("INSERT INTO trip_novalid(travel_id,student_id,date,departure,created,updated) VALUES(%s,%s,%s,%s,%s,%s)",(row_list_no_valid[7],row_list_no_valid[6],now,data_travel_find[6],now,now))
                conexion_sgt.commit()                   
        row_list_no_valid = list_no_valid.fetchone() 
conexion_sgt.close()


