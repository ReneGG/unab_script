
import calendar
import json
import pandas as pd
import psycopg2
import requests
import config
from hashlib import md5
from datetime import date, datetime, time, timedelta
from django.utils.crypto import get_random_string

conexion_scc = psycopg2.connect(host=config.host,database=config.database_scc,user=config.user_scc,password=config.password_scc)


#datos fecha para crear
date_format = "%Y-%m-%d %H:%M:%S.0"
now = datetime.today()
other_now = datetime(2019, 1, 1, 0, 0, 00, 00000)
#fin datos fecha para crear

year_current_str = str(now.year)
month_current = int(now.month)
if month_current >= 1 and month_current <=6:
    month_current_str = '10'
if month_current >= 7 and month_current <=12:
    month_current_str = '20'
period_current = int(year_current_str+month_current_str)


def format_rut(rut):
    dv= rut[len(rut)-1]
    body = rut[0:len(rut)-1]
    rut_f = body+'-'+dv
    return rut_f

def str_split(str_data):
    return str_data.split(',')
def no_duplicate_str(data):
    df = pd.DataFrame(data,columns=['Data'])
    return df.drop_duplicates().reset_index(drop=True)
def on_generate_mail_df(c1,c2,c3):
    mail = []
    for m in str_split(c1):
        mail.append(m)
    for m in str_split(c2):
        mail.append(m)
    for m in str_split(c3):
        mail.append(m)
    df_mail = no_duplicate_str(mail)
    return df_mail
def on_generate_phone_df(phone_data):
    phone = []
    for p in str_split(phone_data):
        phone.append(p)
    df_phone = no_duplicate_str(phone)
    return df_phone

def add_user(now,estudiante_rut_format,estudiante_nombre,estudiante_apellido,estudiante_mail1):
    try:
        password = get_random_string(length=32)
        #si no existe como estudiante se crea
        student_user_save = conexion_scc.cursor()
        student_user_save.execute("INSERT INTO auth_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(password,now,'f',estudiante_rut_format,estudiante_nombre,estudiante_apellido,estudiante_mail1,'f','t',now))
        conexion_scc.commit() 
        return 1
    except:
        return -1

def add_profile_student(user_id):
    try:
        student_profile_save = conexion_scc.cursor()
        student_profile_save.execute("INSERT INTO registration_profile(first_session,group_id,user_id) VALUES(%s,%s,%s)",('Si',3,user_id))
        conexion_scc.commit()      
        return 1
    except:
        return -1

def add_student(user_id,estudiante_carrera_id,estudiante_rut_format,estudiante_nombre,estudiante_apellido,estudiante_mail1,estudiante_mail2,estudiante_mail3,estudiante_mail4,estudiante_phone1,estudiante_phone2,estudiante_phone3,estudiante_genero,estudiante_sede,estudiante_tipo,estudiante_nivel,estudiante_periodo_ingreso,student_path_image,estudiante_region,estudiante_comuna,estudiante_estado,student_credential_state,now):
    try:
        student_save = conexion_scc.cursor()
        student_save.execute("INSERT INTO student_student(user_id,group_id,profesion_id,student_rut,student_first_name,student_last_name,student_mail1,student_mail2,student_mail3,student_mail4,student_fono1,student_fono2,student_fono3,student_gender,student_sede,student_kind,student_level,student_period,student_path_image,student_city,student_commune,student_state,student_credential_state,created,updated) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(user_id,3,estudiante_carrera_id,estudiante_rut_format,estudiante_nombre,estudiante_apellido,estudiante_mail1,estudiante_mail2,estudiante_mail3,estudiante_mail4,estudiante_phone1,estudiante_phone2,estudiante_phone3,estudiante_genero,estudiante_sede,estudiante_tipo,estudiante_nivel,estudiante_periodo_ingreso,student_path_image,estudiante_region,estudiante_comuna,estudiante_estado,student_credential_state,now,now))
        conexion_scc.commit()  
    except:
        return -1

def add_credential(student_id,credential_state,now,other_now,year_delivery):
    try:    
        credential_save = conexion_scc.cursor()
        credential_save.execute("INSERT INTO credential_credential(student_id,credential_state,credential_date_requested,credential_date_created,credential_date_delivery,credential_year2_delivery,credential_user_block_id,credential_date_block,created,updated) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(student_id,credential_state,other_now,other_now,other_now,year_delivery,0,other_now,now,now))
        conexion_scc.commit()  
    except:
        return -1

def consulta_id(estudiante_rut_format):
    try:
        scc_estudiante_user_id = conexion_scc.cursor()
        scc_estudiante_user_id.execute("SELECT id FROM auth_user WHERE username = %s",(estudiante_rut_format,))
        scc_estudiante_user_id_id = scc_estudiante_user_id.fetchone()
        user_id = int(''.join(map(str, scc_estudiante_user_id_id))) #convierto la tupla en un numero
        return user_id
    except: 
        return -1


#Datos tabla base estudiante
base_estudiante = conexion_scc.cursor()
base_estudiante.execute("SELECT estudiante_estado,estudiante_periodo_ingreso,estudiante_nivel,estudiante_rut,estudiante_nombre,estudiante_apellido,estudiante_genero,estudiante_correo_1,estudiante_correo_2,estudiante_correo_3,estudiante_fono,estudiante_comuna,estudiante_region,estudiante_carrera,estudiante_codigo_carrera,estudiante_sede,estudiante_tipo FROM base_estudiante")
row_base_estudiante = base_estudiante.fetchone()
acc = 0
non_created_all = 0
non_created_profile = 0
non_created_student = 0
non_created_credential = 0
while row_base_estudiante is not None:
    estudiante_estado = 'INACTIVO'#row_base_estudiante[0]
    estudiante_periodo_ingreso = row_base_estudiante[1]
    estudiante_nivel = 0#row_base_estudiante[2]
    estudiante_rut = row_base_estudiante[3]
    estudiante_nombre = row_base_estudiante[4]
    estudiante_apellido = row_base_estudiante[5]
    estudiante_genero = row_base_estudiante[6]
    estudiante_correo_1 = row_base_estudiante[7]
    estudiante_correo_2 = row_base_estudiante[8]
    estudiante_correo_3 = row_base_estudiante[9]
    estudiante_fono = row_base_estudiante[10]
    estudiante_comuna = row_base_estudiante[11]
    estudiante_region = row_base_estudiante[12]
    estudiante_carrera = row_base_estudiante[13]
    estudiante_codigo_carrera = row_base_estudiante[14]
    estudiante_sede_origin = row_base_estudiante[15]
    estudiante_sede = 'ERROR'
    if estudiante_sede_origin == 'REPÚBLICA' or estudiante_sede_origin == 'CASONA LAS CONDES':
        estudiante_sede = 'REGIÓN METROPOLITANA DE SANTIAGO'
    if estudiante_sede_origin == 'VIÑA DEL MAR':
        estudiante_sede = 'REGIÓN DE VALPARAÍSO'
    if estudiante_sede_origin == 'CONCEPCIÓN':
        estudiante_sede = 'REGIÓN DEL BIOBÍO'



    estudiante_tipo = row_base_estudiante[16]
    #print(estudiante_periodo_ingreso)
    '''
    print(
    'estudiante_estado =', row_base_estudiante[0],'\n',
    'estudiante_periodo_ingreso =', row_base_estudiante[1],'\n',
    'estudiante_nivel =', row_base_estudiante[2],'\n',
    'estudiante_rut =', row_base_estudiante[3],'\n',
    'estudiante_nombre =', row_base_estudiante[4],'\n',
    'estudiante_apellido =', row_base_estudiante[5],'\n',
    'estudiante_genero =', row_base_estudiante[6],'\n',
    'estudiante_correo_1 =', row_base_estudiante[7],'\n',
    'estudiante_correo_2 =', row_base_estudiante[8],'\n',
    'estudiante_correo_3 =', row_base_estudiante[9],'\n',
    'estudiante_fono =', row_base_estudiante[10],'\n',
    'estudiante_comuna =', row_base_estudiante[11],'\n',
    'estudiante_region =', row_base_estudiante[12],'\n',
    'estudiante_carrera =', row_base_estudiante[13],'\n',
    'estudiante_codigo_carrera =', row_base_estudiante[14],'\n',
    'estudiante_sede =', row_base_estudiante[15],'\n',
    'estudiante_tipo =', row_base_estudiante[16],'\n',
    )
    '''
    estudiante_rut_format = format_rut(estudiante_rut)
    df_mail = on_generate_mail_df(estudiante_correo_1.upper(),estudiante_correo_2.upper(),estudiante_correo_3.upper())        
    try:
        estudiante_mail1= df_mail.loc[0]['Data']
    except:
        estudiante_mail1 = 'no mail'
    try:
        estudiante_mail2= df_mail.loc[1]['Data']
    except:
        estudiante_mail2 = 'no mail'
    try:
        estudiante_mail3= df_mail.loc[2]['Data']
    except:
        estudiante_mail3 = 'no mail'        
    try:
        estudiante_mail4= df_mail.loc[3]['Data']
    except:
        estudiante_mail4 = 'no mail'
    try:
        df_phone = on_generate_phone_df(estudiante_fono)        
        try:
            estudiante_phone1= df_phone.loc[0]['Data']
        except:
            estudiante_phone1 = 'no phone'
        try:
            estudiante_phone2= df_phone.loc[1]['Data']
        except:
            estudiante_phone2 = 'no phone'
        try:
            estudiante_phone3= df_phone.loc[2]['Data']
        except:
            estudiante_phone3 = 'no phone'      
    except:
        estudiante_phone1 = 'no phone'
        estudiante_phone2 = 'no phone'
        estudiante_phone3 = 'no phone'
    #buscamos el id de carrera si no existe se agregará 0
    scc_estudiante_carrera = conexion_scc.cursor()
    scc_estudiante_carrera.execute("SELECT count(*) FROM profession_profession WHERE profession_code = %s",(estudiante_codigo_carrera,))
    scc_estudiante_carrera_count = scc_estudiante_carrera.fetchone()
    scc_estudiante_carrera_count_conv = int(''.join(map(str, scc_estudiante_carrera_count))) #convierto la tupla en un numero
    if scc_estudiante_carrera_count_conv <= 0:
        estudiante_carrera_id = 0
    else:
        scc_estudiante_carrera_id = conexion_scc.cursor()
        scc_estudiante_carrera_id.execute("SELECT id FROM profession_profession WHERE profession_code = %s",(estudiante_codigo_carrera,))
        scc_estudiante_carrera_id_id = scc_estudiante_carrera_id.fetchone()
        estudiante_carrera_id = int(''.join(map(str, scc_estudiante_carrera_id_id))) #convierto la tupla en un numero        
    #SCC
    #comprobamos si el estudiante existe como estudiante en el sistema SCC
    #estudiante_rut_format = '123'
    scc_estudiante = conexion_scc.cursor()
    scc_estudiante.execute("SELECT count(*) FROM student_student WHERE student_rut = %s",(estudiante_rut_format,))
    scc_estudiante_count = scc_estudiante.fetchone()
    scc_estudiante_count_conv = int(''.join(map(str, scc_estudiante_count))) #convierto la tupla en un numero
    scc_estudiante_user = conexion_scc.cursor()
    scc_estudiante_user.execute("SELECT count(*) FROM auth_user WHERE username = %s",(estudiante_rut_format,))
    scc_estudiante_user_count = scc_estudiante_user.fetchone()
    scc_estudiante_user_count_conv = int(''.join(map(str, scc_estudiante_user_count))) #convierto la tupla en un numero
    student_credential_state = 'Sin credencial' 
    student_path_image = ''
    #if acc == 0:
    
    if scc_estudiante_count_conv <= 0 or scc_estudiante_user_count_conv <=0:
        #caso 1 no existe como estudiante ni como usuario
        if scc_estudiante_count_conv <= 0 and scc_estudiante_user_count_conv <=0:                
            result_add_user = add_user(now,estudiante_rut_format,estudiante_nombre,estudiante_apellido,estudiante_mail1)
            if result_add_user != -1:
                user_id = consulta_id(estudiante_rut_format)#obtengo el user id
                if user_id != -1:
                    result_add_profile_student = add_profile_student(user_id)
                    if result_add_profile_student != -1:
                        result_add_student = add_student(user_id,estudiante_carrera_id,estudiante_rut_format,estudiante_nombre,estudiante_apellido,estudiante_mail1,estudiante_mail2,estudiante_mail3,estudiante_mail4,estudiante_phone1,estudiante_phone2,estudiante_phone3,estudiante_genero,estudiante_sede,estudiante_tipo,estudiante_nivel,estudiante_periodo_ingreso,student_path_image,estudiante_region,estudiante_comuna,estudiante_estado,student_credential_state,now)
                        if result_add_student == -1:
                            non_created_student += 1
                    else:
                        non_created_profile += 1
                else:
                    non_created_all += 1 
            else:
                non_created_all += 1 

        #caso 2 no existe como estudiante pero si como usuario
        if scc_estudiante_count_conv <= 0 and scc_estudiante_user_count_conv >0:                
            user_id = consulta_id(estudiante_rut_format)#obtengo el user id
            if user_id != -1:
                result_add_student = add_student(user_id,estudiante_carrera_id,estudiante_rut_format,estudiante_nombre,estudiante_apellido,estudiante_mail1,estudiante_mail2,estudiante_mail3,estudiante_mail4,estudiante_phone1,estudiante_phone2,estudiante_phone3,estudiante_genero,estudiante_sede,estudiante_tipo,estudiante_nivel,estudiante_periodo_ingreso,student_path_image,estudiante_region,estudiante_comuna,estudiante_estado,student_credential_state,now)
                if result_add_student == -1:
                    non_created_student += 1
            else:
                non_created_all += 1             
        #crea credencial si no existe
        user_id_credential = consulta_id(estudiante_rut_format)
        if user_id_credential != -1:
            scc_student = conexion_scc.cursor()
            scc_student.execute("SELECT count(*) FROM student_student WHERE user_id = %s",(user_id_credential,))
            scc_student_count = scc_student.fetchone()
            scc_student_count_conv = int(''.join(map(str, scc_student_count))) #convierto la tupla en un numero
            if scc_student_count_conv > 0:
                scc_student_id = conexion_scc.cursor()
                scc_student_id.execute("SELECT id FROM student_student WHERE user_id = %s LIMIT 1",(user_id_credential,))
                student_id = scc_student_id.fetchone()
                student_id_conv = int(''.join(map(str, student_id))) #convierto la tupla en un numero

                scc_credential = conexion_scc.cursor()
                scc_credential.execute("SELECT count(*) FROM credential_credential WHERE student_id = %s",(student_id_conv,))
                scc_credential_count = scc_credential.fetchone()
                scc_credential_count_conv = int(''.join(map(str, scc_credential_count))) #convierto la tupla en un numero
                if scc_credential_count_conv <= 0:
                    credential_state = 'Sin credencial'
                    estudiante_periodo_ingreso_str = str(estudiante_periodo_ingreso)
                    year_delivery = int(estudiante_periodo_ingreso_str[:4])
                    result_credential = add_credential(student_id_conv,credential_state,now,other_now,year_delivery)
                    if result_credential == -1:
                        non_created_credential += 1
        else:
            non_created_credential += 1  
  
    #SGR
    #SGT

        acc += 1
    row_base_estudiante = base_estudiante.fetchone()

acc_credential_final = acc - non_created_credential
print(acc_credential_final)
acc_final = acc - (non_created_all + non_created_profile)
#dejo todos los registros de creacion de usuarios anteriores bloqueados
records_update = conexion_scc.cursor()
records_update.execute("UPDATE administrator_records SET records_state = 'Bloqueado' WHERE records_type = 'Student'")
conexion_scc.commit()   
#inserto registro creacion de usuarios
records_save = conexion_scc.cursor()
records_save.execute("INSERT INTO administrator_records(records_numbers,records_type,records_state,created,updated) VALUES(%s,%s,%s,%s,%s)",(acc_final,'Student','Activo',now,now))
conexion_scc.commit()   

#dejo todos los registros de creacion de credenciales anteriores bloqueados
records_credential_update = conexion_scc.cursor()
records_credential_update.execute("UPDATE administrator_records SET records_state = 'Bloqueado' WHERE records_type = 'Credential'")
conexion_scc.commit()   
#inserto registro
records_credential_save = conexion_scc.cursor()
records_credential_save.execute("INSERT INTO administrator_records(records_numbers,records_type,records_state,created,updated) VALUES(%s,%s,%s,%s,%s)",(acc_credential_final,'Credential','Activo',now,now))
conexion_scc.commit()   

conexion_scc.close()


