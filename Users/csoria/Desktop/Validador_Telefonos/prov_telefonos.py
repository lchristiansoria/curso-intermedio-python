# -*- coding: utf-8 -*-
"""
Created on Thu Mar 13 15:50:34 2022

@author: csoria
"""
from configparser import ConfigParser
import pyodbc
import pandas as pd
from nosis_python import rabbit
from datetime import datetime

##############################################################################
#VARIABLES
#cargo el Configurador de Config.ini
config = ConfigParser()
config.read('/home/crontab/dl22-43_proveedor_sgr/config.ini')

#datos conexion a la base de datos
USR = config.get('SGR','USUARIO')
PAS = config.get('SGR','PASSWORD')
SRV = config.get('SGR','SERVER')
SRV_001 = config.get('SGR','SERVER_001')
DAB = config.get('SGR','BASE')
TAB_RV = config.get('RIESGOVIVO','TABLA')
TAB_MORA = config.get('MORA','TABLA')
##############################################################################

def envia_log(id_registro, id_tabla ,destino):
    mensaje = str(id_registro) +","
    mensaje = mensaje + str(id_tabla) +","
    mensaje = mensaje + 'Prov_SGR' +","  
    mensaje = mensaje + destino +","
    now = datetime.today().isoformat()
    now = str(now).replace("T"," ").split(".")[0]
    mensaje = mensaje + now
    mensaje = mensaje + ', 1'
    resultado = rabbit.fanout_no_response_generic('192.168.12.1','SGRLogs','sgrLog5','NOSIS','insertaLogsSGR', mensaje)
    return resultado

def inserta_log_error(id_registro, id_tabla, destino):
    now = datetime.today().isoformat()
    now = str(now).replace("T"," ").split(".")[0]
    sql = "insert into SGR.log.ProcesamientoDataLake (id_Registro,Tabla_Registro,Origen,Destino,Fecha_Proceso,Estado) \
            values ('"+str(id_registro)+"','"+str(id_tabla)+"','Prov_SGR','"+destino+"','"+now+"','-1')"
    con=pyodbc.connect(CADENA_SGR)
    con.execute(sql)
    con.commit()
    con.close()

##############################################################################
CADENA_SGR ="DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.8.so.1.1};SERVER="+SRV+";DATABASE="+DAB+";UID="+USR+";PWD="+PAS
CADENA_SGR_001 ="DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.8.so.1.1};SERVER="+SRV_001+";DATABASE="+DAB+";UID="+USR+";PWD="+PAS

#CADENA_SGR = "Driver={ODBC Driver 17 for SQL Server} ;Server="+SRV+";Database="+DAB+";Trusted_Connection=yes;"
#CADENA_SGR_001 = "Driver={ODBC Driver 17 for SQL Server} ;Server="+SRV_001+";Database="+DAB+";Trusted_Connection=yes;"


#CONSULTAS
conn = pyodbc.connect(CADENA_SGR)

SQL_Query = "SELECT [IdRiesgoVivo]\
                    ,Entidades.IdEntidad\
                    ,Entidades.Entidad\
                    ,[CUIT]\
                    ,[RazonSocial]\
                    ,[CodActividad]\
                    ,[ActividadSector]\
                    ,[MiPYMETramo]\
                    ,[CUITAcreedor]\
                    ,[RazonSocialAcreedor]\
                    ,Acreedor.IdTipoAcreedor\
                    ,Acreedor.TipoAcreedor\
                    ,[MontoVigente]\
                    ,[PeriodoInformado]\
                    ,[LoteProceso]\
                    ,[FechaActualizacion]\
                    ,[FechaModificacion]\
                    ,[FechaBaja]\
                    ,[IdTipoMovimiento]	\
            FROM SGR..Riesgo_Vivo RiesgoVivo \
            inner join \
                (\
                    SELECT  [IdEntidad]\
                            ,[Entidad]\
                    FROM SGR..Entidades\
                ) 	AS Entidades \
            on Entidades.IdEntidad = RiesgoVivo.IdEntidad \
            inner join \
                ( \
                    SELECT  TipoAcreedor\
                            ,IdTipoAcreedor \
                    FROM SGR..TiposAcreedor \
                ) 	AS Acreedor \
            on RiesgoVivo.TipoAcreedor = Acreedor.IdTipoAcreedor \
            where pasadoRobot = 0 and Validado = 1"

# guarda el resultado de la consulta en Data Frame
df_riego_vivo = pd.read_sql (SQL_Query,conn)
conn.close()


conn = pyodbc.connect(CADENA_SGR)

SQL_Query = "SELECT \
                    [IdMora] \
                    ,Entidades.IdEntidad \
                    ,Entidades.Entidad \
                    ,[CUIT] \
                    ,[RazonSocial] \
                    ,[MontoDeuda] \
                    ,[DiasMora] \
                    ,Deudor.IdTipoDeudor \
                    ,Deudor.TipoDeudor \
                    ,[PeriodoInformado] \
                    ,[LoteProceso] \
                    ,[FechaActualizacion] \
                    ,[FechaModificacion] \
                    ,[FechaBaja] \
                    ,[IdTipoMovimiento] \
                FROM [SGR].[dbo].[Mora] Mora \
                inner join \
                    ( \
                        SELECT  [IdEntidad] \
                                ,[Entidad] \
                        FROM SGR..Entidades \
                    ) 	AS Entidades \
                    on Entidades.IdEntidad = Mora.IdEntidad \
                inner join \
                    ( \
                        SELECT  TipoDeudor \
                                ,IdTipoDeudor \
                        FROM SGR..TiposDeudor \
                    ) 	AS Deudor \
                on  Deudor.IdTipoDeudor = Mora.TipoDeudor \
        where pasadoRobot = 0 and Validado = 1"

# guarda el resultado de la consulta en Data Frame
df_mora = pd.read_sql (SQL_Query,conn)
conn.close()

if (len(df_riego_vivo) > 0 or len(df_mora) > 0):
    #ACTUALIZAMOS LOS MAESTROS
    con=pyodbc.connect(CADENA_SGR_001)
    con.execute("exec SP_Actualiza_Maestros_SGR")
    con.commit()
    con.close()

    # RIEGO VIVO 
    if len(df_riego_vivo) > 0:
        #ENVIO A LA COLA DE RIESGO VIVO
        for i in range(len(df_riego_vivo)):            
            mensaje = df_riego_vivo.iloc[i].to_json(orient="columns",date_format="iso")
            resultado = rabbit.fanout_no_response_generic('192.168.12.1','RiesgoVivoWrite','riesgoWr1te','NOSIS','RiesgoVivoNoRespuesta', mensaje)
         
            if resultado == 1:
                result = envia_log(df_riego_vivo.iloc[i][0], 1,'RiesgoVivoNoRespuesta')
                # en caso de un error
                if result != 1:
                    inserta_log_error(df_riego_vivo.iloc[i][0], 1, 'insertaLogSGR')
                #update Riesgo_vivo 002
                sql = "update [dbo].[Riesgo_Vivo] set PasadoRobot = 1 where idRiesgoVivo = "+str(df_riego_vivo.iloc[i][0])
                con=pyodbc.connect(CADENA_SGR)
                con.execute(sql)
                con.commit()
                con.close()
            else:
                inserta_log_error(df_riego_vivo.iloc[i][0], 1, 'RiesgoVivoNoRespuesta')

            

    #MORA
    if len(df_mora) > 0 :
        #ENVIO A LA COLA DE MORA
        for i in range(len(df_mora)):            
            mensaje = df_mora.iloc[i].to_json(orient="columns",date_format="iso")
            resultado = rabbit.fanout_no_response_generic('192.168.12.1','MoraWrite','moraWr1te','NOSIS','MoraNoRespuesta', mensaje)

            if resultado == 1:
                result = envia_log(df_mora.iloc[i][0],2 ,'MoraNoRespuesta')
                # en caso de un error
                if result != 1:
                    inserta_log_error(df_mora.iloc[i][0], 2, 'insertaLogSGR')
                #update
                sql = "update [dbo].[Mora] set PasadoRobot = 1 where idMora = "+str(df_mora.iloc[i][0])
                con=pyodbc.connect(CADENA_SGR)
                con.execute(sql)
                con.commit()
                con.close()
            else:
                inserta_log_error(df_mora.iloc[i][0], 2, 'MoraNoRespuesta')

            
        
