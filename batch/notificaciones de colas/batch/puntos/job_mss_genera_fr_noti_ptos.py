import pymysql
import boto3
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
import json
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
import datetime
import jaydebeapi
import sys
import traceback
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GlueJobError(Exception):
    """Excepción base para errores del job de Glue"""
    pass

class DataValidationError(GlueJobError):
    """Error en la validacion de los datos de archivo de minsait"""
    pass

class ConfigurationError(GlueJobError):
    """Error en configuración global de configuracion de parseo"""
    pass

class DataProcessingError(GlueJobError):
    """Error en procesamiento de datos """
    pass

class FileNotFoundError(GlueJobError):
    """Error en la lectura de archivo"""
    pass

def get_secret(vault_name,region_name):
    """"
    obtiene los secretos de la base de datos desde aws secret manager.
    """
    
    region_name = region_name
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=vault_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

glue_params = [
    'JOB_NAME',
    'SYBASE_SECRET',
    'RDS_MYSQL_SECRET',
    "REGION_NAME",
    "JAR_SYBASE_PATH",
    "DRIVER_MYSQL",
    "DRIVER_SYBASE",
    "NEMONICO",
    "MAX_RECORDS",
    "PREFIJO",
    "S3_BUCKET_NAME",
    "TIPO_PROCESO",
    "DIAS",
    "S3_PREFIX"
]
try:
    args = getResolvedOptions(sys.argv, glue_params)
except Exception as e:
    logger.error(f"Error obtenido en la parametrizacion de glue: {e} ")
    raise ConfigurationError("Argumentos requeridos no encontrados")

JOB_NAME = args['JOB_NAME']
DRIVER_MYSQL = args["DRIVER_MYSQL"]
DRIVER_SYBASE = args["DRIVER_SYBASE"]
REGION_NAME = args['REGION_NAME']
JAR_SYBASE_PATH = args.get("JAR_SYBASE_PATH", None)
NEMONICO = args.get("NEMONICO", "NAPTC")
S3_BUCKET_NAME = args.get("S3_BUCKET_NAME", "bb-emisormdp-datasource")
PREFIJO = args.get("PREFIJO", "TCR")
MAX_RECORDS = int(args.get("MAX_RECORDS", 20000))
S3_PREFIX = args.get("S3_PREFIX", "notificaciones/agradecimiento")
TIPO_PROCESO = args.get("TIPO_PROCESO", "B")
DIAS = int(args.get("DIAS", 1))


sybase_credentials = get_secret(args['SYBASE_SECRET'], REGION_NAME)
mysql_credentials = get_secret(args['RDS_MYSQL_SECRET'], REGION_NAME)
#TODO descomentar esta linea cuando se suba a dev :v
# SYBASE_URL = "jdbc:sqlserver://{0}:{1};databaseName={2};encrypt=false;trustServerCertificate=true".format(
#             sybase_credentials["host"], 
#             sybase_credentials["port"], 
#             "db_tarjeta_bb"
#         )

SYBASE_URL = "jdbc:sqlserver://{0}:{1};databaseName={2};encrypt=false;trustServerCertificate=true".format(
            'sybase_database', 
            sybase_credentials["port"], 
            "db_tarjeta_bb"
        )


#jdbc_ods_url = f"jdbc:mysql://{mysql_credentials['host']}:{mysql_credentials['port']}/DB_TC_ODS?useSSL=false&allowPublicKeyRetrieval=true"
jdbc_ods_url = f"jdbc:mysql://{mysql_credentials['host']}:{mysql_credentials['port']}/{mysql_credentials['dbname']}?useSSL=false&allowPublicKeyRetrieval=true"
CONECTION_SYBASE = {
    'url': SYBASE_URL,
    'user': sybase_credentials['username'],
    'password': sybase_credentials['password'],
    'driver': DRIVER_SYBASE
}
CONNECTION_ODS = {
    'url': jdbc_ods_url,
    'user': mysql_credentials['username'],
    'password': mysql_credentials['password'],
    'driver': DRIVER_MYSQL
}
try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.broadcastTimeout", "600")
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    print("GlueContext y SparkSession inicializados correctamente.")
except Exception:
    print("Error en la inicialización:", traceback.format_exc())
    sys.exit(1)


def execute_stored_procedure(proc_name, connection, params=None):
    """
    Ejecuta un procedimiento almacenado en la base de datos y retorna los resultados.
    """
    conn = pymysql.connect(
        host=connection["host"],
        port=int(connection["port"]),
        user=connection["username"],
        password=connection["password"],
        database=connection["dbname"],
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cs:
            if params:
                placeholders = ', '.join(['%s'] * len(params))
                sql = f"CALL {proc_name}({placeholders})"
                logger.info(f"Ejecutando procedimiento almacenado: sql={sql}, params={params}")
                cs.execute(sql, params)
            else:
                sql = f"CALL {proc_name}()"
                cs.execute(sql)
            result = cs.fetchall()
            conn.commit()
            logger.info(f"Procedimiento almacenado {proc_name} ejecutado correctamente.")
            return result[0] if result else None
    except pymysql.MySQLError as e:
        logger.error(f"Error al ejecutar el procedimiento almacenado {proc_name}: {e}")
        raise DataProcessingError(f"Error al ejecutar el procedimiento almacenado {proc_name}: {e}")
    finally:
        conn.close()
def execute_read_query(query, connection):
    """
    Ejecuta una consulta SQL y devuelve el resultado como un DataFrame de Spark.
    """
    try:
        df = spark.read.format("jdbc") \
            .option("url", connection['url']) \
            .option("dbtable", f"({query}) as query") \
            .option("user", connection['user']) \
            .option("password", connection['password']) \
            .option("driver", connection['driver']) \
            .load()
        return df
    except AnalysisException as e:
        logger.error(f"Error al ejecutar la consulta: {e}")
        raise DataProcessingError(f"Error al ejecutar la consulta: {e}")

def execute_write_query(df, table_name, connection):
    """
    Escribe un DataFrame de Spark en una tabla de la base de datos.
    """
    try:
        df.write.format("jdbc") \
            .option("url", connection['url']) \
            .option("dbtable", table_name) \
            .option("user", connection['user']) \
            .option("password", connection['password']) \
            .option("driver", connection['driver']) \
            .mode('append') \
            .save()
        logger.info(f"Datos escritos correctamente en la tabla {table_name}.")
    except AnalysisException as e:
        logger.error(f"Error al escribir en la tabla {table_name}: {e}")
        raise DataProcessingError(f"Error al escribir en la tabla {table_name}: {e}")

def update_notification_data():
   try:
    import pymysql

    conn = pymysql.connect(
        host=mysql_credentials["host"],
        port=int(mysql_credentials["port"]),
        user=mysql_credentials["username"],
        password=mysql_credentials["password"],
        database=mysql_credentials["dbname"],
        cursorclass=pymysql.cursors.DictCursor
    )
    with conn.cursor() as cr:
        update_sql = """
        UPDATE emi_noti_puntos_gen_inf a
            INNER JOIN emi_noti_puntos_dato_cliente m 
                ON a.identificacion = m.identificacion 
            SET 
                a.correo_electronico = COALESCE(m.correo_electronico, '');
        """
        cr.execute(update_sql)
        affected_rows = cr.rowcount
        conn.commit()
        logger.info(f"Datos de notificación actualizados correctamente. Filas afectadas: {affected_rows}")
   except pymysql.MySQLError as e:
        logger.error(f"Error al actualizar los datos de notificación: {e}")
        raise DataProcessingError(f"Error al actualizar los datos de notificación: {e}")
   finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos MySQL cerrada correctamente.")

def execute_sybase_query(query,connection):
    conn = jaydebeapi.connect(
        jclassname=DRIVER_SYBASE,
        url=connection['url'],
        driver_args=[connection['user'], connection['password']],
        jars=JAR_SYBASE_PATH
    )
    try:
        cursor = conn.cursor()      
        cursor.execute(query)
        conn.commit()
        logger.info(f"Consulta ejecutada correctamente: {query}")
    except jaydebeapi.DatabaseError as e:
        logger.error(f"Error al ejecutar la consulta: {e}")
        raise DataProcessingError(f"Error al ejecutar la consulta: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info(f"Conexión a la base de datos Sybase cerrada correctamente.")

def format_notification_data():

    
    """
    Lee la tabla emi_noti_puntos_gen_inf y formatea los datos según el SP pa_tcr_bnotipuntos
    """
    try:
        e_filename = NEMONICO
        e_secuencial = PREFIJO
        e_nemonico = NEMONICO
        
        query = """
        SELECT 
            ente,
            identificacion,
            s_num_bin,
            i_num_cuenta,
            tarjeta,
            s_descripcion,
            PuntosGanados,
            nombres,
            apellidos,
            correo_electronico
        FROM emi_noti_puntos_gen_inf
        WHERE correo_electronico IS NOT NULL
        ORDER BY ente, s_num_bin, i_num_cuenta, PuntosGanados
        """
        
        df = execute_read_query(query, CONNECTION_ODS)
        
        if df.isEmpty():
            logger.warning("No se encontraron datos en emi_noti_puntos_gen_inf.")
            return spark.createDataFrame([], "dato STRING")
        
        from pyspark.sql.functions import (
            concat, lit, col, when, date_format, format_number, 
            row_number, ceil, current_timestamp, regexp_replace,
            substring, lpad, desc, asc, length
        )
        from pyspark.sql.window import Window
        
        # Obtener fecha y hora actual
        now = datetime.datetime.now()
        fecha_str = now.strftime("%Y%m%d")
        hora_str = now.strftime("%H%M%S")
        
        window_spec = Window.orderBy("ente", "s_num_bin", "i_num_cuenta", "PuntosGanados")
        
        df_with_row = df.withColumn("row_num", row_number().over(window_spec))
        
        df_segmented = df_with_row.withColumn(
            "secuencial_id", 
            ceil(col("row_num") / MAX_RECORDS)
        )
        
        df_formatted = df_segmented.select(
            col("secuencial_id"),
            col("row_num").alias("_id"),
            col("correo_electronico").alias("toline"),
            col("i_num_cuenta").alias("NumeroCuenta"),
            col("PuntosGanados"),
            col("s_descripcion").alias("descripcion"),
            concat(col("nombres"), lit(" "), col("apellidos")).alias("nombre_cliente"),
            concat(
                lit("**** **** **** *"),
                substring(col("tarjeta"), -3, 3)
            ).alias("tarjeta_masked"),
            concat(
                lit(e_secuencial),
                lit(fecha_str),
                lit(hora_str),
                lpad(col("row_num").cast("string"), 6, "0")
            ).alias("secuencial"),
            col("ente"),
            col("identificacion"),
            col("tarjeta")
        )
        
        max_segmento = df_formatted.agg({"secuencial_id": "max"}).collect()[0][0]
        
        result_data = []
        
        # Obtener fecha y hora base para todos los archivos
        now = datetime.datetime.now()
        fecha_base = now.strftime("%Y%m%d")
        hora_base = now.strftime("%H%M%S")
        
        for segmento in range(1, max_segmento + 1):
            segmento_df = df_formatted.filter(col("secuencial_id") == segmento)
            segmento_data = segmento_df.collect()
            
            if not segmento_data:
                continue
            
            # Línea 1: Nombre del archivo con secuencial incremental
            filename_line = f"1{e_filename}{fecha_base}{hora_base}{segmento:03d}.build"
            result_data.append((filename_line,))
            
            # Línea 2: Cabecera
            header_line = f"2BOLIVARIANO|{e_nemonico}||inot2|Avisos24|{len(segmento_data)}"
            result_data.append((header_line,))
            
            # Líneas de datos
            for row in segmento_data:
                data_line = (
                    f"2{row['secuencial']}||"
                    f"{row['ente']}|"
                    f"email={row['toline']}||"
                    f"{e_nemonico}|"
                    f"nombre_cliente={row['nombre_cliente']}"
                    f"##nrotrj={row['tarjeta_masked']}"
                    f"##tiptrj={row['descripcion']}"
                    f"##puntos={row['PuntosGanados']}|"
                    f"{e_nemonico}PRIV|"
                    f"nombre_cliente={row['nombre_cliente']}"
                    f"##nrotrj={row['tarjeta_masked']}"
                    f"##tiptrj={row['descripcion']}"
                    f"##puntos={row['PuntosGanados']}"
                )
                result_data.append((data_line,))
            
            # Línea EOF
            eof_line = "2<EOF>"
            result_data.append((eof_line,))
        
        # Crear DataFrame resultado
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("dato", StringType(), True)
        ])
        
        result_df = spark.createDataFrame(result_data, schema)
        
        logger.info(f"Datos formateados correctamente. Total de líneas generadas: {result_df.count()}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error al formatear los datos de notificación de puntos: {e}")
        raise DataProcessingError(f"Error al formatear los datos de notificación de puntos: {e}")

def generate_build_files(df: DataFrame, output_path: str = "/tmp/"):
    """
    Genera archivos .build basado en el DataFrame formateado que viene de format_notification_data()
    El DataFrame ya contiene el formato correcto con prefijos incluidos
    """
    try:
        if df.isEmpty():
            logger.warning("No hay registros para procesar")
            return []
        data_rows = df.collect()
        total_registros = len(data_rows)
        
        logger.info(f"Total de líneas a procesar: {total_registros}")
        
        archivos_generados = []
        archivo_actual = None
        archivo_num = 0
        
        for row in data_rows:
            dato = row['dato']
            
            if dato.startswith("1"):
                if archivo_actual:
                    archivo_actual.close()
                
                nombre_archivo = dato[1:]  # Quitar el "1" del inicio
                ruta_completa = f"{output_path}{nombre_archivo}"
                
                archivo_num += 1
                logger.info(f"Iniciando archivo {archivo_num}: {nombre_archivo}")
                
                try:
                    archivo_actual = open(ruta_completa, 'w', encoding='utf-8')
                    archivos_generados.append(ruta_completa)
                except IOError as e:
                    logger.error(f"Error al crear archivo {ruta_completa}: {e}")
                    raise DataProcessingError(f"Error al crear archivo {ruta_completa}: {e}")
            
            elif dato.startswith("2"):
                if archivo_actual:
                 
                    contenido = dato[1:]  # Quitar el "2" del inicio
                    archivo_actual.write(contenido + "\n")
                else:
                    logger.warning(f"Línea de contenido sin archivo abierto: {dato}")
        
        if archivo_actual:
            archivo_actual.close()
        
        logger.info(f"Proceso completado. Se generaron {len(archivos_generados)} archivos .build")
        
        for archivo in archivos_generados:
            try:
                import os
                tamaño = os.path.getsize(archivo)
                logger.info(f"Archivo generado: {archivo} (Tamaño: {tamaño} bytes)")
            except Exception as e:
                logger.warning(f"No se pudo obtener información del archivo {archivo}: {e}")
        
        return archivos_generados
        
    except Exception as e:
        logger.error(f"Error al generar archivos .build: {e}")
        raise DataProcessingError(f"Error al generar archivos .build: {e}")

def upload_files_to_s3(file_paths: list, bucket_name: str, s3_prefix: str = ""):
    """
    Sube los archivos generados a un bucket de S3
    """
    try:
        s3_client = boto3.client('s3', region_name=REGION_NAME)
        uploaded_files = []
        
        for file_path in file_paths:
            file_name = file_path.split('/')[-1]
            s3_key = f"{s3_prefix}/{file_name}" if s3_prefix else file_name
            
            try:
                s3_client.upload_file(file_path, bucket_name, s3_key)
                uploaded_files.append(s3_key)
                logger.info(f"Archivo subido a S3: s3://{bucket_name}/{s3_key}")
                
                import os
                os.remove(file_path)
                logger.info(f"Archivo local eliminado: {file_path}")
                
            except ClientError as e:
                logger.error(f"Error al subir archivo {file_path} a S3: {e}")
                raise DataProcessingError(f"Error al subir archivo {file_path} a S3: {e}")
        
        logger.info(f"Se subieron {len(uploaded_files)} archivos a S3")
        return uploaded_files
        
    except Exception as e:
        logger.error(f"Error en la subida de archivos a S3: {e}")
        raise DataProcessingError(f"Error en la subida de archivos a S3: {e}")

def main():
   # fecha_proceso = datetime.datetime.now().strftime("%Y-%m-%d")
    fecha_proceso = '2025-07-11'
    logger.info("Iniciando el job de notificaciones Minsait")
    logger.info(f"credenciales de Sybase: {sybase_credentials}")
    logger.info(f"credenciales de MySQL: {mysql_credentials}")
    response = execute_stored_procedure("pa_tcre_cciclofact",mysql_credentials,[TIPO_PROCESO,fecha_proceso,DIAS])
    if not response:
        logger.error("No se obtuvo respuesta del procedimiento almacenado pa_tcre_cciclofact")
        raise DataProcessingError("No se obtuvo respuesta del procedimiento almacenado pa_tcre_cciclofact")
    logger.info(f"Respuesta del procedimiento almacenado: {response}")

    US_MARCA = response.get("ciclo", None)
    FECHA = response.get("fecha", None)
    MES_CIERRE = response.get("i_mes_cierre", None)
    FECHA_ANTES = response.get("fechaAntes", None)

    if not US_MARCA or not FECHA or not MES_CIERRE or not FECHA_ANTES:
        logger.error("Faltan datos en la respuesta del procedimiento almacenado")
        raise DataProcessingError("Faltan datos en la respuesta del procedimiento almacenado")
    if US_MARCA=='1':
        marca = "VISA" if US_MARCA == '1' else "MASTERCARD"
        logger.info("Se procede a ejecutar el procedimiento almacenado pa_tcre_noti_puntos")
        response = execute_stored_procedure("pa_con_ppuntosmcvs", mysql_credentials, [fecha_proceso, marca])
        logger.info(f"Respuesta del procedimiento almacenado pa_con_ppuntosmcvs: {response}")
        logger.info("Procedimiento almacenado pa_tcre_noti_puntos ejecutado correctamente")
        query = f"""
        select identificacion as s_identificacion,tipo_identificacion as s_tipo_identica from emi_noti_puntos_gen_inf
        """
        execute_sybase_query("TRUNCATE TABLE db_general..tcr_tmp_noti_puntos", CONECTION_SYBASE)
        registros_procesados = execute_read_query(query, CONNECTION_ODS)
        execute_write_query(registros_procesados, "db_general..tcr_tmp_noti_puntos",CONECTION_SYBASE)
        update_registros_identificaciones_query = """
            update a
        set a.s_email = co.de_descripcion
        from db_general..tcr_tmp_noti_puntos a
        inner join cobis..cl_ente e on e.en_ced_ruc = a.s_identificacion
        inner join cobis..cl_direccion_email co on co.de_ente = e.en_ente
        and co.de_tipo = 'E'
        and co.de_fecha_modificacion in (
        select max(y.de_fecha_modificacion)
        from cobis..cl_direccion_email y
        where y.de_ente = e.en_ente
            and y.de_tipo = 'E'
        )
         where a.s_tipo_identica <> 'P'        
        """
        update_registros_pasaportes_query = """
            update a
            set a.s_email = co.de_descripcion
            from db_general..tcr_tmp_noti_puntos a
            inner join cobis..cl_ente e on e.p_pasaporte = a.s_identificacion
            inner join cobis..cl_direccion_email co on co.de_ente = e.en_ente
            and co.de_tipo = 'E'
            and co.de_fecha_modificacion in (
           select max(y.de_fecha_modificacion)
            from cobis..cl_direccion_email y
           where y.de_ente = e.en_ente
             and y.de_tipo = 'E'
            )
            where a.s_tipo_identica = 'P'
        """
        # actualizar registros de identificaciones y pasaportes en la tabla db_general..tcr_tmp_noti_puntos
        execute_sybase_query(update_registros_identificaciones_query,CONECTION_SYBASE)
        execute_sybase_query(update_registros_pasaportes_query,CONECTION_SYBASE)
        logger.info("Se actualizan los registros de identificaciones y pasaportes en la tabla tcr_tmp_noti_puntos")
        query = f"""
        select s_identificacion as identificacion ,s_email as correo_electronico from db_general..tcr_tmp_noti_puntos
        """
        registros_actualizados = execute_read_query(query, CONECTION_SYBASE)
        registros_actualizados.show()
        execute_write_query(registros_actualizados,"emi_noti_puntos_dato_cliente",CONNECTION_ODS)
        update_notification_data()
        
        # Formatear los datos de notificación de puntos
        formatted_data = format_notification_data()
        formatted_data.show(20, False) 

        if not formatted_data.isEmpty():
            # Generar archivos .build
            archivos_generados = generate_build_files(
                df=formatted_data,
                output_path="/tmp/"
            )
           
            logger.info(f"Archivos generados: {archivos_generados}/")
            
            if archivos_generados:
                 bucket_name = S3_BUCKET_NAME
                 s3_prefix = f"{S3_PREFIX}/{fecha_proceso}"
                 upload_files_to_s3(archivos_generados, bucket_name, s3_prefix)
                 logger.info(f"Archivos .build generados exitosamente: {len(archivos_generados)} archivos")
            else:
                logger.warning("No se generaron archivos .build")
        else:
            logger.warning("No se generaron archivos porque no hay datos formateados")
        
        logger.info(f"Proceso de formateo completado exitosamente. Total de líneas: {formatted_data.count()}")

        logger.info(f"Registros actualizados en la tabla tcr_tmp_noti_puntos: {registros_actualizados}")
    else:
        logger.info("No se ejecuta el procedimiento almacenado pa_tcre_noti_puntos porque US_MARCA no es 1")




main()