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
    "S3_PREFIX",
    "DEBUG"
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
FILE_ADD = "MDPMC"
DEBUG = args.get("DEBUG", "false").lower() == "true"


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
            return result if result else None
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


def format_data(data, output_path="/tmp/"):
    """
    Procesa los datos del SP y genera archivos .build y logs según la lógica del programa C#
    
    Prefijos:
    - "1": Indica inicio de nuevo archivo .build
    - "2": Contenido del archivo .build
    - "3": Información de log/metadata
    """
    try:
        if not data:
            logger.warning("No hay datos para procesar")
            return {"archivos_generados": [], "log_info": "", "archivo_origen": ""}
        
        file_counter = 0
        nombre_archivo = ""
        archivo_origen = ""
        log_info = ""
        archivos_generados = []
        archivo_actual = None
        
        logger.info(f"Procesando {len(data)} líneas de datos")
        
        for row in data:
            sline = row.get('dato', '')
            
            if not sline:
                continue
                
 
            if sline.startswith("1"):

                if archivo_actual:
                    archivo_actual.close()
                    archivo_actual = None
                

                nombre_archivo = sline[1:]  # Substring(1, length-1)
                ruta_completa = f"{output_path}{nombre_archivo}"
                

                if file_counter == 0:
                    archivo_origen = nombre_archivo
                
                try:

                    archivo_actual = open(ruta_completa, 'w', encoding='utf-8')
                    archivos_generados.append(ruta_completa)
                    logger.info(f"Archivo {file_counter + 1} creado: {nombre_archivo}")
                    file_counter += 1
                    
                except IOError as e:
                    logger.error(f"Error al crear archivo {ruta_completa}: {e}")
                    raise DataProcessingError(f"Error al crear archivo {ruta_completa}: {e}")
            

            elif sline.startswith("2"):
                if archivo_actual and nombre_archivo:
                    try:
                        info = sline[1:]  # Substring(1, length-1)
                        archivo_actual.write(info + "\n")
                    except IOError as e:
                        logger.error(f"Error al escribir en archivo {nombre_archivo}: {e}")
                        raise DataProcessingError(f"Error al escribir en archivo {nombre_archivo}: {e}")
                else:
                    logger.warning(f"Línea de contenido sin archivo abierto: {sline}")
            
            elif sline.startswith("3"):
                info = sline[1:]  
                log_info += info
        
        if archivo_actual:
            archivo_actual.close()
        
        if len(log_info) > 900:
            log_info = log_info[:900]
        
        logger.info(f"Procesamiento completado:")
        logger.info(f"- Archivos generados: {len(archivos_generados)}")
        logger.info(f"- Archivo origen: {archivo_origen}")
        logger.info(f"- Log info length: {len(log_info)}")
        
        for archivo in archivos_generados:
            try:
                import os
                tamaño = os.path.getsize(archivo)
                logger.info(f"Archivo: {archivo} (Tamaño: {tamaño} bytes)")
            except Exception as e:
                logger.warning(f"No se pudo obtener información del archivo {archivo}: {e}")
        
        return {
            "archivos_generados": archivos_generados,
            "log_info": log_info,
            "archivo_origen": archivo_origen
        }
        
    except Exception as e:
        logger.error(f"Error al procesar datos: {e}")
        raise DataProcessingError(f"Error al procesar datos: {e}")

def upload_files_to_s3(file_paths, bucket_name, s3_prefix=""):
    """
    Sube los archivos generados a S3
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
    fecha_proceso = datetime.datetime.now().strftime("%Y-%m-%d")
    
    logger.info("Iniciando el job de notificaciones Minsait")
    logger.info(f"credenciales de Sybase: {sybase_credentials}")
    logger.info(f"credenciales de MySQL: {mysql_credentials}")

    datos_proceso = execute_stored_procedure("pa_tcr_bnotiemptcemas",mysql_credentials,[fecha_proceso,NEMONICO,FILE_ADD,PREFIJO,NEMONICO,DEBUG])
    if not datos_proceso:
        logger.info("No se encontraron datos para procesar.")
        return
    
    logger.info(f"Datos obtenidos del procedimiento almacenado: {len(datos_proceso)} registros")
    
    # Procesar los datos y generar archivos
    resultado = format_data(datos_proceso, output_path="/tmp/")
    
    if resultado["archivos_generados"]:
        logger.info(f"Archivos generados exitosamente:")
        for archivo in resultado["archivos_generados"]:
            logger.info(f"- {archivo}")
    
        try:
            s3_prefix = f"{S3_PREFIX}/{fecha_proceso}"
            uploaded_files = upload_files_to_s3(
                resultado["archivos_generados"], 
                S3_BUCKET_NAME, 
                s3_prefix
            )
            logger.info(f"Archivos subidos a S3: {uploaded_files}")
        except Exception as e:
            logger.error(f"Error al subir archivos a S3: {e}")
    else:
        logger.warning("No se generaron archivos")
    
    logger.info(f"Archivo origen: {resultado['archivo_origen']}")
    logger.info(f"Log info: {resultado['log_info'][:100]}...") 
    
    logger.info("Procesamiento completado exitosamente")

if __name__ == "__main__":
    try:
        main()
        job.commit()
    except Exception as e:
        logger.error(f"Error en el job: {e}")
        logger.error(traceback.format_exc())
        job.commit()
        sys.exit(1)
    finally:
        sc.stop()
        logger.info("SparkContext detenido correctamente.")