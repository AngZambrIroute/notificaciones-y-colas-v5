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

def main():
    fecha_proceso = datetime.datetime.now().strftime("%Y-%m-%d")

    logger.info("Iniciando el job de notificaciones renovacion de tarjeta de credito")
    logger.info(f"credenciales de Sybase: {sybase_credentials}")
    logger.info(f"credenciales de MySQL: {mysql_credentials}")
    execute_stored_procedure("pa_tcr_balr_renov_tc",mysql_credentials,[fecha_proceso])
    logger.info(f"Respuesta del procedimiento almacenado: pa_tcr_balr_renov_tc ejecutado con fecha {fecha_proceso}")
    #limpiar tabla db_general..tcre_tmp_alr_renov_tc
    execute_sybase_query("TRUNCATE TABLE db_general..tcre_tmp_alr_renov_tc", CONECTION_SYBASE)
    logger.info("Tabla db_general..tcre_tmp_alr_renov_tc truncada correctamente.")
    query_renovaciones_pendientes = """
        select t_tipo_alerta as ar_tipo_alerta,
       t_id_cuenta as ar_id_cuenta, t_id_tarjeta as ar_id_tarjeta,
       t_tipo_tarjeta as ar_tipo_tarjeta, t_nro_tarjeta as ar_nro_tarjeta,
       t_id_cliente as ar_id_cliente, t_nombre_cliente as ar_nombre_cliente, 
       t_identificacion as ar_identificacion, t_tipo_identifica as ar_tipo_identifica,
       t_tipo_mov as ar_tipo_mov, t_codigo_renov as ar_codigo_renov,
       t_motivo_renov as ar_motivo_renov, t_cod_bodega as ar_cod_bodega, 
       t_bodega as ar_bodega, t_id_fec_proc as ar_id_fec_proc, 
       t_fecha_envio as ar_fecha_envio, t_telefono as ar_telefono,
       t_mail as ar_mail from emi_noti_renov_tc_info

    """
    renovaciones_pendientes = execute_read_query(query_renovaciones_pendientes,CONNECTION_ODS)
    if renovaciones_pendientes.count() == 0:
        logger.info("No hay renovaciones pendientes para procesar.")
        return
    logger.info(f"Renovaciones pendientes encontradas: {renovaciones_pendientes.count()}")
    # mover las renovaciones pendientes a sybase
    execute_write_query(renovaciones_pendientes, "db_general..tcre_tmp_alr_renov_tc", CONECTION_SYBASE)
    logger.info("Renovaciones pendientes movidas a Sybase correctamente.")
    execute_sybase_query("EXEC db_tarjeta_bb..pa_tcre_binfalrrenv", CONECTION_SYBASE)
    logger.info("Procedimiento almacenado pa_tcre_binfalrrenv ejecutado correctamente.")
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