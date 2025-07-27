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

def execute_sp_sybase(proc_name,connection,params=None):
    conn = jaydebeapi.connect(
        jclassname=DRIVER_SYBASE,
        url=connection['url'],
        driver_args=[connection['user'], connection['password']],
        jars=JAR_SYBASE_PATH
    )
    try:
        cursor = conn.cursor()
        if params:
            placeholders = ', '.join(['?'] * len(params))
            sql = f"EXEC {proc_name} {placeholders}"
            cursor.execute(sql, params)
        else:
            sql = f"EXEC {proc_name}"
            cursor.execute(sql)
        conn.commit()
        logger.info(f"Procedimiento almacenado {proc_name} ejecutado correctamente.")
    except jaydebeapi.DatabaseError as e:
        logger.error(f"Error al ejecutar el procedimiento almacenado {proc_name}: {e}")
        raise DataProcessingError(f"Error al ejecutar el procedimiento almacenado {proc_name}: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info(f"Conexión a la base de datos Sybase cerrada correctamente.")



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

def execute_stored_procedure(proc_name,connection, params=None):
    """
    Ejecuta un procedimiento almacenado en la base de datos.
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
                cs.execute(sql, params)
            else:
                sql = f"CALL {proc_name}()"
                cs.execute(sql)
            conn.commit()
            logger.info(f"Procedimiento almacenado {proc_name} ejecutado correctamente.")
    except pymysql.MySQLError as e:
        logger.error(f"Error al ejecutar el procedimiento almacenado {proc_name}: {e}")
        raise DataProcessingError(f"Error al ejecutar el procedimiento almacenado {proc_name}: {e}")

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
        UPDATE emi_noti_agradec_pago_inf a
            INNER JOIN emi_tcr_tmp_medio m 
                ON a.s_identifica = m.identificacion 
                AND a.s_tipo_identifica = m.tipo_identificacion
            SET 
                a.i_cod_mis = COALESCE(m.mis, a.i_cod_mis),
                a.s_correo = COALESCE(m.correo, a.s_correo),
                a.s_movil = COALESCE(m.telefono, a.s_movil);
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
    
def format_notification_data():
    """
    Lee la tabla emi_noti_agradec_pago_inf y formatea los datos según el SP pa_tcr_bobtener_noti
    """
    try:
        # Leer datos de la tabla emi_noti_agradec_pago_inf
        query = """
        SELECT 
            i_cod_mis,
            s_correo,
            i_num_cuenta,
            s_tipo_identifica,
            s_identifica,
            s_nom_plas,
            s_descrip_bi,
            d_fecha_proceso,
            i_num_pago,
            m_monto,
            m_saldo_pm,
            m_saldo_rotativo,
            m_saldo_diferido,
            m_pago_contado,
            m_sum_credito,
            m_saldo_pc,
            d_fec_top_pag,
            d_fec_corte,
            d_fecha_transac
        FROM emi_noti_agradec_pago_inf
        ORDER BY i_num_cuenta ASC
        """
        
        df = execute_read_query(query, CONNECTION_ODS)
        if df.isEmpty():
            logger.warning("No se encontraron datos en emi_noti_agradec_pago_inf.")
            return spark.createDataFrame([], schema=df.schema)  # Retorna un DataFrame vacío con la misma estructura
        
        from pyspark.sql.functions import concat, lit, col, when, date_format, format_number
        
        # Formatear los datos según el SP
        formatted_df = df.select(
            concat(
                lit("||"),
                col("i_cod_mis").cast("string"),
                lit("|email="),
                col("s_correo"),
                lit("||NAPTC|"),
                lit("CUENTA_TC="),
                col("i_num_cuenta").cast("string"),
                lit("##"),
                lit("TI_IDENT="),
                col("s_tipo_identifica"),
                lit("##"),
                lit("CO_IDENT="),
                col("s_identifica"),
                lit("##"),
                lit("NOM_PLAS="),
                col("s_nom_plas"),
                lit("##"),
                lit("DESC_BIN="),
                col("s_descrip_bi"),
                lit("##"),
                lit("CO_MIS="),
                col("i_cod_mis").cast("string"),
                lit("##"),
                lit("FCH_PRO="),
                date_format(col("d_fecha_proceso"), "dd/MM/yyyy"),
                lit("##"),
                lit("NUM_PAGO="),
                col("i_num_pago").cast("string"),
                lit("##"),
                lit("MONTO_PAGO="),
                format_number(col("m_monto"), 1),
                lit("##"),
                lit("SALDO_PM="),
                format_number(col("m_saldo_pm"), 1),
                lit("##"),
                lit("SALDO_ACTUAL="),
                format_number(col("m_saldo_rotativo"), 1),
                lit("##"),
                lit("SALDO_DIFERIDO="),
                format_number(col("m_saldo_diferido"), 1),
                lit("##"),
                lit("PAGO_CONTADO="),
                format_number(col("m_pago_contado"), 1),
                lit("##"),
                lit("SUM_CREDITOS="),
                format_number(col("m_sum_credito"), 1),
                lit("##"),
                lit("SALDO_PC="),
                format_number(when(col("m_saldo_pc") < 0, 0).otherwise(col("m_saldo_pc")), 1),
                lit("##"),
                lit("FCH_TOPE="),
                date_format(col("d_fec_top_pag"), "dd/MM/yyyy"),
                lit("##"),
                lit("FCHA_CORTE="),
                date_format(col("d_fec_corte"), "dd/MM/yyyy"),
                lit("##"),
                lit("FCH_TRX="),
                date_format(col("d_fecha_transac"), "dd/MM/yyyy"),
                lit("|NAPTCPRIV|"),
                lit("CUENTA_TC="),
                col("i_num_cuenta").cast("string"),
                lit("##"),
                lit("TI_IDENT="),
                col("s_tipo_identifica"),
                lit("##"),
                lit("CO_IDENT="),
                col("s_identifica"),
                lit("##"),
                lit("NOM_PLAS="),
                col("s_nom_plas"),
                lit("##"),
                lit("DESC_BIN="),
                col("s_descrip_bi"),
                lit("##"),
                lit("CO_MIS="),
                col("i_cod_mis").cast("string"),
                lit("##"),
                lit("FCH_PRO="),
                date_format(col("d_fecha_proceso"), "dd/MM/yyyy"),
                lit("##"),
                lit("NUM_PAGO="),
                col("i_num_pago").cast("string"),
                lit("##"),
                lit("MONTO_PAGO="),
                format_number(col("m_monto"), 1),
                lit("##"),
                lit("SALDO_PM="),
                format_number(col("m_saldo_pm"), 1),
                lit("##"),
                lit("SALDO_ACTUAL="),
                format_number(col("m_saldo_rotativo"), 1),
                lit("##"),
                lit("SALDO_DIFERIDO="),
                format_number(col("m_saldo_diferido"), 1),
                lit("##"),
                lit("PAGO_CONTADO="),
                format_number(col("m_pago_contado"), 1),
                lit("##"),
                lit("SUM_CREDITOS="),
                format_number(col("m_sum_credito"), 1),
                lit("##"),
                lit("SALDO_PC="),
                format_number(when(col("m_saldo_pc") < 0, 0).otherwise(col("m_saldo_pc")), 1),
                lit("##"),
                lit("FCH_TOPE="),
                date_format(col("d_fec_top_pag"), "dd/MM/yyyy"),
                lit("##"),
                lit("FCHA_CORTE="),
                date_format(col("d_fec_corte"), "dd/MM/yyyy"),
                lit("##"),
                lit("FCH_TRX="),
                date_format(col("d_fecha_transac"), "dd/MM/yyyy")
            ).alias("data")
        )
        
        logger.info(f"Datos formateados correctamente. Total de registros: {formatted_df.count()}")
        return formatted_df
        
    except Exception as e:
        logger.error(f"Error al formatear los datos de notificación: {e}")
        raise DataProcessingError(f"Error al formatear los datos de notificación: {e}")

def generate_build_files(df: DataFrame, output_path: str = "/tmp/", nemonico: str = "NAPTC",max_records: int = 20000,prefijo: str = "TCR"):
    """
    Genera archivos .build basado en el DataFrame formateado
    Cada archivo puede contener máximo 20,000 registros
    """
    try:

        MAX_RECORDS_PER_FILE = max_records
        PREFIX = prefijo

        now = datetime.datetime.now()
        fecha_str = now.strftime("%Y%m%d%H%M%S%f")[:-3]  
        fecha_r_str = now.strftime("%Y%m%d%H%M%S")  
        
        total_registros = df.count()
        logger.info(f"Total de registros a procesar: {total_registros}")
        
        if total_registros == 0:
            logger.warning("No hay registros para procesar")
            return []
        
        num_archivos = (total_registros + MAX_RECORDS_PER_FILE - 1) // MAX_RECORDS_PER_FILE
        
        data_rows = df.collect()
        
        archivos_generados = []
        
        for archivo_num in range(1, num_archivos + 1):
            inicio = (archivo_num - 1) * MAX_RECORDS_PER_FILE
            fin = min(inicio + MAX_RECORDS_PER_FILE, total_registros)
            registros_archivo = fin - inicio
            
            nombre_archivo = f"{nemonico}{fecha_str}{archivo_num:04d}.build"
            ruta_completa = f"{output_path}{nombre_archivo}"
            
            logger.info(f"Generando archivo {archivo_num}/{num_archivos}: {nombre_archivo} con {registros_archivo} registros")
            
            try:
               
                with open(ruta_completa, 'w', encoding='utf-8') as file:
                
                    cabecera = f"BOLIVARIANO|{nemonico}||inot2|Avisos24|{registros_archivo}"
                    file.write(cabecera + "\n")
                    
                
                    for i in range(inicio, fin):
                        
                        line_prefix = f"{PREFIX}{fecha_r_str}{i:06d}"
                        data_content = data_rows[i]['data']
                        file.write(line_prefix + data_content + "\n")
                    
                    file.write("<EOF>")
                
                archivos_generados.append(ruta_completa)
                logger.info(f"Archivo generado exitosamente: {ruta_completa}")
                
            except IOError as e:
                logger.error(f"Error al escribir archivo {ruta_completa}: {e}")
                raise DataProcessingError(f"Error al escribir archivo {ruta_completa}: {e}")
        
        logger.info(f"Proceso completado. Se generaron {len(archivos_generados)} archivos .build")
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
            s3_key = f"{s3_prefix}{file_name}" if s3_prefix else file_name
            
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
    """
    Funcion principal
    """
    logger.info("Iniciando el job de notificaciones Minsait")
    logger.info(f"credenciales de Sybase: {sybase_credentials}")
    logger.info(f"credenciales de MySQL: {mysql_credentials}")
    # obtener estados excluyentes y numeros de pagos vencidos desde sybase
    estados_excluyentes = execute_read_query("""
    SELECT ta.tabla AS s_codigo, ca.codigo AS s_valor
    FROM cobis..cl_tabla ta
    JOIN cobis..cl_catalogo ca ON ta.codigo = ca.tabla
    WHERE ta.tabla = 'tcr_excl_noti_agra'
    UNION
    SELECT pa_nemonico AS codigo, pa_char AS valor
    FROM cobis..cl_parametro
    WHERE pa_nemonico = 'CPAEX'

    """,CONECTION_SYBASE)
    estados_excluyentes.show()

    #cargar estados excluyentes a emi_tmp_parcat en ods
    execute_write_query(estados_excluyentes, "emi_tmp_parcat", CONNECTION_ODS)
    # ejecutar stored procedure para obtener datos de notificaciones de agradecimiento
    execute_stored_procedure("pa_mdp_bnoti_agradec",mysql_credentials,[fecha_proceso])
    # obtener los datos que se obtuvieron del stored procedure
    datos_procesados = execute_read_query("""
            select distinct s_identifica as identificacion ,s_tipo_identifica as tipo_identificacion
            from emi_noti_agradec_pago_inf""",
            CONNECTION_ODS)
    # cargar los datos de identificacion y tipo a sybase
    execute_write_query(datos_procesados,"db_general..tcr_tmp_medio",CONECTION_SYBASE)
    #ejecuta sp que obtiene datos de comunicaciones de los clientes
    execute_sp_sybase("db_tarjeta_bb..pa_tcr_bmedios_agrad",CONECTION_SYBASE,[])
    #obtener los datos de clientes de la tabla temporal creada por el sp
    datos_cliente = execute_read_query("""
        select distinct identificacion,tipo_identificacion,mis,telefono,correo from db_general..tcr_tmp_medio
        """,CONECTION_SYBASE,)
    #cargar los datos de clientes a la tabla emi_tcr_tmp_medio en ods 
    execute_write_query(datos_cliente,"emi_tcr_tmp_medio",CONNECTION_ODS)
    #actualizar los datos de notificacion de agradecimiento con los datos de clientes
    update_notification_data()
    # formatea los datos de la tabla emi_noti_agradec_pago_inf al formato requerido por latinia .build
    formatted_data = format_notification_data()
    formatted_data.show(10, False) 
    #genera los archivos .build con los datos formateados con un maximo de 20,000 registros por archivo
    if not formatted_data.isEmpty():
        archivos_generados = generate_build_files(
            df=formatted_data,
            output_path="/tmp/",
            nemonico=NEMONICO,
            max_records=MAX_RECORDS,
            prefijo=PREFIJO
        )
        bucket_name = S3_BUCKET_NAME
        if not bucket_name:
            logger.error("El nombre del bucket S3 no está configurado. No se subirán archivos.")
            raise ConfigurationError("El nombre del bucket S3 no está configurado.")
        if not archivos_generados:
            logger.warning("No se generaron archivos .build. Verifique los datos de entrada.")
            raise DataProcessingError("No se generaron archivos .build. Verifique los datos de entrada.")

        # sube los archivos generados al s3 configurado
        bucket_name = S3_BUCKET_NAME
        s3_prefix = f"{S3_PREFIX}/{fecha_proceso}/"
        upload_files_to_s3(archivos_generados, bucket_name, s3_prefix)

        logger.info(f"Proceso completado exitosamente. Archivos generados: {len(archivos_generados)}")
    else:
        logger.warning("No se generaron archivos porque no hay datos formateados")

main()