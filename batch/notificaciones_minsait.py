import sys
import traceback
import logging
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import jaydebeapi
import boto3
from botocore.exceptions import ClientError
import json
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import substring, lit, col, count, regexp_replace
from pyspark.sql import DataFrame

# configuracion de logging
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
    'BUCKET_SOURCE', 
    'FECHA_NOTIFICACION',
    "CONFIG_FILENAME",
    "REGION_NAME",
    "DRIVER_MYSQL",
    "JDBC_SYBASE",
]
try:
    args = getResolvedOptions(sys.argv, glue_params)
except Exception as e:
    logger.error(f"Error obtenido en la parametrizacion de glue: {e} ")
    raise ConfigurationError("Argumentos requeridos no encontrados")

JOB_NAME = args['JOB_NAME']
BUCKET_SOURCE = args['BUCKET_SOURCE']
FECHA_NOTIFICACION = args['FECHA_NOTIFICACION']
CONFIG_FILENAME = args['CONFIG_FILENAME']
DRIVER_MYSQL = args["DRIVER_MYSQL"]
JDBC_SYBASE = args["JDBC_SYBASE"]
# JDBC_MYSQL = "jdbc:mysql://{0}:{1}/{2}".format(
#     args['HOST_MYSQL'], 
#     args['PORT_MYSQL'], 
#     args['DATABASE_MYSQL']
# )

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




def read_json_file_from_s3(bucket_url:str):
    """
    Lee un archivo JSON desde S3 y lo convierte en un DataFrame de Spark.
    
    :param bucket_url: URL del bucket S3 donde se encuentra el archivo JSON.
    :return: DataFrame de Spark con los datos del archivo JSON.
    """
    try:
        df = spark.read.json(bucket_url)
        return df
    except AnalysisException as e:
        logger.error(f"Error al leer el archivo JSON desde S3: {e}")
        raise FileNotFoundError("Archivo no encontrado o error en la lectura del archivo JSON")
    

def search_email_by_client_id(numero_identificacion: str, sybase_credentials: dict):
    """
    Busca el correo electrónico asociado a un número de identificación en el DataFrame de clientes.
    
    :param numero_identificacion: Número de identificación del cliente.
    :param sybase_credentials: Credenciales de la base de datos Sybase.
    :return: Correo electrónico del cliente o None si no se encuentra.
    """
    if not numero_identificacion:
        logger.error("Número de identificacion no proporcionado")
        return None
        
    query: str = f"""
        select de.de_ente,de.de_descripcion,de.de_tipo 
        from cobis..cl_ente cl 
        inner join cobis..cl_direccion_email de on de.de_ente= cl.en_ente 
        where cl.en_ced_ruc='{numero_identificacion}' and de.de_tipo in ('E','M')
    """
    
    result = execute_query_sybase(query, sybase_credentials).select("de_ente", "de_descripcion", "de_tipo").collect()
    email = None
    telefono = None
    ccliente = None
    for row in result:
        ccliente = row["de_ente"]
        if row["de_tipo"] == "E":
            email = row["de_descripcion"]
        elif row["de_tipo"] == "M":
            telefono = row["de_descripcion"]
    return {
        "email": {"ccliente": ccliente, "valor": email},
        "telefono": {"ccliente": ccliente, "valor": telefono}
    }
def execute_query_sybase(query: str, sybase_credentials: dict) -> DataFrame:
    """
    Ejecuta una consulta SQL en la base de datos Sybase y devuelve el resultado como un DataFrame de Spark.
    
    :param query: Consulta SQL a ejecutar.
    :param sybase_credentials: Credenciales de la base de datos Sybase.
    :return: DataFrame de Spark con los resultados de la consulta.
    """
    try:
        # Construir la URL JDBC para Sybase/SQL Server
        jdbc_url = "jdbc:sqlserver://{0}:{1};databaseName={2};encrypt=false;trustServerCertificate=true".format(
            sybase_credentials["host"],
            sybase_credentials["port"],
            sybase_credentials.get("database", "db_tarjeta_bb")
        )
        
        return spark.read \
        .format("jdbc") \
        .option("driver", JDBC_SYBASE) \
        .option("url", jdbc_url) \
        .option("query", query) \
        .option("user", sybase_credentials['username']) \
        .option("password", sybase_credentials['password']) \
        .load()

    except Exception as e:
        logger.error(f"Error al ejecutar la consulta en Sybase: {e}")
        raise DataProcessingError("Error al procesar los datos desde Sybase")
    

# Funcion para ejecutar Store procedure o querys (SELECT , UPDATE etc) y retorna data en caso de ser necesario o None



def execute_pa_tcr_cdatclidiferidocons(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_user: str,
    jdbc_password: str,
    jdbc_driver: str,
    e_tarjeta: str,
    e_valor: str,
    e_fecha_hora: str,
    e_cta: str,
    e_cod_comercio: int,
    e_tip_comercio: str,
    e_tip_mensaje: str,
    e_tip_aviso: str,
    e_canal: str,
    e_cod_pais: str,
    e_plazo: str,
    e_empresa: str,
    e_cons_inter: str,
    e_cob_recurr: str
) -> dict:
    """
    Ejecuta el procedimiento almacenado 'dbo.pa_tcr_cdatclidiferidocons'
    en SQL Server usando jaydebeapi y devuelve los parámetros de salida.

    Args:
        spark (SparkSession): La sesión de Spark actual.
        jdbc_url (str): La URL de conexión JDBC a la base de datos.
        jdbc_user (str): El nombre de usuario para la conexión JDBC.
        jdbc_password (str): La contraseña para la conexión JDBC.
        jdbc_driver (str): La clase del driver JDBC (ej. "com.microsoft.sqlserver.jdbc.SQLServerDriver").
        e_tarjeta (str): Número de tarjeta enmascarado.
        e_valor (str): Monto de la transacción.
        e_fecha_hora (str): Fecha de la transacción (Formato TO MMDD).
        e_cta (str): Número de cuenta secuencial de la tarjeta.
        e_cod_comercio (int): Código de comercio.
        e_tip_comercio (str): Tipo de Comercio.
        e_tip_mensaje (str): Tipo de mensaje (aut 0100, 0200 reverso 0400).
        e_tip_aviso (str): Tipo de Aviso.
        e_canal (str): Código Canal.
        e_cod_pais (str): Código País ISO.
        e_plazo (str): Tiempo Plazo.
        e_empresa (str): Establecimiento.
        e_cons_inter (str): Consumo por Internet.
        e_cob_recurr (str): Cobro Recurrente.

    Returns:
        dict: Un diccionario que contiene los valores de los parámetros de salida del SP.
              Retorna un diccionario vacío si ocurre un error.
    """
    conn = None
    output_params = {}

    try:
        print(f"Conectando a la base de datos usando jaydebeapi...")
        print(f"URL: {jdbc_url}")
        print(f"Usuario: {jdbc_user}")
        print(f"Driver: {jdbc_driver}")
        
        # Ruta al JAR del driver JDBC
        jar_path = "/home/hadoop/workspace/resources/mssql-jdbc-12.10.1.jre11.jar"
        
        # Establecer conexión usando jaydebeapi
        conn = jaydebeapi.connect(
            jclassname=jdbc_driver,
            url=jdbc_url,
            driver_args=[jdbc_user, jdbc_password],
            jars=jar_path
        )
        
        print("Conexión establecida exitosamente con jaydebeapi")
        
        # Crear cursor
        cursor = conn.cursor()
        
        # Construir la llamada al procedimiento almacenado
        # Para SQL Server con parámetros de salida, usamos la sintaxis EXECUTE con variables OUTPUT
        sp_call = """
        DECLARE @s_nombre_cliente VARCHAR(255),
                @s_transaccion VARCHAR(255),
                @s_des_comercio VARCHAR(255),
                @s_des_pais VARCHAR(255),
                @s_det_pais VARCHAR(255),
                @s_identificacion VARCHAR(255),
                @s_nombre_titular VARCHAR(255),
                @s_nro_tarjeta VARCHAR(255),
                @s_des_tiptar VARCHAR(255),
                @s_monto VARCHAR(255),
                @s_plazo VARCHAR(255),
                @s_fecha VARCHAR(255),
                @s_hora VARCHAR(255),
                @s_det_comercio VARCHAR(255),
                @s_det_plazo VARCHAR(255),
                @s_telefono VARCHAR(255),
                @s_mail VARCHAR(255),
                @s_error CHAR(2),
                @s_mensaje VARCHAR(255),
                @s_servicio VARCHAR(255),
                @s_canal VARCHAR(255),
                @s_des_canal VARCHAR(255),
                @s_des_estado VARCHAR(255),
                @s_cliente INT,
                @s_tipropietario CHAR(1),
                @s_mail_titular VARCHAR(255),
                @s_telefono_titular VARCHAR(255),
                @s_servicio_titular VARCHAR(255);

        EXECUTE dbo.pa_tcr_cdatclidiferidocons
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            @s_nombre_cliente OUTPUT,
            @s_transaccion OUTPUT,
            @s_des_comercio OUTPUT,
            @s_des_pais OUTPUT,
            @s_det_pais OUTPUT,
            @s_identificacion OUTPUT,
            @s_nombre_titular OUTPUT,
            @s_nro_tarjeta OUTPUT,
            @s_des_tiptar OUTPUT,
            @s_monto OUTPUT,
            @s_plazo OUTPUT,
            @s_fecha OUTPUT,
            @s_hora OUTPUT,
            @s_det_comercio OUTPUT,
            @s_det_plazo OUTPUT,
            @s_telefono OUTPUT,
            @s_mail OUTPUT,
            @s_error OUTPUT,
            @s_mensaje OUTPUT,
            @s_servicio OUTPUT,
            @s_canal OUTPUT,
            @s_des_canal OUTPUT,
            @s_des_estado OUTPUT,
            @s_cliente OUTPUT,
            @s_tipropietario OUTPUT,
            @s_mail_titular OUTPUT,
            @s_telefono_titular OUTPUT,
            @s_servicio_titular OUTPUT;

        SELECT @s_nombre_cliente AS s_nombre_cliente,
               @s_transaccion AS s_transaccion,
               @s_des_comercio AS s_des_comercio,
               @s_des_pais AS s_des_pais,
               @s_det_pais AS s_det_pais,
               @s_identificacion AS s_identificacion,
               @s_nombre_titular AS s_nombre_titular,
               @s_nro_tarjeta AS s_nro_tarjeta,
               @s_des_tiptar AS s_des_tiptar,
               @s_monto AS s_monto,
               @s_plazo AS s_plazo,
               @s_fecha AS s_fecha,
               @s_hora AS s_hora,
               @s_det_comercio AS s_det_comercio,
               @s_det_plazo AS s_det_plazo,
               @s_telefono AS s_telefono,
               @s_mail AS s_mail,
               @s_error AS s_error,
               @s_mensaje AS s_mensaje,
               @s_servicio AS s_servicio,
               @s_canal AS s_canal,
               @s_des_canal AS s_des_canal,
               @s_des_estado AS s_des_estado,
               @s_cliente AS s_cliente,
               @s_tipropietario AS s_tipropietario,
               @s_mail_titular AS s_mail_titular,
               @s_telefono_titular AS s_telefono_titular,
               @s_servicio_titular AS s_servicio_titular;
        """
        
        # Parámetros de entrada para el procedimiento almacenado
        input_params = [
            e_tarjeta,
            e_valor,
            e_fecha_hora,
            e_cta,
            e_cod_comercio,
            e_tip_comercio,
            e_tip_mensaje,
            e_tip_aviso,
            e_canal,
            e_cod_pais,
            e_plazo,
            e_empresa,
            e_cons_inter,
            e_cob_recurr
        ]
        
        print(f"Ejecutando procedimiento almacenado con parámetros: {input_params}")
        
        # Ejecutar el procedimiento almacenado
        cursor.execute(sp_call, input_params)
        
        # Obtener los resultados (parámetros de salida)
        results = cursor.fetchone()
        
        if results:
            # Mapear los resultados a un diccionario
            column_names = [desc[0] for desc in cursor.description]
            output_params = dict(zip(column_names, results))
            
            print("Procedimiento almacenado ejecutado exitosamente")
            print(f"Parámetros de salida obtenidos: {len(output_params)} campos")
        else:
            print("No se obtuvieron resultados del procedimiento almacenado")
            
        # Cerrar cursor
        cursor.close()

    except jaydebeapi.Error as e:
        error_msg = f"Error de jaydebeapi al ejecutar el procedimiento almacenado: {e}"
        print(error_msg)
        import traceback
        print(f"Traceback completo: {traceback.format_exc()}")
        output_params = {"error": error_msg}
        
    except Exception as e:
        error_msg = f"Error general al ejecutar el procedimiento almacenado 'dbo.pa_tcr_cdatclidiferidocons': {e}"
        print(error_msg)
        import traceback
        print(f"Traceback completo: {traceback.format_exc()}")
        output_params = {"error": error_msg}
        
    finally:
        # Asegúrate de cerrar la conexión
        if conn is not None:
            try:
                conn.close()
                print("Conexión jaydebeapi cerrada")
            except Exception as e:
                print(f"Error al cerrar la conexión jaydebeapi: {e}")
    
    return output_params
def execute_pa_tce_csecnotilatinia(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_user: str,
    jdbc_password: str,
    jdbc_driver: str,
    e_tarjeta: str = '',
    e_cuenta: str = ''
) -> dict:
    """
    Ejecuta el procedimiento almacenado 'dbo.pa_tce_csecnotilatinia'
    en SQL Server usando jaydebeapi y devuelve los parámetros de salida.
    """
    conn = None
    output_params = {}

    try:
        print(f"Conectando para ejecutar pa_tce_csecnotilatinia...")
        
        # Ruta al JAR del driver JDBC
        jar_path = "/home/hadoop/workspace/resources/mssql-jdbc-12.10.1.jre11.jar"
        
        # Establecer conexión usando jaydebeapi
        conn = jaydebeapi.connect(
            jclassname=jdbc_driver,
            url=jdbc_url,
            driver_args=[jdbc_user, jdbc_password],
            jars=jar_path
        )
        
        cursor = conn.cursor()
        
        # Construir la llamada al procedimiento almacenado con parámetros OUTPUT
        sp_call = """
        DECLARE @s_secuencial VARCHAR(255),
                @s_ente INT,
                @s_claveunica VARCHAR(255);

        EXECUTE dbo.pa_tce_csecnotilatinia
            ?, ?,
            @s_secuencial OUTPUT,
            @s_ente OUTPUT,
            @s_claveunica OUTPUT;

        SELECT @s_secuencial AS s_secuencial,
               @s_ente AS s_ente,
               @s_claveunica AS s_claveunica;
        """
        
        # Ejecutar el procedimiento
        cursor.execute(sp_call, [e_tarjeta, e_cuenta])
        
        # Obtener resultados
        results = cursor.fetchone()
        
        if results:
            column_names = [desc[0] for desc in cursor.description]
            output_params = dict(zip(column_names, results))
            print("pa_tce_csecnotilatinia ejecutado exitosamente")
        
        cursor.close()

    except Exception as e:
        error_msg = f"Error al ejecutar pa_tce_csecnotilatinia: {e}"
        print(error_msg)
        output_params = {"error": error_msg}
        
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
    
    return output_params

def execute_pa_tcr_talrlimcupo(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_user: str,
    jdbc_password: str,
    jdbc_driver: str,
    e_tarjeta: str,
    e_mesanio_hora: str,
    e_cta: str,
    e_cod_resp_iso: str,
    e_cod_resp_interno: str,
    e_cupo_disponible: str,
    e_cod_comercio: int,
    e_tip_comercio: str,
    e_empresa: str
) -> dict:
    """
    Ejecuta el procedimiento almacenado 'dbo.pa_tcr_talrlimcupo'
    en SQL Server usando jaydebeapi y devuelve los parámetros de salida.
    """
    conn = None
    output_params = {}

    try:
        print(f"Conectando para ejecutar pa_tcr_talrlimcupo...")
        
        jar_path = "/home/hadoop/workspace/resources/mssql-jdbc-12.10.1.jre11.jar"
        
        conn = jaydebeapi.connect(
            jclassname=jdbc_driver,
            url=jdbc_url,
            driver_args=[jdbc_user, jdbc_password],
            jars=jar_path
        )
        
        cursor = conn.cursor()
        
        # Construir la llamada con todos los parámetros OUTPUT
        sp_call = """
        DECLARE @s_servicio VARCHAR(255), @s_canal VARCHAR(255), @s_desc_canal VARCHAR(255),
                @s_operacion CHAR(1), @s_producto INT, @s_cliente INT,
                @s_nombre_titular VARCHAR(255), @s_nombre VARCHAR(255), @s_identificacion VARCHAR(255),
                @s_cupo VARCHAR(255), @s_nro_tarjeta VARCHAR(255), @s_des_tiptar VARCHAR(255),
                @s_motivo VARCHAR(255), @s_telefono VARCHAR(255), @s_mail VARCHAR(255),
                @s_fecha VARCHAR(255), @s_hora VARCHAR(255), @s_error CHAR(1),
                @s_mensaje VARCHAR(255), @s_des_comercio VARCHAR(255), @s_det_comercio VARCHAR(255);

        EXECUTE dbo.pa_tcr_talrlimcupo
            ?, ?, ?, ?, ?, ?, ?, ?, ?,
            @s_servicio OUTPUT, @s_canal OUTPUT, @s_desc_canal OUTPUT,
            @s_operacion OUTPUT, @s_producto OUTPUT, @s_cliente OUTPUT,
            @s_nombre_titular OUTPUT, @s_nombre OUTPUT, @s_identificacion OUTPUT,
            @s_cupo OUTPUT, @s_nro_tarjeta OUTPUT, @s_des_tiptar OUTPUT,
            @s_motivo OUTPUT, @s_telefono OUTPUT, @s_mail OUTPUT,
            @s_fecha OUTPUT, @s_hora OUTPUT, @s_error OUTPUT,
            @s_mensaje OUTPUT, @s_des_comercio OUTPUT, @s_det_comercio OUTPUT;

        SELECT @s_servicio AS s_servicio, @s_canal AS s_canal, @s_desc_canal AS s_desc_canal,
               @s_operacion AS s_operacion, @s_producto AS s_producto, @s_cliente AS s_cliente,
               @s_nombre_titular AS s_nombre_titular, @s_nombre AS s_nombre, @s_identificacion AS s_identificacion,
               @s_cupo AS s_cupo, @s_nro_tarjeta AS s_nro_tarjeta, @s_des_tiptar AS s_des_tiptar,
               @s_motivo AS s_motivo, @s_telefono AS s_telefono, @s_mail AS s_mail,
               @s_fecha AS s_fecha, @s_hora AS s_hora, @s_error AS s_error,
               @s_mensaje AS s_mensaje, @s_des_comercio AS s_des_comercio, @s_det_comercio AS s_det_comercio;
        """
        
        input_params = [
            e_tarjeta, e_mesanio_hora, e_cta, e_cod_resp_iso, e_cod_resp_interno,
            e_cupo_disponible, e_cod_comercio, e_tip_comercio, e_empresa
        ]
        
        cursor.execute(sp_call, input_params)
        results = cursor.fetchone()
        
        if results:
            column_names = [desc[0] for desc in cursor.description]
            output_params = dict(zip(column_names, results))
            print("pa_tcr_talrlimcupo ejecutado exitosamente")
        
        cursor.close()

    except Exception as e:
        error_msg = f"Error al ejecutar pa_tcr_talrlimcupo: {e}"
        print(error_msg)
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        output_params = {"error": error_msg}
        
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                print(f"Error al cerrar conexión: {e}")
    
    return output_params

def main():
    """
    Función principal del job de Glue.
    """
    try:
        sybase_credentials = get_secret(args['SYBASE_SECRET'], args['REGION_NAME'])
        if sybase_credentials["host"] == "localhost":
            sybase_credentials["host"] = "sybase_database"
            sybase_credentials["port"] = 1433

        SYBASE_URL = "jdbc:sqlserver://{0}:{1};databaseName={2};encrypt=false;trustServerCertificate=true".format(
            sybase_credentials["host"], 
            sybase_credentials["port"], 
            "db_tarjeta_bb"
        )
        SYBASE_USER = sybase_credentials["username"]
        SYBASE_PASSWORD = sybase_credentials["password"]
        input_params = {
        "e_tarjeta": "************1234",
        "e_valor": "123.45",
        "e_fecha_hora": "07191500", # MMddHHmm
        "e_cta": "1234567890123456789012345678901234567890",
        "e_cod_comercio": 1001,
        "e_tip_comercio": "ECOM",
        "e_tip_mensaje": "0100",
        "e_tip_aviso": "A",
        "e_canal": "WEB",
        "e_cod_pais": "ECU",
        "e_plazo": "003",
        "e_empresa": "MiTiendaOnline",
        "e_cons_inter": "S",
        "e_cob_recurr": "N"
        }
        output_data = execute_pa_tcr_cdatclidiferidocons(
            spark,
            SYBASE_URL,
            SYBASE_USER,
            SYBASE_PASSWORD,
            JDBC_SYBASE,
            **input_params # Desempaqueta el diccionario de parámetros de entrada
        )
        if output_data:
            print("\n--- Parámetros de Salida Obtenidos ---")
            for key, value in output_data.items():
                print(f"{key}: {value}")
        else:
            print("\nNo se pudieron obtener los parámetros de salida o hubo un error.")


    except GlueJobError as e:
        logger.error(f"Error en el job de Glue: {e}")
        sys.exit(1)



if __name__ == "__main__":
    main()
    job.commit()  
