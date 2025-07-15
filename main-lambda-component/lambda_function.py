import boto3
import json
import yaml
from request_validation import validate_request
from dotenv import load_dotenv
import os
import botocore
from utils.utils import get_proccess_date,create_session,validate_config
import requests
import logging
import pyodbc

import datetime
import uuid

load_dotenv()



def load_yaml_file(config_path):
    """
    Carga del archivo yaml de configuracion
    """

    try:
        with open(config_path,'r') as file:
            config = yaml.safe_load(file)
            print(f"Configuracion cargada desde {config_path}")
            return config
        
    except Exception as e:
        print(f"Error al cargar el archivo de configuracion: {e}")
        raise



def  config_logger(config_file:dict):
    """
    configuracion del logger de la aplicacion lambda
    Args:
        config_file (dict): archivo de configuracion
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(config_file["logging"]["format"])
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    error_handler = logging.StreamHandler()
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    warn_handler = logging.StreamHandler()
    warn_handler.setLevel(logging.WARNING)
    warn_handler.setFormatter(formatter)
    logger.addHandler(warn_handler)
    logger.info("Logger configurado correctamente")

    return logger

def crear_conexion_db(logger):
    """Establece una conexión con la base de datos del ODS."""
    try:
        server = os.environ['DB_SERVER']
        database = os.environ['DB_DATABASE']
        username = os.environ['DB_USERNAME']
        password = os.environ['DB_PASSWORD']
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        logger.info(f"Conectando a la base de datos: {database}")
        conn = pyodbc.connect(conn_str, autocommit=True)
        logger.info("Conexión a la base de datos establecida.")
        return conn
    except Exception as e:
        logger.error(f"Error fatal al conectar con la base de datos: {e}", exc_info=True)
        raise

def obtener_y_formatear_notificacion(body_sat: dict, logger: logging.Logger) -> dict:
    """
    Orquesta el proceso completo:
    1. Se conecta a la base de datos.
    2. Ejecuta el Stored Procedure con todos los parámetros mapeados.
    3. Construye el objeto final (request) para enviar a Latinia.
    """
        
    conn = None
    try:
        conn = crear_conexion_db(logger)
        cursor = conn.cursor()

        # Mapeo completo de parámetros según el documento técnico (sección 2.2.2.1)
        params_sp = (
            body_sat.get("Número de tarjeta (ofuscado)"),
            body_sat.get("Importe en moneda del titular"),
            body_sat.get("Fecha y hora de la operación"),
            body_sat.get("Cuenta"),
            body_sat.get("Código de comercio"),
            "TIPO_COMERCIO_DEFAULT", # VALOR PENDIENTE
            "TIPO_MENSAJE_DEFAULT",  # VALOR PENDIENTE
            body_sat.get("Código de notificación"),
            body_sat.get("Medio de comunicación"),
            body_sat.get("país del comercio"),
            body_sat.get("Número de cuotas"),
            body_sat.get("Código de la entidad"),
            "N", # valor estático para diferidos
            "?"  # VALOR PENDIENTE
        )
        
        sp_name = 'db_tarjeta_bb..pa_tcr_cdatclidiferidocons'
        sql_exec = f"EXEC {sp_name} {'?,' * (len(params_sp)-1)}?"
        
        logger.info(f"Ejecutando SP: {sp_name}")
        cursor.execute(sql_exec, params_sp)
        row = cursor.fetchone()
        
        if not row:
            raise ValueError(f"SP '{sp_name}' no devolvió resultados para la tarjeta {params_sp[0]}.")

        logger.info("Datos recibidos del SP. Construyendo request para Latinia.")
        
        nemónico = body_sat.get('Código del evento')
        
        data_latinia = {
            "des_transaccion": row.descripcion_transaccion,
            "tipotrj": "VISA",
            "valor": str(body_sat.get("Importe en moneda del titular")),
            "numtrj": body_sat.get("Número de tarjeta (ofuscado)"),
            "identi": body_sat.get("Identificador del cliente"),
            "plazo": str(body_sat.get("Número de cuotas")),
            "nombre_cliente": row.nombre_cliente,
            "nombre_titular": row.nombre_cliente,
            "fecha": datetime.datetime.strptime(body_sat.get("Fecha y hora de la operación"), '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d'),
            "hora": datetime.datetime.strptime(body_sat.get("Fecha y hora de la operación"), '%Y-%m-%dT%H:%M:%S').strftime('%H:%M:%S'),
        }

        request_latinia = {
            "header": {
                "id": f"{nemónico}{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}",
                "refCompany": "BOLIVARIANO", "refService": nemónico,
                "keyValue": body_sat.get("Identificador del cliente"), "refMsgLabel": "Avisos24"
            },
            "info": {"loginEnterprise": "BOLIVARIANO", "refContract": nemónico},
            "data": data_latinia,
            "addresses": []
        }
        
        if row.email:
            request_latinia["addresses"].append({"className": "Email", "type": "to", "ref": row.email})
        if row.telefono:
            request_latinia["addresses"].append({"className": "phone", "type": "to", "ref": row.telefono})
            
        logger.info(f"Request para Latinia construido exitosamente para el nemónico: {nemónico}")
        return request_latinia

    finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos cerrada.")

def preparar_notificacion(body_sat: dict, logger: logging.Logger) -> dict:
    """
    [RENOMBRADA] Orquesta la conexión a la BD, ejecuta el SP y construye el request para Latinia.
    """
    logger.info("Iniciando preparación de la notificación...")
    conn = None
    try:
        conn = crear_conexion_db(logger)
        cursor = conn.cursor()

        params_sp = (
            body_sat.get("Número de tarjeta (ofuscado)"), body_sat.get("Importe en moneda del titular"),
            body_sat.get("Fecha y hora de la operación"), body_sat.get("Cuenta"), body_sat.get("Código de comercio"),
            "TIPO_COMERCIO_DEFAULT", "TIPO_MENSAJE_DEFAULT", body_sat.get("Código de notificación"),
            body_sat.get("Medio de comunicación"), body_sat.get("país del comercio"), body_sat.get("Número de cuotas"),
            body_sat.get("Código de la entidad"), "N", "?"
        )
        
        sp_name = 'db_tarjeta_bb..pa_tcr_cdatclidiferidocons'
        sql_exec = f"EXEC {sp_name} {'?,' * (len(params_sp)-1)}?"
        
        logger.info(f"Ejecutando SP: {sp_name}")
        cursor.execute(sql_exec, params_sp)
        row = cursor.fetchone()
        
        if not row:
            raise ValueError(f"SP '{sp_name}' no devolvió resultados.")

        logger.info("Datos recibidos del SP. Construyendo request para Latinia.")
        
        nemónico = body_sat.get('Código del evento')
        fecha_op = datetime.datetime.strptime(body_sat.get("Fecha y hora de la operación"), '%Y-%m-%dT%H:%M:%S')
        
        data_latinia = {
            "des_transaccion": row.descripcion_transaccion, "tipotrj": "VISA",
            "valor": str(body_sat.get("Importe en moneda del titular")), "numtrj": body_sat.get("Número de tarjeta (ofuscado)"),
            "identi": body_sat.get("Identificador del cliente"), "plazo": str(body_sat.get("Número de cuotas")),
            "nombre_cliente": row.nombre_cliente, "nombre_titular": row.nombre_cliente,
            "fecha": fecha_op.strftime('%Y-%m-%d'), "hora": fecha_op.strftime('%H:%M:%S'),
        }

        request_latinia = {
            "header": {
                "id": f"{nemónico}{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}",
                "refCompany": "BOLIVARIANO", "refService": nemónico,
                "keyValue": body_sat.get("Identificador del cliente"), "refMsgLabel": "Avisos24"
            },
            "info": {"loginEnterprise": "BOLIVARIANO", "refContract": nemónico},
            "data": data_latinia, "addresses": []
        }
        
        if row.email:
            request_latinia["addresses"].append({"className": "Email", "type": "to", "ref": row.email})
        if row.telefono:
            request_latinia["addresses"].append({"className": "phone", "type": "to", "ref": row.telefono})
            
        logger.info(f"Request para Latinia construido exitosamente para el nemónico: {nemónico}")
        return request_latinia
    finally:
        if conn:
            conn.close()
            logger.info("Conexión a la base de datos cerrada.")


def lambda_handler(event, context):
    logger = None
    request_a_enviar = None
    try:
        # 1. Configuración y Validación
        fecha_proceso = get_proccess_date()
        print(f"Fecha de proceso: {fecha_proceso}")
        error, body_sat = validate_request(event)
        if error:
            return {"statusCode": 400, "headers": {"Content-Type": "application/json"}, 'body': json.dumps({'error': 'Error en la validacion de datos', 'message': error})}

        enviroment = os.getenv("ENV", "dev")
        config_file = load_yaml_file(f"config-{enviroment}.yml")
        validate_config(config_file)
        logger = config_logger(config_file)
        ssm = boto3.client('ssm')
        
        # 2. Lógica de Negocio
        request_a_enviar = preparar_notificacion(body_sat, logger)
        
        # 3. Decisión de Ruta de Envío
        queue_url = ssm.get_parameter(Name=config_file["sqs"]["queue_url"])['Parameter']['Value']
        parametro_mantenimiento = ssm.get_parameter(Name=config_file["latinia"]["mantenimiento"])['Parameter']['Value']
        latinia_url = ssm.get_parameter(Name=config_file["latinia"]["url"])['Parameter']['Value']

        if parametro_mantenimiento == "True":
            logger.warning("Latinia en mantenimiento. Mensaje será encolado.")
            message_id = send_notification_to_queue(queue_url, request_a_enviar)
            invoke_retry_handler(logger)
            return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, 'body': json.dumps({'message': 'Latinia en mantenimiento, mensaje encolado.', 'messageId': message_id})}
        else:
            logger.info("Latinia disponible. Enviando notificación.")
            timeout_seconds = int(config_file["latinia"]["timeout_seconds"])
            session = create_session()
            send_notification_to_latinia(latinia_url, request_a_enviar, session, timeout_seconds)
            return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, 'body': json.dumps({'message': 'Notificacion enviada a Latinia'})}

    # 4. Manejo de Excepciones
    except requests.exceptions.RequestException as latinia_error:
        if logger: logger.error(f"Error de comunicación con Latinia. El mensaje será encolado.", exc_info=True)
        if request_a_enviar:
            config_file = load_yaml_file(f"config-{os.getenv('ENV', 'dev')}.yml")
            ssm_client = boto3.client('ssm')
            queue_url = ssm_client.get_parameter(Name=config_file["sqs"]["queue_url"])['Parameter']['Value']
            send_notification_to_queue(queue_url, request_a_enviar)
            invoke_retry_handler(logger)
            return {"statusCode": 502, "headers": {"Content-Type": "application/json"}, 'body': json.dumps({'codigoError': 69, 'message': 'Error al comunicarse con Latinia. Mensaje encolado para reintento.'})}
        else:
            return {"statusCode": 500, "headers": {"Content-Type": "application/json"}, 'body': json.dumps({'codigoError': 69, 'message': 'Error al comunicarse con Latinia, pero no se pudo construir el mensaje para encolar.'})}
    except Exception as e:
        if logger: logger.critical(f"Error no esperado en lambda_handler: {e}", exc_info=True)
        return {"statusCode": 500, "headers": {"Content-Type": "application/json"}, 'body': json.dumps({'codigoError': 9999, 'message': 'Ocurrió un error inesperado en el servidor.'})}
    
def send_notification_to_queue(queue_url, body):
    """
    [MODIFICADA] Envia la notificación a la cola SQS, añadiendo el contador de intentos.
    """
    sqs = boto3.client('sqs')
    try:
        mensaje_para_cola = {
            "payload": body,
            "intentos": 0
        }
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(mensaje_para_cola) 
        )
        print("Respuesta de la cola:", response)
        return response["MessageId"]
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error al enviar mensaje a SQS: {e}", exc_info=True)
        raise


def invoke_retry_handler(logger):
    """
    [NUEVO] Invoca la Lambda de reintentos de forma asíncrona.
    """
    try:
        lambda_client = boto3.client('lambda')
        # Nombre de la segunda Lambda que procesará la cola.
        retry_lambda_name = os.environ.get('RETRY_LAMBDA_NAME', 'notificaciones_procesador_cola_reintentos')
        
        logger.info(f"Invocando la Lambda de reintentos: {retry_lambda_name}")
        lambda_client.invoke(
            FunctionName=retry_lambda_name,
            InvocationType='Event'  # 'Event' para invocar y no esperar respuesta (asíncrono)
        )
    except botocore.exceptions.ClientError as e:
        logger.error(f"No se pudo invocar la Lambda de reintentos: {e}", exc_info=True)
        
    
def send_notification_to_latinia(latinia_url, body, session, timeout_seconds):
    """Envio de notificacion a latinia."""
    try:
        response = session.post(
            url=latinia_url,
            json=body,
            timeout=timeout_seconds
        )
        response.raise_for_status() 
        print("Respuesta de Latinia:", response.json())
    except requests.exceptions.RequestException as e:
        logging.error(f"Error en la petición a Latinia: {e}", exc_info=True)
        raise

