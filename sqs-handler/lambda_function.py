import json
import os
import boto3
import logging
import yaml
import datetime
import requests
import pytz
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


sqs = boto3.client('sqs')
s3 = boto3.client('s3')


BUCKET_NAME = os.getenv("CONFIG_BUCKET_NAME") or "bb-emisormdp-config"


def get_secret(secret_name: str, region_name: str = "us-east-1") -> dict:
    """
    Obtiene un secreto de AWS Secrets Manager.
    
    Args:
        secret_name (str): Nombre del secreto a obtener.
        region_name (str): Región de AWS donde se encuentra el secreto.
        
    Returns:
        dict: Contenido del secreto como un diccionario.
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except Exception as e:
        print(f"Error al obtener el secreto: {e}")
        raise


def create_session(reintentos:int = 3,backoff_factor:float = 0.5,):
    """
    crear una sesion de requests con reintentos
    y manejo de errores para las peticiones HTTP.
    Args:
        reintentos (int): cantidad de reintentos
        backoff_factor (float): factor de retroceso para los reintentos
    """
    session = requests.Session()
    retry_reintentos = Retry(
        total=reintentos,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
        allowed_methods=["POST"],
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry_reintentos)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Lambda-Notification-Service/1.0',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    })
    session.timeout = (10,10)
    return session

def load_yaml_file(config_path):
    """
    Carga del archivo yaml de configuracion
    """
    response = s3.get_object(Bucket=BUCKET_NAME, Key=config_path)
    config_data = response['Body'].read().decode('utf-8')
    print(f"Configuracion cargada desde S3: {BUCKET_NAME}/{config_path}")
    try:
        config = yaml.safe_load(config_data)
        print(f"Configuracion cargada desde {config_path}")
        return config

    except Exception as e:
        print(f"Error al cargar el archivo de configuracion: {e}")
        raise

def config_logger(config_file:dict):
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

def get_proccess_date():

    """
    Obtener la fecha y hora actual
    """
    ecuador_timezone = pytz.timezone("America/Guayaquil")
    return datetime.datetime.now(ecuador_timezone).strftime('%Y-%m-%d %H:%M:%S')

def get_oauth_token(latinia_url_auth, latinia_secret_id_oauth, logger):
    """
    Obtiene el token de autenticación de Latinia
    Args:
        latinia_url_auth (str): URL de autenticación de Latinia
        latinia_secret_id_oauth (str): ID del secreto de OAuth en AWS Secrets Manager
        logger (Logger): Logger configurado para la aplicación
    Returns:
        str: Token de autenticación
    """

    try:
        logger.info("Obteniendo token de OAuth para Latinia")

        secret = get_secret(latinia_secret_id_oauth)
        if not secret:
            logger.error("No se pudo obtener el secreto de Oauth")
            raise ValueError("No se pudo obtener el secreto de OAuth")
        
        logger.info(f"Secreto de OAuth obtenido: {latinia_secret_id_oauth}")

        session = create_session()


        auth_data = {
            "grant_type": secret.get("grant_type", "client_credentials"),
        }

        if "scope" in secret and secret["scope"]:
            auth_data["scope"] = secret["scope"]

        auth_data = {k: v for k, v in auth_data.items() if v}

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        logger.info(f"Enviando petición OAuth con grant_type: {auth_data.get('grant_type')}")
        logger.info(f"Usando Basic Auth con client_id: {secret.get('client_id')[:10]}...")


        response = session.post(
            url=latinia_url_auth,
            data=auth_data,
            headers=headers,
            auth=(secret.get("client_id"), secret.get("client_secret")),
            timeout=30
        )
        logger.info(f"Respuesta de autenticación OAuth: {response.status_code}")
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get("access_token")
        if not access_token:
            logger.error("No se obtuvo el access_token de la respuesta de OAuth")
            raise ValueError("No se pudo obtener el access_token de la respuesta de OAuth")
        

        return access_token
    except Exception as e:
        logger.error(f"Error al obtener el token de OAuth: {e}", exc_info=True)
        raise


def lambda_handler(event,context):
    fecha_proceso = get_proccess_date()
    print(f"Fecha de proceso: {fecha_proceso}")
    enviroment = os.getenv("ENV")
    enviroment = "dev" if enviroment is None else enviroment
    config_file = load_yaml_file(f"config-{enviroment}.yml")
    logger = config_logger(config_file)
    if config_file is None:
        logger.error("No se pudo cargar el archivo de configuracion")
        return {
            "statusCode": 500,
            "headers": {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'codigoError': 60001,
                'message': 'Error al cargar el archivo de configuracion',
                'timestamp': get_proccess_date(),
            })
        }
    logger.info(f"Evento recibido: {event}")
    queue_url = config_file["sqs"]["queue_url"]
    parametro_mantenimiento = config_file["latinia"]["mantenimiento"]
    latinia_url = config_file["latinia"]["url"]
    reintentos = int(config_file["lambda"]["backoff"]["max_retries"])
    backoff_factor = float(config_file["lambda"]["backoff"]["backoff_factor"])
    latinia_secret_id_oauth = config_file["latinia"]["secret_name_oauth"]
    latinia_url_auth = config_file["latinia"]["auth"]

    try:
        if parametro_mantenimiento is True:
            logger.info("El servicio de Latinia esta en mantenimiento, no se procesaran mensajes")
            return {
                "statusCode": 503,
                "headers": {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'codigoError': 60003,
                    'message': 'El servicio de Latinia está en mantenimiento, no se procesarán mensajes',
                    'timestamp': get_proccess_date(),
                })
            }
        get_queue_attributes(queue_url, logger)

        stats = process_all_messages_and_send_to_latinia(
            queue_url=queue_url,
            latinia_url=latinia_url,
            reintentos=reintentos,
            backoff_factor=backoff_factor,
            timeout_seconds= config_file["lambda"]["timeout_seconds"],
            logger=logger,
            latinia_secret_id_oauth=latinia_secret_id_oauth,
            latinia_url_auth=latinia_url_auth,
        )
        return {
            "statusCode": 200,
            "headers": {'Content-Type': 'application/json'},
            'body': json.dumps({
                'codigoError': 0,
                'message': f'Procesamiento completado: {stats["successful_sends"]} exitosos, {stats["failed_sends"]} fallidos',
                'stats': stats,
                'timestamp': get_proccess_date(),
            })
        }
    except Exception as e:
        logger.error(f"Error al procesar los mensajes de la cola: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'codigoError': 60002,
                'message': 'Error al procesar los mensajes de la cola',
                'timestamp': get_proccess_date(),
            })
        }

def send_notification_to_latinia(latinia_url, body, session, timeout_seconds, logger, latinia_secret_id_oauth, latinia_url_auth):
    """
    Envio de notificacion a latinia
    Args:
        latinia_url (string):url de latinia
        body (dict): cuerpo del request previamente validado
        session (Session): sesion de requests con configuracion de reintentos
        timeout_seconds (int): timeout en segundos
        logger: logger configurado
    """
    
    req_session = session
    try:
        logger.info(f"Enviando notificación a Latinia: {latinia_url}")
        logger.info(f"Payload a enviar: {json.dumps(body, indent=2, ensure_ascii=False)}")
        oauth_token = get_oauth_token(latinia_url_auth, latinia_secret_id_oauth, logger)
        
        response = req_session.post(
            url=latinia_url,
            json=body,
            timeout=timeout_seconds,
            headers={"Authorization": f"Bearer {oauth_token}"}
        )
        
        logger.info(f"Respuesta de Latinia: {response.status_code} - {response.text}")
        response.raise_for_status()
        
        # Log de respuesta exitosa
        try:
            response_data = response.json()
            logger.info(f"Respuesta JSON de Latinia: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
        except json.JSONDecodeError:
            logger.info(f"Respuesta de Latinia (texto plano): {response.text}")
            
        return response
        
    except requests.exceptions.ConnectionError as e:
        logger.error("Error de conexión a Latinia después de agotar reintentos", exc_info=True, stack_info=True)
        raise
    except requests.exceptions.Timeout as e:
        logger.error("La solicitud a Latinia ha excedido el tiempo de espera", exc_info=True, stack_info=True)
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP en respuesta de Latinia: {response.status_code} - {response.text}", exc_info=True, stack_info=True)
        raise
    except requests.exceptions.RequestException as e:
        logger.error("Error general al comunicarse con Latinia", exc_info=True, stack_info=True)
        raise

def get_queue_attributes(queue_url, logger):
    """
    Obtiene información sobre la cola SQS
    Args:
        queue_url (string): URL de la cola SQS
        logger: Logger configurado
    """
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['All']
        )
        
        attributes = response.get('Attributes', {})
        logger.info("=== Información de la Cola ===")
        logger.info(f"URL: {queue_url}")
        logger.info(f"Mensajes aproximados: {attributes.get('ApproximateNumberOfMessages', 'N/A')}")
        logger.info(f"Mensajes en vuelo: {attributes.get('ApproximateNumberOfMessagesNotVisible', 'N/A')}")
        logger.info(f"Mensajes en DLQ: {attributes.get('ApproximateNumberOfMessagesDelayed', 'N/A')}")
        logger.info(f"Creada: {attributes.get('CreatedTimestamp', 'N/A')}")
        logger.info(f"Última modificación: {attributes.get('LastModifiedTimestamp', 'N/A')}")
        logger.info("===============================\n")
        
    except Exception as e:
        logger.error(f"Error al obtener atributos de la cola: {e}", exc_info=True)

def process_message_and_send_to_latinia(message, latinia_url, session, timeout_seconds, logger, latinia_secret_id_oauth, latinia_url_auth):
    """
    Procesa un mensaje de SQS y envía su payload a Latinia
    Args:
        message (dict): Mensaje de SQS
        latinia_url (string): URL de la API de Latinia
        session (Session): Sesión de requests configurada
        timeout_seconds (int): Timeout en segundos
        logger: Logger configurado
    Returns:
        bool: True si el envío fue exitoso, False en caso contrario
    """
    try:
        message_id = message.get('MessageId', 'N/A')
        logger.info(f"Procesando mensaje {message_id}")
    
        message_body = message.get('Body', '{}')
        
        try:
            parsed_body = json.loads(message_body)
            logger.info(f"Cuerpo del mensaje parseado: {json.dumps(parsed_body, indent=2, ensure_ascii=False)}")
            
            payload = parsed_body.get('payload')
            if not payload:
                logger.error(f"No se encontró 'payload' en el mensaje {message_id}")
                return False
                
            logger.info(f"Payload extraído del mensaje {message_id}: {json.dumps(payload, indent=2, ensure_ascii=False)}")
            

            response = send_notification_to_latinia(
                latinia_url=latinia_url,
                body=payload, 
                session=session,
                timeout_seconds=timeout_seconds,
                logger=logger,
                latinia_secret_id_oauth=latinia_secret_id_oauth,
                latinia_url_auth=latinia_url_auth
            )
            
            logger.info(f"Mensaje {message_id} enviado exitosamente a Latinia")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Error al parsear el cuerpo del mensaje {message_id} como JSON: {e}")
            logger.error(f"Cuerpo del mensaje: {message_body}")
            return False
            
    except Exception as e:
        logger.error(f"Error al procesar mensaje {message.get('MessageId', 'N/A')}: {e}", exc_info=True)
        return False
    
def process_all_messages_and_send_to_latinia(queue_url, latinia_url, reintentos, backoff_factor, timeout_seconds, logger,latinia_secret_id_oauth, latinia_url_auth):
    """
    Lee todos los mensajes de la cola y envía sus payloads a Latinia
    Args:
        queue_url (string): URL de la cola SQS
        latinia_url (string): URL de la API de Latinia
        reintentos (int): Número de reintentos para la sesión
        backoff_factor (float): Factor de backoff para reintentos
        timeout_seconds (int): Timeout en segundos
        logger: Logger configurado
    Returns:
        dict: Estadísticas del procesamiento
    """
    stats = {
        'total_messages': 0,
        'successful_sends': 0,
        'failed_sends': 0,
        'processed_messages': []
    }
    
    try:
        session = create_session(reintentos, backoff_factor)
        logger.info("Sesión de requests creada con configuración de reintentos")
        
        logger.info(f"Iniciando procesamiento de mensajes de la cola: {queue_url}")
        
        while True:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                MessageAttributeNames=['All'],
                AttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            
            if not messages:
                logger.info("No hay más mensajes en la cola")
                break
            
            # Procesar cada mensaje
            for message in messages:
                stats['total_messages'] += 1
                message_id = message.get('MessageId', 'N/A')
                receipt_handle = message.get('ReceiptHandle')
                
                logger.info(f"--- Procesando mensaje #{stats['total_messages']} (ID: {message_id}) ---")
                
                success = process_message_and_send_to_latinia(
                    message=message,
                    latinia_url=latinia_url,
                    session=session,
                    timeout_seconds=timeout_seconds,
                    logger=logger,
                    latinia_secret_id_oauth=latinia_secret_id_oauth,
                    latinia_url_auth=latinia_url_auth
                )
                
                if success:
                    stats['successful_sends'] += 1
                    
                    try:
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        logger.info(f"Mensaje {message_id} eliminado de la cola")
                    except Exception as e:
                        logger.error(f"Error al eliminar mensaje {message_id} de la cola: {e}")
                        
                else:
                    stats['failed_sends'] += 1
                    logger.error(f"Falló el envío del mensaje {message_id} a Latinia")
                
                stats['processed_messages'].append({
                    'message_id': message_id,
                    'success': success,
                    'timestamp': get_proccess_date()
                })
                
                logger.info(f"--- Fin procesamiento mensaje #{stats['total_messages']} ---\n")
        
        logger.info(f"Procesamiento completado. Estadísticas: {json.dumps(stats, indent=2, ensure_ascii=False)}")
        return stats
        
    except Exception as e:
        logger.error(f"Error durante el procesamiento de mensajes: {e}", exc_info=True)
        raise







