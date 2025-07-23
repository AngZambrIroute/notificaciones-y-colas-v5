import json
import os
import boto3
import requests
import logging
import yaml
import datetime
import pytz




def validate_config(config):
    required_structure = {
        'lambda': ['timeout_seconds', 'env', 'backoff'],
        'latinia': ['url', 'mantenimiento'],
        'sqs': ['queue_url'],
        'logging': {
            'level': str,
            'format': str,
            'handlers': dict,
            'root': dict,
        },
    }

    def validate_section(section, requirements):
        if isinstance(requirements, list):
            return all(key in section for key in requirements)
        elif isinstance(requirements, dict):
            return all(k in section and isinstance(section[k], v) for k, v in requirements.items())
        return False

    for section, keys in required_structure.items():
        if section not in config:
            raise ValueError(f"Missing section: {section}")
        if not validate_section(config[section], keys):
            raise ValueError(f"Invalid or missing keys in section: {section}")

    # Validar campos internos específicos
    backoff_required = ['max_retries', 'delay_seconds',
                        'backoff_factor', 'max_delay_seconds']
    if not all(k in config['lambda']['backoff'] for k in backoff_required):
        raise ValueError("Missing keys in lambda.backoff")

    print("Archivo YAML válido.")

def get_process_date():
    """
    Obtener la fecha y hora actual
    """
    ecuador_timezone = pytz.timezone("America/Guayaquil")
    return datetime.datetime.now(ecuador_timezone).strftime('%Y-%m-%d %H:%M:%S')

def load_yaml_file(config_path):
    """
    carga del archivo yaml de configuracion.
    """
    try:
        with open(config_path,'r') as file:
            config = yaml.safe_load(file)
            print(f"Archivo de configuracion cargado: {config_path}")
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


# Estos valores se deben configurar como variables de entorno en la Lambda.
MAX_REINTENTOS = int(os.environ.get('MAX_REINTENTOS', 10))
QUEUE_URL = os.environ.get('QUEUE_URL') 
DLQ_URL = os.environ.get('DLQ_URL')
LATINIA_URL_PARAM_NAME = os.environ.get('LATINIA_URL_PARAM_NAME')
LATINIA_TIMEOUT = int(os.environ.get('LATINIA_TIMEOUT', 10))

# --- Clientes de AWS ---
sqs = boto3.client('sqs')
ssm = boto3.client('ssm')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_requests_session():
    """Crea una sesión de requests con reintentos para las llamadas a Latinia."""
    
    session = requests.Session()
  
    return session

def send_notification_to_latinia(latinia_url, body, session):
    """Función de ayuda para enviar la notificación a Latinia."""
    response = session.post(url=latinia_url, json=body, timeout=LATINIA_TIMEOUT)
    response.raise_for_status() 
    logger.info(f"Respuesta de Latinia en reintento: {response.text}")

def lambda_handler(event, context):
    """
    Handler para la Lambda de reintentos, desencadenada por SQS.
    Procesa un lote de mensajes recibidos en el 'event'.
    """
    fecha_proceso = get_process_date()
    logger.info(f"Iniciando procesamiento de {len(event.get('Records', []))} mensajes de la cola.")
    
    enviroment = os.getenv("ENV")
    enviroment = "dev" if enviroment is None else enviroment
    CONFIG_FILE = load_yaml_file(f"config-{enviroment}.yml")
    if CONFIG_FILE is None:
        logger.error("Error al cargar el archivo de configuracion, Abortando procesamiento.")
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
                'codigoError':7011,
                'message':'Error al cargar el archivo de configuracion',
                'messageId':'',
                'timestamp':fecha_proceso,
            })
        }
    

    
    # Obtenemos la URL de Latinia 
    try:
        validate_config(CONFIG_FILE)
        print("Archivo de configuracion valido")
        logger = config_logger(CONFIG_FILE)
        latinia_url = CONFIG_FILE["latinia"]["url"]
    except Exception as e:
        logger.error(f"CRÍTICO: No se pudo obtener la URL de Latinia. No se pueden procesar mensajes. Error: {e}")
        # Devolvemos todos los mensajes como fallidos
        return {'batchItemFailures': [{'itemIdentifier': record['messageId']} for record in event.get('Records', [])]}

    session = create_requests_session()
    
    batch_item_failures = []

    for record in event.get('Records', []):
        message_id = record['messageId']
        try:
            # El cuerpo del mensaje es un string JSON
            message_body = json.loads(record['body'])
            
            notificacion_payload = message_body.get('payload')
            intentos = message_body.get('intentos', 0)

            if notificacion_payload:
                logger.info(f"Procesando mensaje ID: {message_id} (Intento #{intentos + 1})")

               
                if intentos >= MAX_REINTENTOS:
                    logger.warning(f"Mensaje {message_id} ha alcanzado el límite de {MAX_REINTENTOS} reintentos. Moviendo a DLQ.")
                    sqs.send_message(QueueUrl=DLQ_URL, MessageBody=json.dumps(notificacion_payload))
                   
                    continue 

               
                send_notification_to_latinia(latinia_url, notificacion_payload, session)
                logger.info(f"Éxito al enviar mensaje ID: {message_id}.")
                
            else:
                logger.error(f"Mensaje {message_id} con formato inválido (sin 'payload'). Será descartado.")

        except Exception as e:
           
            logger.error(f"Fallo al procesar mensaje ID: {message_id}. Error: {e}")
            batch_item_failures.append({"itemIdentifier": message_id})

    return {'batchItemFailures': batch_item_failures}