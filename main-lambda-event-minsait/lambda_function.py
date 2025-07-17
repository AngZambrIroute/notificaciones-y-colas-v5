import boto3
import json
import yaml
from dotenv import load_dotenv
import os
import requests
import uuid
import logging
from utils import validate_request
import datetime
import pytz
sqs = boto3.client('sqs')
ssm = boto3.client('ssm') 
s3_client = boto3.client('s3')




BUCKET_NAME = "bb-emisor-eventos-noti"
BUCKET_PREFIX = "eventos.json"
load_dotenv()

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



def get_process_date():
    """
    Obtener la fecha y hora actual
    """
    ecuador_timezone = pytz.timezone("America/Guayaquil")
    return datetime.datetime.now(ecuador_timezone).strftime('%Y-%m-%d %H:%M:%S')

def lambda_handler(event,context):
    fecha_proceso = get_process_date()
    print("Fecha de proceso:", fecha_proceso)
    if 'body' in event:
        if isinstance(event['body'], str):
            body = json.loads(event["body"])
        else:
            body = event["body"]

        result = validate_request(body)

        print("Resultado de validación:", result)
        if not result["valid"]:
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json"
                },
                'body':json.dumps({
                    'error':'Error en la validacion de datos',
                    'message':result["errors"]
                })
            }
        
        
        enviroment = os.getenv("ENV")
        enviroment = "dev" if enviroment is None else enviroment
        config_file = load_yaml_file(f"config-{enviroment}.yml")


        if config_file is None:
            return {
                "statusCode":500,
                "headers":{
                "   Content-Type":"application/json",
                
                },
            '   body':json.dumps({
                    'codigoError':7011,
                    'message':'Error al cargar el archivo de configuracion',
                    'messageId':'',
                    'timestamp':fecha_proceso,
                })
            }
        try:
            validate_config(config_file)
            print("Archivo de configuracion valido")
            logger = config_logger(config_file)
            logger.info("Parametros obtenidos correctamente")
            data = result["data"]
            unique_id = str(uuid.uuid4())
            timestamp = datetime.datetime.utcnow().isoformat()
            s3_key:str = f"{BUCKET_PREFIX}/message_{timestamp}_{unique_id}.json"
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            s3_path = f"s3://{BUCKET_NAME}/{s3_key}"
            logger.info(f"Notificación enviada a S3: {s3_path}")  
            return {
                "statusCode":200,
                "headers":{
                "Content-Type":"application/json",
                
                },
                'body':json.dumps({
                    'codigoError':0,
                    'message':'Notificacion enviada correctamente',
                    'messageId':unique_id,
                    'timestamp':fecha_proceso,
                })
            }      
        except requests.exceptions.RequestException as e:
            return {
                "statusCode":500,
                "headers":{
                    "Content-Type":"application/json",          
                },
                'body':json.dumps({
                'codigoError':69,
                'message':'Hubo un error al enviar la notificacion, vuelva a intentarlo mas tarde',
                'messageId':'',
                'timestamp':fecha_proceso,
            })
        }    
    

def send_notification(data,latinia_url,s3_url):
    """
    sube la notificacion a s3 y ejecuta glue
    """
    
    
    
    
