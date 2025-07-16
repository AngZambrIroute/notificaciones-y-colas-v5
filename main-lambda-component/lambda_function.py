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
import uuid
sqs = boto3.client('sqs')
ssm = boto3.client('ssm') 
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


def lambda_handler(event,context):
    fecha_proceso = get_proccess_date()
    print("Fecha de proceso:", fecha_proceso)

    #validacion de datos 
    error,body = validate_request(event)
    if error:
        return {
            "statusCode":400,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
                'error':'Error en la validacion de datos',
                'message':error
            })
        }
    enviroment = os.getenv("ENV")
    enviroment = "dev" if enviroment is None else enviroment
    config_file = load_yaml_file(f"config-{enviroment}.yml")


    if config_file is None:
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
                'codigoError':7011,
                'message':'Error al cargar el archivo de configuracion',
                'messageId':'',
                'timestamp':get_proccess_date(),
            })
        }
    try:
        validate_config(config_file)
        print("Archivo de configuracion valido")
        logger = config_logger(config_file)
        queue_url = ssm.get_parameter(
            Name=config_file["sqs"]["queue_url"]
        )['Parameter']['Value']
        parametro_mantenimiento = ssm.get_parameter(
            Name=config_file["latinia"]["mantenimiento"]
        )['Parameter']['Value']
        latinia_url = ssm.get_parameter(
            Name=config_file["latinia"]["url"]
        )['Parameter']['Value']

        if parametro_mantenimiento == "True":
            logger.info("Latinia fuera de servicio.Todo trafico se envia hacia la cola")
            mesaage_id = send_notification_to_queue(queue_url, body)

            return {
                "statusCode":200,
                "headers":{
                    "Content-Type":"application/json",
                    
                },
                'body':json.dumps({
                    'codigoError':10,
                    'message':'Latinia fuera de servicio. Todo trafico se envia hacia la cola',
                    'messageId':mesaage_id,
                    'timestamp':get_proccess_date(),
                })
            }

        else:
            logger.info("Latinia se encuentra disponible. Envio de notificacion a Latinia")
            timeout_seconds = int(config_file["latinia"]["timeout_seconds"])
            session = create_session()
            send_notification_to_latinia(latinia_url,body,session,timeout_seconds)
            return {
                "statusCode":200,
                "headers":{
                    "Content-Type":"application/json",
                    
                },
                'body':json.dumps({
                    'codigoError':0,
                    'message':'Notificacion enviada a Latinia',
                    'messageId':'',
                    'timestamp':get_proccess_date(),
                })
            }
    except ValueError as e:
        logger.error("Error en la validacion del archivo de configuracion",exc_info=True,stack_info=True)
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",    
            },
            'body':json.dumps({
                'codigoError':60010,
                'message':'Error al cargar el archivo de configuracion',
                'messageId':'',
                'timestamp':get_proccess_date(),
            })
        }
    except botocore.exceptions.ClientError as e:
        logger.error("Error al comunicarse con AWS",exc_info=True,stack_info=True)
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
                'codigoError':9082,
                'message':'Error al comunicarse con AWS',
                'messageId':'',
                'timestamp':get_proccess_date(),
            })
        }
    except requests.exceptions.RequestException as e:
        logger.error("Hubo un error al comunicarse con latinia. el mensaje será reencolado",exc_info=True,stack_info=True)
        send_notification_to_queue(queue_url, body)
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
               'codigoError':69,
                'message':'Error al comunicarse con latinia. el mensaje será reencolado',
                'messageId':'',
                'timestamp':get_proccess_date(),
            })
        }    
    
def send_notification_to_queue(queue_url,body):
    """
    Envio de notificacion a la cola
    Args:
        queue_url (string):url de la cola
        body (dict): cuerpo del request previamente validado
    """
    
    try:
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    'payload':body,
                    'timestamp':get_proccess_date(),
                    'messageId':str(uuid.uuid4()),
                    'intentos':0
                }),
            MessageAttributes={
                'MessageId': {
                    'DataType': 'String',
                    'StringValue': str(uuid.uuid4())
                },
                'FechaProceso': {
                    'DataType': 'String',
                    'StringValue': get_proccess_date()
                }
            }
        )
        print("Respuesta de la cola:", response)
        return response["MessageId"]
    except botocore.exceptions.ClientError as e:
        raise
        
    
def send_notification_to_latinia(latinia_url,body,session,timeout_seconds):
    """
    Envio de notificacion a latinia
    Args:
        latinia_url (string):url de latinia
        body (dict): cuerpo del request previamente validado
        session (Session): sesion de requests con configuracion de reintentos
    """
    
    req_session = session
    try:
        response = req_session.post(
            url=latinia_url,
            json=body,
            timeout=timeout_seconds
        )
        response.raise_for_status()
        print("Respuesta de Latinia:", response.json())
    except requests.exceptions.RequestException as e:
        raise