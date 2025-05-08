import boto3
import json
import yaml
from request_validation import validate_request
from dotenv import load_dotenv
import os
from utils.utils import get_proccess_date,create_session
import requests

import datetime
import uuid
sqs = boto3.client('sqs')
ssm = boto3.client('ssm') 
load_dotenv()

timeout_seconds = int(os.getenv("TIMEOUT_SECONDS", 5))

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
    #cargar parametros
    queue_url = ssm.get_parameter(
        Name='/dev-notificaciones-colas/queue/url'
    )['Parameter']['Value']
    parametro_mantenimiento = ssm.get_parameter(
        Name='/dev-notificaciones-colas/latinia/mantenimiento'
    )['Parameter']['Value']
    latinia_url = ssm.get_parameter(
        Name='/dev-notificaciones-colas/latinia/url'
    )['Parameter']['Value']
    
    if parametro_mantenimiento == "True":
        print("Servicio en mantenimiento")
        send_notification_to_queue(queue_url,body)
    else:            
        try:
            session = create_session()
            send_notification_to_latinia(latinia_url,body,session)
        except Exception as e:
            send_notification_to_queue(queue_url,body)
            print("Error al enviar notificacion a latinia:", e)
            print("Enviando notificacion a latinia")
    
    print("URL de la cola:", queue_url)
    print("URL de latinia:", latinia_url)
    print("Parametro de mantenimiento:", parametro_mantenimiento)
    
    
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
    except Exception as e:
        print("Error al enviar mensaje a la cola:", e) 
    
def send_notification_to_latinia(latinia_url,body,session):
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

