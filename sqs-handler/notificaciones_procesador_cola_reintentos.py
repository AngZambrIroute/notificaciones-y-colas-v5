import json
import os
import boto3
import requests
import logging


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
    response.raise_for_status() # Lanza un error para códigos 4xx/5xx
    logger.info(f"Respuesta de Latinia en reintento: {response.text}")

def lambda_handler(event, context):
    """
    Handler para la Lambda de reintentos, desencadenada por SQS.
    Procesa un lote de mensajes recibidos en el 'event'.
    """
    logger.info(f"Iniciando procesamiento de {len(event.get('Records', []))} mensajes de la cola.")
    
    # Obtenemos la URL de Latinia 
    try:
        latinia_url = ssm.get_parameter(Name=LATINIA_URL_PARAM_NAME)['Parameter']['Value']
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
                    # Enviamos el payload original a la DLQ
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