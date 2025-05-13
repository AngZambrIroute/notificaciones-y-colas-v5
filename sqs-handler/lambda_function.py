import boto3
import json
import os
import botocore
import logging

import datetime
import uuid
import time
sqs = boto3.client('sqs')
ssm = boto3.client('ssm') 


def  config_logger():
    """
    configuracion del logger de la aplicacion lambda
    Args:
        config_file (dict): archivo de configuracion
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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


def get_all_messages(queue_url,api_url,max_messages=10,wait_time=20,visibility_timeout=20):
    """
     Sondea mensajes de SQS y los procesa uno por uno
    Args:
        queue_url (str): URL de la cola SQS
        api_url (str): URL de la API
        max_messages (int): Número máximo de mensajes a recibir (1-10)
        wait_time (int): Tiempo máximo de espera en segundos
        visibility_timeout (int): Tiempo de visibilidad en segundos
    """
    logger =config_logger()
    logger.info("iniciando sondeo de mensajes de la cola.")

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=visibility_timeout
            )
            messages = response.get("Messages",[])
            if  not messages:
                logger.info("No hay mensajes en la cola.")
                time.sleep(5)
                continue
            logger.info(f"Recibidos {len(messages)} mensajes de la cola.")
        except KeyboardInterrupt:
            logger.error("Saliendo del bucle de sondeo de mensajes.")
            break
        except Exception  as e:
            logger.error(f"Error al recibir mensajes de la cola: {e}")
            time.sleep(5)
def lambda_handler(event,context):
   """
    Lambda function to handle SQS messages.
   """
   logger = config_logger()
   logger.info("iniciando lambda_handler")
   queue_url = ssm.get_parameter(
        Name="/dev-notificaciones-colas/queue/url")['Parameter']['Value']
   get_all_messages(queue_url,api_url=None,max_messages=10,wait_time=20,visibility_timeout=20)
#    for registro in event["Records"]:
#        logger.info(f"Registro recibido: {registro}")
#        return {
#                     'batchItemFailures': [
#                         {'itemIdentifier': registro['messageId']} for registro in event['Records']
#                     ]
#                 }