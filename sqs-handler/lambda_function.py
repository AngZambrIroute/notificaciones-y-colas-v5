import boto3
import json
import os
import botocore
import logging

import datetime
import uuid
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


def lambda_handler(event,context):
   """
    Lambda function to handle SQS messages.
   """
   logger = config_logger()
   for registro in event["Records"]:
       logger.info|(f"Registro recibido: {registro}")
       return {
                    'batchItemFailures': [
                        {'itemIdentifier': registro['messageId']} for registro in event['Records']
                    ]
                }