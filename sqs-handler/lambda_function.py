import json
import os
import boto3
import logging
import yaml
import datetime
import pytz


sqs = boto3.client('sqs')
s3 = boto3.client('s3') 

BUCKET_NAME = os.getenv("CONFIG_BUCKET_NAME") or "bb-emisormdp-config"

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

def get_proccess_date():

    """
    Obtener la fecha y hora actual
    """
    ecuador_timezone = pytz.timezone("America/Guayaquil")
    return datetime.datetime.now(ecuador_timezone).strftime('%Y-%m-%d %H:%M:%S')

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



