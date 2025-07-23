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
    return logger

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
    queue_url = config_file["sqs"]["queue_url"]
    parametro_mantenimiento = config_file["latinia"]["mantenimiento"]
    latinia_url = config_file["latinia"]["url"]
    reintentos = int(config_file["lambda"]["backoff"]["max_retries"])
    backoff_factor = float(config_file["lambda"]["backoff"]["backoff_factor"])

    try:
        get_queue_attributes(queue_url, logger)

        total_messages = read_all_messages_from_queue(queue_url, logger)

        return {
            "statusCode": 200,
            "headers": {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'codigoError': 0,
                'message': f'Se procesaron {total_messages} mensajes de la cola',
                'totalMessages': total_messages,
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

def read_all_messages_from_queue(queue_url, logger):
    """
    Lee todos los mensajes disponibles en la cola SQS y los imprime como logs
    Args:
        queue_url (string): URL de la cola SQS
        logger: Logger configurado
    Returns:
        int: Número total de mensajes procesados
    """
    total_messages = 0
    
    try:
        logger.info(f"Iniciando lectura de mensajes de la cola: {queue_url}")
        
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
            
            for i, message in enumerate(messages, 1):
                total_messages += 1
                message_id = message.get('MessageId', 'N/A')
                receipt_handle = message.get('ReceiptHandle', 'N/A')
                
                logger.info(f"--- Mensaje #{total_messages} ---")
                logger.info(f"MessageId: {message_id}")
                logger.info(f"ReceiptHandle: {receipt_handle[:50]}...")  # Solo primeros 50 caracteres
                
                try:
                    message_body = json.loads(message.get('Body', '{}'))
                    logger.info(f"Cuerpo del mensaje: {json.dumps(message_body, indent=2, ensure_ascii=False)}")
                except json.JSONDecodeError:
                    logger.warning(f"Cuerpo del mensaje no es JSON válido: {message.get('Body', '')}")
                
                message_attributes = message.get('MessageAttributes', {})
                if message_attributes:
                    logger.info("Atributos del mensaje:")
                    for attr_name, attr_data in message_attributes.items():
                        attr_value = attr_data.get('StringValue', attr_data.get('BinaryValue', 'N/A'))
                        logger.info(f"  {attr_name}: {attr_value}")
                
                # Log de atributos del sistema
                attributes = message.get('Attributes', {})
                if attributes:
                    logger.info("Atributos del sistema:")
                    for attr_name, attr_value in attributes.items():
                        logger.info(f"  {attr_name}: {attr_value}")
                
                logger.info(f"--- Fin Mensaje #{total_messages} ---\n")
                
                # IMPORTANTE: No eliminar mensajes aquí para solo lectura
                # Si quieres eliminar después de leer, descomenta las siguientes líneas:
                # sqs.delete_message(
                #     QueueUrl=queue_url,
                #     ReceiptHandle=receipt_handle
                # )
                # logger.info(f"Mensaje {message_id} eliminado de la cola")
            
            logger.info(f"Procesados {len(messages)} mensajes en este lote")
            
        logger.info(f"Lectura completada. Total de mensajes procesados: {total_messages}")
        return total_messages
        
    except Exception as e:
        logger.error(f"Error al leer mensajes de la cola: {e}", exc_info=True)
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





