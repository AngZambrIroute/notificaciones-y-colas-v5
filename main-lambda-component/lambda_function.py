import boto3
import json
import yaml
from request_validation import validate_request
from dotenv import load_dotenv
import os
import botocore
from utils.utils import get_proccess_date,create_session,validate_config,build_latinia_payload
from utils.utils import get_secret
from utils.utils import get_params_noti_as_dict
import requests
import logging
import uuid
sqs = boto3.client('sqs')
s3 = boto3.client('s3') 


BUCKET_NAME = os.getenv("CONFIG_BUCKET_NAME") or "bb-emisormdp-config"
load_dotenv()
SECRET_KEY_NAME = os.getenv("SECRET_KEY_NAME") or "mysql_mock"

flujo_operacion = {
    "codigoError":0,
    "mensaje":"",
    "estado":"OK",
}




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

def config_logger(config_file: dict):
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
    # *******************Carga de configuracion y logger************************
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
                'timestamp':fecha_proceso,
            })
        }
    logger = config_logger(config_file)
    fecha_proceso = get_proccess_date().strftime('%Y-%m-%d %H:%M:%S')
    logger.info("Fecha de proceso de notificacion: %s", fecha_proceso)
    logger.info("Evento recibido: %s", json.dumps(event, indent=2, ensure_ascii=False))
    #********************Validacion de request************************
    error,body = validate_request(event)
    if error:
        status_code = 400
        if error.get("error_type") == "UNEXPECTED_ERROR":
            status_code = 500
        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json",
            },
            'body': json.dumps({
                'codigoError': 40001,
                'error': error.get("error_type", "VALIDATION_ERROR"),
                'message': error.get("message", "Error en la validación de datos"),
                'details': error.get("errors", []) if error.get("error_type") == "VALIDATION_ERROR" else error.get("details"),
                'timestamp': fecha_proceso,
            }, ensure_ascii=False, indent=2)
        }
    
    try:
        #*********************Validacion de archivo de configuracion************************
        validate_config(config_file)
        print(f"Archivo de configuracion valido: {config_file}")
        #*********************Obtencion de parametros de notificacion************************
        queue_url = config_file["sqs"]["queue_url"]
        parametro_mantenimiento = config_file["latinia"]["mantenimiento"]
        latinia_url = config_file["latinia"]["url"]
        latinia_url_auth = config_file["latinia"]["auth"]
        latinia_secret_id_oauth = config_file["latinia"]["secret_name_oauth"]
        db_secret_name = config_file["db"]["secret_name_db"]
        reintentos = int(config_file["lambda"]["backoff"]["max_retries"])
        backoff_factor = float(config_file["lambda"]["backoff"]["backoff_factor"])
        
        #lectura de secreto para conexion rds
        secret = get_secret(db_secret_name)
        if secret is None:
            logger.error("No se pudo obtener el secreto de la base de datos")
            return {
                "statusCode":500,
                "headers":{
                    "Content-Type":"application/json",
                    
                },
                'body':json.dumps({
                    'codigoError':60010,
                    'message':'Error al obtener el secreto de la base de datos',
                    'messageId':'',
                    'timestamp':fecha_proceso,
                })
            }
        user = secret["username"]
        password = secret["password"]
        host = secret["host"]
        port = int(secret["port"])
        db = secret["dbname"]

        params_noti = get_params_noti_as_dict(user, password, host, port, db)
        if not params_noti:
            logger.error("No se encontraron parametros de notificacion")
            return {
                "statusCode":500,
                "headers":{
                    "Content-Type":"application/json",
                    
                },
                'body':json.dumps({
                    'codigoError':60010,
                    'message':'No se encontraron parametros de notificacion',
                    'messageId':'',
                    'timestamp':fecha_proceso,
                })
            }
        logger.info(f"Parametros de notificacion obtenidos: {params_noti}")
        body = build_latinia_payload(body,params_noti,logger)
        logger.info(f"Payload de Latinia construido: {json.dumps(body, indent=2, ensure_ascii=False)}")
        if parametro_mantenimiento is True:
            logger.info("Latinia fuera de servicio.Todo trafico se envia hacia la cola")
            message_id = send_notification_to_queue(queue_url, body,fecha_proceso)

            return {
                "statusCode":200,
                "headers":{
                    "Content-Type":"application/json",
                    
                },
                'body':json.dumps({
                    'codigoError':10,
                    'message':'Latinia fuera de servicio. Todo trafico se envia hacia la cola',
                    'messageId':message_id,
                    'timestamp':fecha_proceso,
                })
            }

        else:
            logger.info("Latinia se encuentra disponible. Envio de notificacion a Latinia")
            timeout_seconds = int(config_file["latinia"]["timeout_seconds"])
            session = create_session(reintentos,backoff_factor)
            try:
                oauth_token = get_oauth_token(latinia_url_auth, latinia_secret_id_oauth, logger)
                send_notification_to_latinia(latinia_url,body,session,timeout_seconds,logger,oauth_token)
                return {
                "statusCode":200,
                "headers":{
                    "Content-Type":"application/json",
                    
                },
                'body':json.dumps({
                    'codigoError':0,
                    'message':'Notificacion enviada a Latinia',
                    'messageId':'',
                    'timestamp':fecha_proceso,
                })
            }
            except requests.exceptions.Timeout:
                send_notification_to_queue(queue_url, body,fecha_proceso)
                return {
                    "statusCode":500,
                    "headers":{
                        "Content-Type":"application/json",
                        
                    },
                    'body':json.dumps({
                        'codigoError':60010,
                        'message':'La solicitud a Latinia ha excedido el tiempo de espera. Cambiando parametro de mantenimiento a True',
                        'messageId':'',
                        'timestamp':fecha_proceso,
                    })
                }
            
            except requests.exceptions.Timeout:
                message_id = send_notification_to_queue(queue_url, body,fecha_proceso)
                change_param_to_config_file(config_file, "mantenimiento", True)
                return {
                    "statusCode": 500,
                    "headers": {
                        "Content-Type": "application/json",
                    },
                    'body': json.dumps({
                        'codigoError': 60010,
                        'message': f'La solicitud a Latinia ha excedido el tiempo de espera. Cambiando parametro de mantenimiento a True:',
                        'messageId': message_id,
                        'timestamp': fecha_proceso,
                 })
                }
            except requests.exceptions.RequestException as e:
                logger.error(f"Hubo un error al comunicarse con Latinia. El mensaje será reencolado: {e}",exc_info=True,stack_info=True)
                change_param_to_config_file(config_file, "mantenimiento", True)
                message_id = send_notification_to_queue(queue_url, body,fecha_proceso)
                return {
                    "statusCode":500,
                    "headers":{
                        "Content-Type":"application/json",
                        
                    },
                    'body':json.dumps({
                        'codigoError':69,
                        'message':'Error al comunicarse con Latinia. El mensaje será reencolado',
                        'messageId':message_id,
                        'timestamp':fecha_proceso,
                    })
                }
            
    except ValueError as e:
        logger.error("Error en la validacion del archivo de configuracion",exc_info=True,stack_info=True)
        change_param_to_config_file(config_file, "mantenimiento", True)
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",    
            },
            'body':json.dumps({
                'codigoError':60010,
                'message':'Error al cargar el archivo de configuracion',
                'messageId':'',
                'timestamp':fecha_proceso,
            })
        }
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error al comunicarse con AWS{e}",exc_info=True,stack_info=True)
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
                'codigoError':9082,
                'message':'Error al comunicarse con AWS',
                'messageId':'',
                'timestamp':fecha_proceso,
            })
        }
    except requests.exceptions.RequestException as e:
        logger.error("Hubo un error al comunicarse con latinia. el mensaje será reencolado",exc_info=True,stack_info=True)
        send_notification_to_queue(queue_url, body,fecha_proceso)
        return {
            "statusCode":500,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
               'codigoError':69,
                'message':'Error al comunicarse con latinia. el mensaje será reencolado',
                'messageId':'',
                'timestamp':fecha_proceso,
            })
        }    

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
        secret = get_secret(latinia_secret_id_oauth)
        if not secret:
            raise ValueError("No se pudo obtener el secreto de OAuth")
        session = create_session()
        auth_data = {
            # "grant_type": secret.get("grant_type", "client_credentials"),
            "client_id": secret.get("client_id"),
            "client_secret": secret.get("client_secret")
        }
        if "scope" in secret and secret["scope"]:
            auth_data["scope"] = secret["scope"]
        auth_data = {k: v for k, v in auth_data.items() if v}

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        logger.info(f"Enviando petición OAuth con grant_type: {auth_data.get('grant_type')}")
        response = session.post(latinia_url_auth, data=auth_data, headers=headers,timeout=30)
        logger.info(f"Respuesta de autenticación OAuth: {response.status_code}")

        if response.status_code !=200:
            logger.error(f"Error al obtener el token de OAuth: {response.status_code} - {response.text}")
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("access_token")
    
    except Exception as e:
        logger.error(f"Error al obtener el token de OAuth: {e}", exc_info=True)
        raise

def send_notification_to_queue(queue_url,body,fecha_proceso):
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
                    'timestamp':fecha_proceso,
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
                    'StringValue': fecha_proceso
                }
            }
        )
        print("Respuesta de la cola:", response)
        return response["MessageId"]
    except botocore.exceptions.ClientError as e:
        print(f"Error al enviar el mensaje a la cola: {e}")
        if e.response['Error']['Code'] == 'ThrottlingException':
            print("Se ha alcanzado el límite de solicitudes a la cola.")
        elif e.response['Error']['Code'] == 'QueueDoesNotExist':
            print("La cola especificada no existe.")
        elif e.response['Error']['Code'] == 'InvalidParameterValue':
            print("Uno o más parámetros proporcionados son inválidos.")
        raise
        



def send_notification_to_latinia(latinia_url,body,session,timeout_seconds,logger,oauth_token=None):
    """
    Envio de notificacion a latinia
    Args:
        latinia_url (string):url de latinia
        body (dict): cuerpo del request previamente validado
        session (Session): sesion de requests con configuracion de reintentos
    """
    req_session = session
    try:

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        if oauth_token:
            headers['Authorization'] = f'Bearer {oauth_token}'
            logger.info("Token de OAuth incluido en la solicitud a Latinia")
        else:
            logger.warning("No se proporcionó token de OAuth. La solicitud a Latinia puede fallar.")

        response = req_session.post(
            url=latinia_url,
            json=body,
            headers=headers,
            timeout=timeout_seconds
        )
        logger.info(f"Respuesta de Latinia: {response.status_code} - {response.text}")
        response.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        logger.error("Error de conexión a Latinia", exc_info=True, stack_info=True)
        raise
    except requests.exceptions.Timeout as e:
        logger.error("La solicitud a Latinia ha excedido el tiempo de espera", exc_info=True, stack_info=True)
        raise
    except requests.exceptions.RequestException as e:
        logger.error("Error al comunicarse con Latinia", exc_info=True, stack_info=True)
        raise
    
def change_param_to_config_file(config_file,param_name,new_value):
    """
    Cambia un parametro en el archivo de configuracion
    Args:
        config_file (dict): archivo de configuracion
        param_name (str): nombre del parametro a cambiar
        new_value (any): nuevo valor del parametro
    """
    
    if param_name in config_file["latinia"]:
        config_file["latinia"][param_name] = new_value
    else:
        raise KeyError(f"El parametro {param_name} no existe en el archivo de configuracion")
    
    return config_file

def change_param_to_config_file(config_file, param_name, new_value):
    """
    Cambia un parametro en el archivo de configuracion
    Args:
        config_file (dict): archivo de configuracion
        param_name (str): nombre del parametro a cambiar
        new_value (any): nuevo valor del parametro
    """
    if param_name in config_file["latinia"]:
        config_file["latinia"][param_name] = new_value
    else:
        raise KeyError(f"El parametro {param_name} no existe en el archivo de configuracion")
    
    return config_file