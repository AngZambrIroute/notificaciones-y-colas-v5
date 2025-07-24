from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import pytz
import requests
from logging import Logger
import json
import uuid
import boto3
import datetime

def get_secret(secret_name: str, region_name: str = "us-east-1") -> dict:
    """
    Obtiene un secreto de AWS Secrets Manager.
    
    Args:
        secret_name (str): Nombre del secreto a obtener.
        region_name (str): Región de AWS donde se encuentra el secreto.
        
    Returns:
        dict: Contenido del secreto como un diccionario.
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except Exception as e:
        print(f"Error al obtener el secreto: {e}")
        raise


def create_session(reintentos:int = 3,backoff_factor:float = 0.5,):
    """
    crear una sesion de requests con reintentos
    y manejo de errores para las peticiones HTTP.
    Args:
        reintentos (int): cantidad de reintentos
        backoff_factor (float): factor de retroceso para los reintentos
    """
    session = requests.Session()
    retry_reintentos = Retry(
        total=reintentos,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
        allowed_methods=["POST"],
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry_reintentos)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        'User-Agent': 'Lambda-Notification-Service/1.0',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    })
    session.timeout = (10,10)
    return session

def generate_simple_sequential_id(canal: str = "BMO") -> str:
    """
    Genera un ID único usando timestamp + UUID truncado
    """
    canal = canal.ljust(3, '0')[:3]
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y%m%d%H%M%S")
    
    # Usar últimos 6 dígitos del UUID (hex a decimal)
    uuid_suffix = str(abs(hash(str(uuid.uuid4()))))[-6:].zfill(6)
    
    return f"{canal}{timestamp}{uuid_suffix}"



def get_proccess_date():

    """
    Obtener la fecha y hora actual
    """
    ecuador_timezone = pytz.timezone("America/Guayaquil")
    return datetime.datetime.now(ecuador_timezone).strftime('%Y-%m-%d %H:%M:%S')

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
