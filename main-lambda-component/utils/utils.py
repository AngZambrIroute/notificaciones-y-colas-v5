from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import pytz
import requests
from logging import Logger

import datetime

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
        total=3,
        backoff_factor=0.5,
        allowed_methods=[500, 502, 503, 504],
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
