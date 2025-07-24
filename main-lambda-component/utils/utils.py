from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import pytz
import requests
from logging import Logger
import pymysql
from typing import Dict, Any, List, Tuple
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








def get_proccess_date():

    """
    Obtener la fecha y hora actual
    """
    ecuador_timezone = pytz.timezone("America/Guayaquil")
    return datetime.datetime.now(ecuador_timezone)
#.strftime('%Y-%m-%d %H:%M:%S')




def generate_simple_sequential_id(canal: str = "BMO",fecha_proceso:datetime=get_proccess_date()) -> str:
    """
    Genera un ID único usando timestamp + UUID truncado
    """
    canal = canal.ljust(3, '0')[:3]
    now = fecha_proceso
    timestamp = now.strftime("%Y%m%d%H%M%S")
    
    # Usar últimos 6 dígitos del UUID (hex a decimal)
    uuid_suffix = str(abs(hash(str(uuid.uuid4()))))[-6:].zfill(6)
    
    return f"{canal}{timestamp}{uuid_suffix}"


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





def get_params_noti(user: str, password: str, host: str, port: int, db: str) -> List[Dict[str, Any]]:
    """
    Obtiene los parametros de notificaciones desde DB_TC_ODS
    Llama al procedimiento almacenado pa_tcr_obtener_param_noti()
    
    Args:
        user (str): Usuario de la base de datos
        password (str): Contraseña de la base de datos
        host (str): Host de la base de datos
        port (int): Puerto de la base de datos
        db (str): Nombre de la base de datos
        
    Returns:
        List[Dict[str, Any]]: Lista de parámetros con nombre, valor y descripción
    """
    connection = pymysql.connect(
        host=host, 
        user=user, 
        password=password, 
        port=port, 
        db=db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor  # Para obtener resultados como diccionario
    )
    
    try:
        with connection.cursor() as cursor:
            cursor.callproc('pa_tcr_obtener_param_noti')
            results = cursor.fetchall()
            
            if not isinstance(results[0] if results else {}, dict):
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in results]
            
            return results
            
    except pymysql.Error as e:
        print(f"Error al ejecutar el procedimiento almacenado: {e}")
        raise
    except Exception as e:
        print(f"Error inesperado: {e}")
        raise
    finally:
        connection.close()

def get_params_noti_as_dict(user: str, password: str, host: str, port: int, db: str) -> Dict[str, str]:
    """
    Obtiene los parametros de notificaciones como diccionario clave-valor
    
    Args:
        user (str): Usuario de la base de datos
        password (str): Contraseña de la base de datos
        host (str): Host de la base de datos
        port (int): Puerto de la base de datos
        db (str): Nombre de la base de datos
        
    Returns:
        Dict[str, str]: Diccionario con pa_nombre como clave y pa_valor como valor
    """
    params_list = get_params_noti(user, password, host, port, db)
    params_dict = {
        param['pa_nombre']: param['pa_valor'] 
        for param in params_list
    }
    
    return params_dict

def get_specific_param(user: str, password: str, host: str, port: int, db: str, param_name: str) -> str:
    """
    Obtiene un parámetro específico por nombre
    
    Args:
        user (str): Usuario de la base de datos
        password (str): Contraseña de la base de datos
        host (str): Host de la base de datos
        port (int): Puerto de la base de datos
        db (str): Nombre de la base de datos
        param_name (str): Nombre del parámetro a buscar
        
    Returns:
        str: Valor del parámetro solicitado
        
    Raises:
        ValueError: Si el parámetro no existe
    """
    params_dict = get_params_noti_as_dict(user, password, host, port, db)
    
    if param_name not in params_dict:
        raise ValueError(f"Parámetro '{param_name}' no encontrado")
    
    return params_dict[param_name]




def build_latinia_payload(request_data: dict, db_params: dict, logger=None) -> dict:
    """
    Construye el payload para Latinia basándose en el request de entrada y parámetros de DB
    Pasa el objeto data tal como viene en el request
    
    Args:
        request_data (dict): Request de entrada validado
        db_params (dict): Parámetros obtenidos de la base de datos
        logger: Logger opcional
    
    Returns:
        dict: Payload formateado para Latinia
    """
    try:
        # Generar ID único basado en el canal
        canal = request_data.get("channels", "BMO")
        unique_id = generate_simple_sequential_id(canal)
        
        ref_service = request_data.get("refService", "DEFAULT")
        cod_ente = str(request_data.get("cod_ente", "000000"))
        data_section = request_data.get("data", {})  # Pasar tal como viene
        addresses = request_data.get("addresses", [])
        contents = request_data.get("contents", [])
        
        empresa = db_params.get("NotiEmpresa", "BOLIVARIANO")
        ref_msg_label = db_params.get("NotiRefMessageLabel", "Avisos24")
        
        latinia_payload = {
            "header": {
                "id": unique_id,
                "refCompany": empresa,
                "refService": ref_service,
                "keyValue": cod_ente,
                "channels": canal,
                "refMsgLabel": ref_msg_label
            },
            "info": {
                "loginEnterprise": empresa,
                "refContract": ref_service  
            },
            "data": data_section,  
            "addresses": addresses,
            "contents": contents
        }
        
        if logger:
            logger.info(f"Payload construido para Latinia con ID: {unique_id}")
            logger.debug(f"Payload completo: {json.dumps(latinia_payload, indent=2, ensure_ascii=False)}")
        
        return latinia_payload
        
    except Exception as e:
        if logger:
            logger.error(f"Error al construir payload para Latinia: {e}", exc_info=True)
        raise