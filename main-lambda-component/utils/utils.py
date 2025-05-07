from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import pytz
import requests

import datetime

def create_session():
    """
    crear una sesion de requests con reintentos
    y manejo de errores para las peticiones HTTP.
    """
    session = requests.Session()
    retry_reintentos = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
        respect_retry_after_header=True
    )
    adapter = HTTPAdapter(max_retries=retry_reintentos)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_proccess_date():
    """
    Obtener la fecha y hora actual
    """
    ecuador_timezone = pytz.timezone("America/Guayaquil")
    return datetime.datetime.now(ecuador_timezone).strftime('%Y-%m-%d %H:%M:%S')