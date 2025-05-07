import os
import sys
import json
import pytest
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from request_validation import validate_request



def load_json_file(filename):
    """
    Helper para cargar un archivo JSON de prueba.
    """
    filepath = os.path.join(os.path.dirname(__file__),filename)
    with open(filepath,"r") as file:
        return json.load(file)
    
def test_valid_request():
    """
    Prueba de validacion pasa correctamente con un JSON valido
    """

    valid_request = load_json_file("valid_request.json")
    error,body = validate_request(valid_request)

    assert error is None
    assert body == valid_request

def test_invalid_request():
    """
    Prueba de validacion falla correctamente con un JSON inválido
    
    """
    invalid_request = load_json_file("invalid_request.json")

    error,body = validate_request(invalid_request)

    assert error is not None
    assert "error al validar el request" in error
    assert body is None

def test_request_with_missing_fields():
    """
    Prueba con un request al que le faltan campos requeridos.
    """
    # Crear un request con campos faltantes
    incomplete_request = {
        "header": {
            "id": "12345",
            "refCompany": "ABC",
            "refService": "BQTCT",  # Requiere más campos
            "keyValue": "key123",
            "channels": "email",
            "refMsgLabel": "Bloqueo"
        },
        "info": {
            "loginEnterprise": "enterprise123",
            "refContract": "contract456"
        },
        "data": {
            "tipotrj": "credito",
            "numtrj": "1234567890123456"
            # Faltan campos requeridos: identi, fecha, motivo, nombre_titular, canal
        },
        "addresses": [
            {
                "className": "email",
                "type": "TO",
                "ref": "usuario@ejemplo.com"
            }
        ]
    }
    
    # Validar el request
    error, body = validate_request(incomplete_request)
    
    # Verificar que hay un error
    assert error is not None
    assert body is None

def test_invalid_json_string():
    """
    Prueba con un string JSON mal formado.
    """
    # JSON mal formado (le falta una comilla)
    malformed_json = '{"header": {"id": "123", "refCompany: "ABC"}}'
    
    # Validar el request
    error, body = validate_request({"body": malformed_json})
    
    # Verificar que hay un error de formato JSON
    assert error is not None
    assert "Request con formato inválido" in error
    assert body is None