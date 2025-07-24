from jsonschema import validate,ValidationError
import json
import os

def load_nemonic_config():
    """
    carga el archivo de configuracion de request por nemonico
    """
    config_path="./config/nemonic_config.json"
    try:
        with open(config_path,"r") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: El archivo de configuracion {config_path} no fue encontrado.")
        return {}
    
def format_validation_error(error: ValidationError) -> dict:
    """
    Formatea el error de validación de JSON Schema en un formato más legible
    
    Args:
        error (ValidationError): Error de validación de jsonschema
        
    Returns:
        dict: Error formateado
    """
    error_details = {
        "field": ".".join(str(p) for p in error.absolute_path) if error.absolute_path else "root",
        "message": error.message,
        "failed_value": error.instance,
        "validator": error.validator,
        "validator_value": error.validator_value
    }
    
    # Casos específicos para diferentes tipos de errores
    if error.validator == "required":
        missing_fields = error.validator_value
        if isinstance(missing_fields, list):
            error_details["missing_fields"] = missing_fields
            error_details["message"] = f"Faltan los siguientes campos requeridos: {', '.join(missing_fields)}"
        else:
            error_details["missing_fields"] = [missing_fields]
            error_details["message"] = f"Falta el campo requerido: {missing_fields}"
    
    elif error.validator == "enum":
        valid_values = error.validator_value
        error_details["valid_values"] = valid_values
        error_details["message"] = f"Valor inválido. Valores permitidos: {', '.join(map(str, valid_values))}"
    
    elif error.validator == "type":
        expected_types = error.validator_value
        if isinstance(expected_types, list):
            error_details["expected_types"] = expected_types
            error_details["message"] = f"Tipo de dato inválido. Se esperaba: {' o '.join(expected_types)}"
        else:
            error_details["expected_types"] = [expected_types]
            error_details["message"] = f"Tipo de dato inválido. Se esperaba: {expected_types}"
    
    elif error.validator == "pattern":
        pattern = error.validator_value
        error_details["expected_pattern"] = pattern
        error_details["message"] = f"El formato no es válido. Debe cumplir el patrón: {pattern}"
    
    return error_details

def get_allowed_nemonics_from_config():
    """
    Extrae la lista de nemonicos permitidos del archivo de configuracion
    """
    config = load_nemonic_config()
    return list(config.keys())

def generate_conditional_validations(nemonic_config):
    """
    Genera las validaciones condicionales basadas en la configuracion 
    """
    validations = []
    for nemonic,config in nemonic_config.items():
        if config.get("required_fields"):
            validations.append({
                "if":{
                    "properties":{
                        "refService":{
                                    "enum":[nemonic]
                        }
                    }
                },
                "then":{
                    "properties":{
                        "data":{
                            "required":config["required_fields"]
                        }
                    }
                }
            })
    return validations

NEMONIC_CONFIG = load_nemonic_config()
ALLOWED_NEMONICS = get_allowed_nemonics_from_config()

request_schema = {
    "type":"object",
    "required":["data","addresses"],
    "properties":{
        "refservice":{
            "type":"string",
            "enum":ALLOWED_NEMONICS
        },
        "channels":{
            "type": ["string"]
        },
        "cod_ente":{
            "type": ["integer", "null"],
            "minimum": 0
        },
        # "header":{
        #     "type":"object",
        #     "properties":{
        #         "id":{
        #             "type":"string"
        #         },
        #         "refCompany":{
        #             "type":"string"
        #         },
        #         "refService":{
        #             "type":"string",
        #             "enum":ALLOWED_NEMONICS
        #         },
        #         "keyValue":{
        #             "type":"string"
        #         },
        #         "channels":{
        #             "type":["string","null"]
        #         },
        #         "refMsgLabel":{"type":"string"}
        #     }
        # },
        # "info":{
        #     "type":"object",
        #     "required": ["loginEnterprise", "refContract"],
        #     "properties": {
        #         "loginEnterprise": {"type": "string"},
        #         "refContract": {"type": "string"}
        #     }
        # },
        "data":{
            "type": "object",
            "properties": {
                "desccanal": {"type": ["string", "null"]},
                "canal": {"type": ["string", "null"]},
                "tipotrj": {"type": ["string", "null"]},
                "valor": {"type": ["string", "null"]},
                "numtrj": {"type": ["string", "null"]},
                "identi": {"type": ["string", "null"]},
                "des_transaccion": {"type": ["string", "null"]},
                "des_comercio": {"type": ["string", "null"]},
                "plazo": {"type": ["string", "null"]},
                "nombre_titular": {"type": ["string", "null"]},
                "estado_pais": {"type": ["string", "null"]},
                "nom_pais": {"type": ["string", "null"]},
                "fecha": {"type": ["string", "null"]},
                "hora": {"type": ["string", "null"]},
                "nombre_cliente": {"type": ["string", "null"]},
                "motivo": {"type": ["string", "null"]},
                "fecha_hora": {"type": ["string", "null"]},
                "gsm": {"type": ["string", "null"]},
                "email": {"type": ["string", "null"]}
            }
        },
        "addresses":{
            "type": "array",
            "items": {
                "type": "object",
                "required": ["className", "type", "ref"],
                "properties": {
                    "className": {"type": "string"},
                    "type": {"type": "string"},
                    "ref": {"type": "string"}
                }
            }
        },
        "contents": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["value", "type", "encoding", "name"],
                "properties": {
                    "value": {"type": "string"},
                    "type": {"type": "string"},
                    "encoding": {"type": "string"},
                    "name": {"type": "string"}
                }
            }
        }

    },
    "allOf":generate_conditional_validations(NEMONIC_CONFIG)
}

def validate_request(request):
    """
    valida que el evento recibido cumpla con el esquema definido
    """
    try:
        if 'body' in request:
            if isinstance(request['body'],str):
                body = json.loads(request["body"])
            else:
                body = request["body"]
        else:
            body = request
        
        validate(instance=body,schema=request_schema)
        return None,body
    
    except ValidationError as e:
        main_error = format_validation_error(e)
        all_errors = [main_error]
        if hasattr(e,'context') and e.context:
            for sub_error in e.context:
                all_errors.append(format_validation_error(sub_error))
        return {
            "error_type": "VALIDATION_ERROR",
            "message": "Error en la validación de datos",
            "errors": all_errors,
            "total_errors": len(all_errors)
        }, None

    except json.JSONDecodeError as e:
        return {
            "error_type": "INVALID_FORMAT_ERROR",
            "message": "Request con formato inválido",
            "details": str(e)
        }, None
    except Exception as e:
       return {
            "error_type": "UNEXPECTED_ERROR",
            "message": "Error inesperado durante la validación",
            "details": str(e)
        }, None
    
        