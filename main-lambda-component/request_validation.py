from jsonschema import validate
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
            "type": ["string", "null"]
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
    except json.JSONDecodeError as e:
        return f"Request con formato inválido: {e}",None
    except Exception as e:
        return f"Error al validar el request: {e}",None
    
        