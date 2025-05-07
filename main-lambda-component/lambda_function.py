import boto3
import json
from request_validation import validate_request
from utils.utils import get_proccess_date

import datetime
import uuid


def lambda_handler(event,context):
    fecha_proceso = get_proccess_date()
    print("Fecha de proceso:", fecha_proceso)

    #validacion de datos 
    error,body = validate_request(event)
    if error:
        return {
            "statusCode":400,
            "headers":{
                "Content-Type":"application/json",
                
            },
            'body':json.dumps({
                'error':'Error en la validacion de datos',
                'message':error
            })
        }
    
    


if __name__ == "__main__":
   lambda_handler(None, None)