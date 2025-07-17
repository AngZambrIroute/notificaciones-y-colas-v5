from jsonschema import validate, ValidationError
from jsonschema.exceptions import SchemaError
from jsonschema import Draft7Validator

# Esquema JSON Schema generado a partir del ejemplo
event_schema = {
    "type": "object",
    "properties": {
        "identificacion": {
            "type": "object",
            "properties": {
                "codigoEntidad": {"type": "string"},
                "centroAlta": {"type": "string"},
                "cuenta": {"type": "string"},
                "numeroBeneficiario": {"type": "string"},
                "identificadorCliente": {"type": "string"}
            },
            "required": ["codigoEntidad", "centroAlta", "cuenta", "numeroBeneficiario", "identificadorCliente"]
        },
        "tarjeta": {
            "type": "object",
            "properties": {
                "numeroTarjetaOfuscado": {"type": "string"},
                "fechaCaducidad": {"type": "string", "format": "date"}
            },
            "required": ["numeroTarjetaOfuscado", "fechaCaducidad"]
        },
        "operacion": {
            "type": "object",
            "properties": {
                "fechaHoraOperacion": {"type": "string", "format": "date-time"},
                "fechaHoraProceso": {"type": "string", "format": "date-time"},
                "codigoComercio": {"type": "string"},
                "nombreComercio": {"type": "string"},
                "poblacionComercio": {"type": "string"},
                "paisComercio": {"type": "string"}
            },
            "required": ["fechaHoraOperacion", "fechaHoraProceso", "codigoComercio", "nombreComercio", "poblacionComercio", "paisComercio"]
        },
        "importesYMonedas": {
            "type": "object",
            "properties": {
                "importeMonedaTitular": {"type": "number"},
                "monedaTitular": {"type": "string"},
                "importeMonedaOriginal": {"type": "number"}
            },
            "required": ["importeMonedaTitular", "monedaTitular", "importeMonedaOriginal"]
        },
        "financiamiento": {
            "type": "object",
            "properties": {
                "numeroFinanciacion": {"type": "string"},
                "porcentajeInteres": {"type": "number"},
                "taeCae": {"type": "number"},
                "importeTotalIntereses": {"type": "number"},
                "importeCuota": {"type": "number"},
                "numeroCuotas": {"type": "integer"},
                "importeComisiones": {"type": "number"},
                "fechaAltaCompraCuotas": {"type": "string", "format": "date"},
                "fechaProximaCuota": {"type": "string", "format": "date"},
                "importeComisionApertura": {"type": "number"}
            },
            "required": ["numeroFinanciacion", "porcentajeInteres", "taeCae", "importeTotalIntereses", "importeCuota", "numeroCuotas", "importeComisiones", "fechaAltaCompraCuotas", "fechaProximaCuota", "importeComisionApertura"]
        },
        "cuentaYFormaDePago": {
            "type": "object",
            "properties": {
                "limiteCreditoCuenta": {"type": "number"},
                "formaPago": {"type": "string"},
                "importeFijo": {"type": "number"},
                "porcentajePago": {"type": "number"},
                "importeMinimoPago": {"type": "number"},
                "fechaCargo": {"type": "string", "format": "date"},
                "importeCargo": {"type": "number"}
            },
            "required": ["limiteCreditoCuenta", "formaPago", "importeFijo", "porcentajePago", "importeMinimoPago", "fechaCargo", "importeCargo"]
        },
        "notificacion": {
            "type": "object",
            "properties": {
                "numeroNotificacion": {"type": "string"},
                "codigoPublicidad": {"type": "string"},
                "codigoNotificacion": {"type": "string"}
            },
            "required": ["numeroNotificacion", "codigoPublicidad", "codigoNotificacion"]
        },
        "canalEnvio": {
            "type": "object",
            "properties": {
                "medioComunicacion": {"type": "string"},
                "descripcionMedio": {"type": "string"}
            },
            "required": ["medioComunicacion", "descripcionMedio"]
        },
        "evento": {
            "type": "object",
            "properties": {
                "codigoEvento": {"type": "string"},
                "descripcionEvento": {"type": "string"},
                "descripcionReducidaEvento": {"type": "string"}
            },
            "required": ["codigoEvento", "descripcionEvento", "descripcionReducidaEvento"]
        }
    },
    "required": [
        "identificacion",
        "tarjeta",
        "operacion",
        "importesYMonedas",
        "financiamiento",
        "cuentaYFormaDePago",
        "notificacion",
        "canalEnvio",
        "evento"
    ]
}

# Función de validación
def validate_request(event):
    """
    Valida la estructura del evento recibido.
    Retorna el JSON validado y una lista de errores (si hay).
    """
    validator = Draft7Validator(event_schema)
    errors = sorted(validator.iter_errors(event), key=lambda e: e.path)

    if not errors:
        return {
            "valid": True,
            "data": event,
            "errors": []
        }

    error_list = []
    for error in errors:
        field_path = ".".join(str(x) for x in error.absolute_path)
        error_list.append({
            "field": field_path,
            "message": error.message
        })

    return {
        "valid": False,
        "data": None,
        "errors": error_list
    }


def main():
    test_event = {
        "identificacion": {
        "codigoEntidad": "1234",
    "centroAlta": "5678",
    "cuenta": "ES7621000418401234567891",
"numeroBeneficiario": "001",
"identificadorCliente": "987654321"
},
"tarjeta": {
"numeroTarjetaOfuscado": "1234********5678",
"fechaCaducidad": "2027-12"
},
"operacion": {
"fechaHoraOperacion": "2025-07-15T13:45:00Z",
"fechaHoraProceso": "2025-07-15T14:00:00Z",
"codigoComercio": "C123456789",
"nombreComercio": "Supermercado Central",
"poblacionComercio": "Madrid",
"paisComercio": "ES"
},
"importesYMonedas": {
"importeMonedaTitular": 100.00,
"monedaTitular": "EUR",
"importeMonedaOriginal": 110.00
},
"financiamiento": {
"numeroFinanciacion": "F1234567",
"porcentajeInteres": 12.5,
"taeCae": 13.2,
"importeTotalIntereses": 15.50,
"importeCuota": 19.25,
"numeroCuotas": 6,
"importeComisiones": 2.00,
"fechaAltaCompraCuotas": "2025-07-01",
"fechaProximaCuota": "2025-08-01",
"importeComisionApertura": 1.00
},
"cuentaYFormaDePago": {
"limiteCreditoCuenta": 3000.00,
"formaPago": "fijo",
"importeFijo": 100.00,
"porcentajePago": 10.0,
"importeMinimoPago": 50.00,
"fechaCargo": "2025-07-16",
"importeCargo": 90.00
},
"notificacion": {
"numeroNotificacion": "N123456",
"codigoPublicidad": "PUB01",
"codigoNotificacion": "NOTIF001"
},
"canalEnvio": {
"medioComunicacion": "email",
"descripcionMedio": "Correo electrónico"
},
"evento": {
"codigoEvento": "E200",
"descripcionEvento": "Compra con tarjeta",
"descripcionReducidaEvento": "Compra POS"
}
}
    try:
        result = validate_request(test_event)
        if result["valid"]:
            print("Evento válido:", result)
        else:
            print("Errores de validación:", result["errors"])
    except ValidationError as e:
        print("Error de validación:", e.message)

main()