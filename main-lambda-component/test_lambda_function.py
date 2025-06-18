import unittest
from unittest.mock import patch, MagicMock
import json
import yaml
import requests
import lambda_function 

class TestLambdaHandler(unittest.TestCase):

    def setUp(self):
        """Este método se ejecuta ANTES de cada prueba para preparar datos comunes."""
        print(f"\n--- Configurando Mocks para la prueba: {self.id()} ---")
        
        self.mock_body_sat = {
            "Número de tarjeta (ofuscado)": "************1234", "Importe en moneda del titular": 100.0,
            "Fecha y hora de la operación": "2025-06-13T10:30:00", "Cuenta": "987654321",
            "Código de comercio": "5411", "Código de notificación": "DIFCO",
            "Medio de comunicación": "APP", "país del comercio": "EC", "Número de cuotas": 12,
            "Código de la entidad": "BOLIVARIANO", "Identificador del cliente": "0987654321",
            "Código del evento": "DIFCO", "Descripción del evento": "Compra de prueba en unittest"
        }
        self.mock_db_row = MagicMock()
        self.mock_db_row.email, self.mock_db_row.telefono, self.mock_db_row.nombre_cliente, self.mock_db_row.descripcion_transaccion = "test@cliente.com", "555-876-5432", "Cliente de Prueba", "Compra de prueba desde SP"

    def test_flujo_exitoso_enviado_a_latinia(self):
        print("\n--- Ejecutando prueba de flujo exitoso ---")
        with patch('lambda_function.validate_request') as mock_validate, \
             patch('lambda_function.crear_conexion_db') as mock_crear_db, \
             patch('lambda_function.boto3.client') as mock_boto_client, \
             patch('lambda_function.send_notification_to_latinia') as mock_send_latinia:

            mock_validate.return_value = (None, self.mock_body_sat)
            mock_crear_db.return_value.cursor.return_value.fetchone.return_value = self.mock_db_row

            mock_ssm_instance = MagicMock()
            with open('config-dev.yml', 'r') as f: config_file = yaml.safe_load(f)

            def ssm_get_parameter_side_effect(Name):
                if Name == config_file["latinia"]["mantenimiento"]: return {'Parameter': {'Value': 'False'}}
                if Name == config_file["latinia"]["url"]: return {'Parameter': {'Value': 'http://fake-latinia-api.com'}}
                if Name == config_file["sqs"]["queue_url"]: return {'Parameter': {'Value': 'http://my-fake-queue.com'}}
                return None
            mock_ssm_instance.get_parameter.side_effect = ssm_get_parameter_side_effect
            
            def boto_client_side_effect(service_name, *args, **kwargs):
                if service_name == 'ssm': return mock_ssm_instance
                return MagicMock() 
            mock_boto_client.side_effect = boto_client_side_effect
            
            resultado = lambda_function.lambda_handler({"body": "{}"}, None)
            self.assertEqual(resultado.get('statusCode'), 200)
            mock_send_latinia.assert_called_once()
            print("¡Prueba de flujo exitoso completada!")

    def test_falla_latinia_encola_e_invoca_reintento(self):
        print("\n--- Ejecutando prueba de fallo y encolamiento ---")
        with patch('lambda_function.validate_request') as mock_validate, \
             patch('lambda_function.crear_conexion_db') as mock_crear_db, \
             patch('lambda_function.boto3.client') as mock_boto_client, \
             patch('lambda_function.send_notification_to_latinia') as mock_send_latinia, \
             patch('lambda_function.send_notification_to_queue') as mock_send_queue, \
             patch('lambda_function.invoke_retry_handler') as mock_invoke_retry: 

            mock_validate.return_value = (None, self.mock_body_sat)
            mock_crear_db.return_value.cursor.return_value.fetchone.return_value = self.mock_db_row
            mock_send_latinia.side_effect = requests.exceptions.RequestException("Simulated connection error")

            mock_ssm_instance, mock_lambda_instance = MagicMock(), MagicMock()
            with open('config-dev.yml', 'r') as f: config_file = yaml.safe_load(f)
            
            def ssm_get_parameter_side_effect(Name):
                if Name == config_file["sqs"]["queue_url"]: return {'Parameter': {'Value': 'http://my-fake-queue.com'}}
                if Name == config_file["latinia"]["mantenimiento"]: return {'Parameter': {'Value': 'False'}}
                if Name == config_file["latinia"]["url"]: return {'Parameter': {'Value': 'http://fake-latinia-api.com'}}
                return None
            mock_ssm_instance.get_parameter.side_effect = ssm_get_parameter_side_effect
            
            def boto_client_side_effect(service_name, *args, **kwargs):
                if service_name == 'ssm': return mock_ssm_instance
                if service_name == 'lambda': return mock_lambda_instance
                return MagicMock()
            mock_boto_client.side_effect = boto_client_side_effect

            resultado = lambda_function.lambda_handler({"body": "{}"}, None)
            self.assertEqual(resultado.get('statusCode'), 502)
            mock_send_latinia.assert_called_once()
            mock_send_queue.assert_called_once()
            mock_invoke_retry.assert_called_once()
            print("¡Prueba de fallo y encolamiento completada!")

if __name__ == '__main__':
    unittest.main(verbosity=2)