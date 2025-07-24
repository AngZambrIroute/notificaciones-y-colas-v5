import pymysql
from typing import Dict, Any, List, Tuple

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