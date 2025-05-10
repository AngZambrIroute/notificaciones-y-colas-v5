
import sys
import os
import pytest
import yaml

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_yaml_file(config_path):
    """
    Carga del archivo yaml de configuracion
    """

    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            print(f"Configuracion cargada desde {config_path}")
            return config
    except Exception as e:
        print(f"Error al cargar el archivo de configuracion: {e}")
        return None

def test_load_yaml_file():
    route:str = "../config-dev.yml"
    config = load_yaml_file(route)
    print(config)
    assert config["latinia"]  is not None

