# Notificaciones y Colas v5

Sistema de procesamiento de notificaciones bancarias desarrollado en AWS Glue con Apache Spark para el procesamiento de datos masivos y generación de archivos para el sistema de notificaciones Latinia.

## 📋 Descripción del Proyecto

Este proyecto gestiona el procesamiento de diferentes tipos de notificaciones bancarias:
- **Agradecimiento de Pago**: Notificaciones de confirmación de pagos realizados
- **Puntos**: Notificaciones de acumulación de puntos en tarjetas de crédito
- **Empresas**: Notificaciones corporativas para clientes empresariales
- **Renovación de Tarjetas**: Notificaciones de renovación de tarjetas de crédito

El sistema procesa datos desde bases de datos Sybase y MySQL, genera archivos en formato `.build` compatibles con Latinia, y los almacena en Amazon S3.

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Sybase DB     │────│   AWS Glue      │────│   Amazon S3     │
│ (Transaccional) │    │   (Spark Jobs)  │    │ (Archivos .build)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                       ┌─────────────────┐
                       │   MySQL ODS     │
                       │ (Staging Data)  │
                       └─────────────────┘
```

## 📁 Estructura del Proyecto

```
notificaciones-y-colas-v5/
├── README.md                          # Documentación principal
├── requirements.txt                   # Dependencias del proyecto
├── ejemplo.json                       # Ejemplo de estructura de datos
├── subida_archivos.txt               # Log de archivos subidos
├── conf_emi_posiciones_comercio.json # Configuración de posiciones
├── main.py                           # Script principal
├── prueba.bash                       # Script de pruebas
├── exec.bash                         # Script de ejecución
├── job_emi_insert.py                 # Job de inserción
├── job_emi_procesa_noti_proc.py      # Job de procesamiento
├── sp.sql                           # Stored procedures
├── test_connection.py               # Test de conexiones
├── upload.txt                       # Control de uploads
├──
├── batch-noti-linea/                 # Módulo de notificaciones de línea
│
├── notificaciones-y-colas-v5/       # Módulo principal
│   ├── ejemplo.json
│   ├── requirements.txt
│   ├── subida_archivos.txt
│   │
│   ├── batch/                        # Jobs de procesamiento batch
│   │   ├── notificaciones_minsait.py # Job principal de Minsait
│   │   └── notificaciones de colas/   # Módulos específicos por tipo
│   │       └── batch/
│   │           ├── agradecimiento de pago/
│   │           │   ├── job_mss_genera_fr_noti_agradec.py
│   │           │   ├── exec.bash
│   │           │   ├── script.sql
│   │           │   ├── sp.sql
│   │           │   └── tabla.sql
│   │           │
│   │           ├── puntos/
│   │           │   ├── job_mss_genera_fr_noti_ptos.py
│   │           │   ├── emi_tc_aux_correo_puntos.sql
│   │           │   ├── emi_tc_datos_masivos_puntos.sql
│   │           │   ├── pa_con_ppuntosmcvs.sql
│   │           │   ├── pa_tcre_cciclofact.sql
│   │           │   └── pa_tcre_cciclofact.sql
│   │           │
│   │           ├── empresas/
│   │           │   ├── job_mss_genera_fr_noti_emp.py
│   │           │   ├── fn_split_str.sql
│   │           │   ├── pa_con_nnotiempresarial.sql
│   │           │   └── pa_tcr_bnotiempresas.sql
│   │           │
│   │           ├── renovacion_tarjeta/
│   │           │   ├── job_mss_genera_fr_noti_renovtrj.py
│   │           │   ├── emi_tcre_tmp_alr_renov.sql
│   │           │   ├── pa_tcr_b_noti_renov_tc.sql
│   │           │   └── pa_tcre_binfalrrenv.sql
│   │           │
│   │           └── resources/
│   │               ├── mssql-jdbc-12.10.1.jre11.jar
│   │               └── mysql-connector.jar
│   │
│   ├── main-lambda-component/         # Función Lambda principal
│   │   ├── lambda_function.py
│   │   ├── config-dev.yml
│   │   ├── permisos.txt
│   │   ├── request_validation.py
│   │   ├── requirements.txt
│   │   ├── test_lambda_function.py
│   │   ├── __pycache__/
│   │   ├── config/
│   │   │   └── nemonic_config.json
│   │   ├── test/
│   │   │   ├── config_file.py
│   │   │   ├── invalid_request.json
│   │   │   ├── requirements.txt
│   │   │   ├── valid_request.json
│   │   │   ├── validation_request.py
│   │   │   └── __pycache__/
│   │   └── utils/
│   │       ├── utils.py
│   │       └── __pycache__/
│   │
│   ├── main-lambda-event-minsait/     # Lambda para eventos Minsait
│   │   ├── lambda_function.py
│   │   ├── config-dev.yml
│   │   ├── requirements.txt
│   │   └── utils.py
│   │
│   ├── scripts/                       # Scripts de utilidad
│   │   └── catalogo.sql
│   │
│   └── sqs-handler/                   # Manejador de colas SQS
│       ├── lambda_function.py
│       └── requirements.txt
│
└── resources/                         # Recursos compartidos
    ├── mssql-jdbc-12.10.1.jre11.jar
    └── mysql-connector.jar
```

## 🔧 Componentes Principales

### 📊 Jobs de AWS Glue

#### 1. Agradecimiento de Pago
- **Archivo**: `job_mss_genera_fr_noti_agradec.py`
- **Propósito**: Procesa notificaciones de confirmación de pagos
- **Tablas**: `emi_noti_agradec_pago_inf`
- **SP Principal**: `pa_tcr_bobtener_noti`
- **Funcionalidades**:
  - Formateo de datos según SP `pa_tcr_bobtener_noti`
  - Generación de archivos .build con límite de 20,000 registros
  - Actualización de datos de notificación
  - Subida automática a S3

#### 2. Puntos
- **Archivo**: `job_mss_genera_fr_noti_ptos.py`
- **Propósito**: Procesa notificaciones de acumulación de puntos
- **Tablas**: `emi_noti_puntos_gen_inf`
- **SP Principal**: `pa_tcr_bnotipuntos`
- **Funcionalidades**:
  - Validación de ciclos de facturación
  - Actualización de datos de clientes desde Sybase
  - Formateo de datos de puntos
  - Generación de archivos con secuencial incremental

#### 3. Empresas
- **Archivo**: `job_mss_genera_fr_noti_emp.py`
- **Propósito**: Procesa notificaciones corporativas
- **SP Principal**: `pa_tcr_bnotiemptcemas`
- **Funcionalidades**:
  - Procesamiento de datos empresariales
  - Generación de archivos .build y logs
  - Manejo de prefijos para diferentes tipos de contenido

#### 4. Renovación de Tarjetas
- **Archivo**: `job_mss_genera_fr_noti_renovtrj.py`
- **Propósito**: Procesa notificaciones de renovación
- **SP Principal**: `pa_tcr_balr_renov_tc`

### 🔄 Funciones Lambda

#### Lambda Principal (`main-lambda-component`)
- Orquesta el flujo de notificaciones
- Valida requests entrantes con `request_validation.py`
- Gestiona la configuración del sistema
- Manejo de errores y logging

#### Lambda Minsait (`main-lambda-event-minsait`)
- Maneja eventos específicos de Minsait
- Procesa datos de transacciones
- Integración con sistemas externos

## ⚙️ Configuración

### Variables de Entorno (Glue Jobs)

```bash
# Configuración de Base de Datos
SYBASE_SECRET=conn_cob_cuentas          # Secret Manager para Sybase
RDS_MYSQL_SECRET=mysql_mock             # Secret Manager para MySQL
DRIVER_MYSQL=com.mysql.cj.jdbc.Driver
DRIVER_SYBASE=com.microsoft.sqlserver.jdbc.SQLServerDriver
JAR_SYBASE_PATH=s3://path/to/mssql-jdbc-12.10.1.jre11.jar

# Configuración de Archivos
NEMONICO=NAPTC                          # Código de identificación
PREFIJO=TCR                             # Prefijo para archivos
MAX_RECORDS=20000                       # Máx registros por archivo

# Configuración S3
S3_BUCKET_NAME=bb-emisormdp-datasource
S3_PREFIX=notificaciones/               # Prefijo en S3

# Configuración de Proceso
TIPO_PROCESO=B                          # Tipo de proceso (A/B)
DIAS=1                                  # Días de procesamiento
REGION_NAME=us-east-1                   # Región AWS
DEBUG=false                             # Modo debug
```

### Estructura de Archivos Generados

Los archivos `.build` siguen este formato:

```
BOLIVARIANO|{NEMONICO}||inot2|Avisos24|{CANTIDAD_REGISTROS}
{PREFIJO}{TIMESTAMP}{SECUENCIAL}||{DATOS_FORMATEADOS}
<EOF>
```

**Ejemplo Agradecimiento:**
```
BOLIVARIANO|NAPTC||inot2|Avisos24|1500
TCR20250728143015000001||123456|email=cliente@banco.com||NAPTC|CUENTA_TC=123456##TI_IDENT=C##CO_IDENT=1234567890##...
<EOF>
```

**Ejemplo Puntos:**
```
BOLIVARIANO|NPGTC||inot2|Avisos24|1200
TCR20250728143015000001||987654|email=cliente@banco.com||NPGTC|nombre_cliente=Juan Pérez##nrotrj=**** **** **** *123##tiptrj=Visa Classic##puntos=1500
<EOF>
```

## 🚀 Ejecución

### Ejecución Local (Desarrollo)

```bash
# Navegar al directorio del job específico
cd batch/notificaciones\ de\ colas/batch/agradecimiento\ de\ pago/

# Ejecutar con script local
./exec.bash

# O ejecutar directamente con Python
python job_mss_genera_fr_noti_agradec.py \
  --JOB_NAME test-job \
  --SYBASE_SECRET conn_cob_cuentas \
  --RDS_MYSQL_SECRET mysql_mock \
  --NEMONICO NAPTC
```

### Ejecución en AWS Glue

```bash
# Usando AWS CLI
aws glue start-job-run \
  --job-name job_mss_genera_fr_noti_agradec \
  --arguments '{
    "--SYBASE_SECRET":"conn_cob_cuentas",
    "--RDS_MYSQL_SECRET":"mysql_mock",
    "--NEMONICO":"NAPTC",
    "--MAX_RECORDS":"20000",
    "--S3_BUCKET_NAME":"bb-emisormdp-datasource"
  }'
```

## 🔍 Flujo de Procesamiento

### Flujo General
1. **Inicialización**: Configuración de Spark y conexiones a BD
2. **Extracción**: Ejecución de stored procedures específicos
3. **Validación**: Verificación de datos y reglas de negocio
4. **Transformación**: Aplicación de lógica usando PySpark
5. **Formateo**: Conversión al formato requerido por Latinia
6. **Segmentación**: División en archivos de máximo 20,000 registros
7. **Generación**: Creación de archivos .build con nomenclatura específica
8. **Almacenamiento**: Subida a Amazon S3
9. **Cleanup**: Limpieza de archivos temporales

### Prefijos de Contenido
- **Prefijo "1"**: Indica inicio de nuevo archivo .build
- **Prefijo "2"**: Contenido del archivo .build
- **Prefijo "3"**: Información de log/metadata

## 📊 Monitoreo y Logs

### Logs de AWS Glue
- Los logs se almacenan automáticamente en CloudWatch
- Configuración de logging nivel INFO, WARNING, ERROR
- Trazabilidad completa del procesamiento

### Métricas Clave
- Número de registros procesados por tipo
- Archivos generados exitosamente
- Tiempo de ejecución por job
- Errores de procesamiento y conexión
- Tamaño de archivos generados

## 🛠️ Desarrollo

### Requisitos del Entorno

```bash
# Python 3.8+
python -m pip install -r requirements.txt

# Dependencias principales
pymysql==1.1.0
boto3==1.35.0
pyspark==3.5.0
jaydebeapi==1.2.3

# Para desarrollo local
# Java 8 o 11
# Drivers JDBC (incluidos en resources/)
```

### Estructura de Código Estándar

Cada job sigue esta estructura:

```python
# 1. Configuración e inicialización
def get_secret(vault_name, region_name)
def execute_read_query(query, connection)
def execute_write_query(df, table_name, connection)
def execute_stored_procedure(proc_name, connection, params)

# 2. Lógica de negocio específica
def format_notification_data()
def update_notification_data()

# 3. Generación de archivos
def generate_build_files(df, output_path)
def upload_files_to_s3(file_paths, bucket_name, s3_prefix)

# 4. Función principal
def main()
```

### Clases de Excepción Personalizadas

```python
class GlueJobError(Exception): """Excepción base"""
class DataValidationError(GlueJobError): """Error en validación"""
class ConfigurationError(GlueJobError): """Error en configuración"""
class DataProcessingError(GlueJobError): """Error en procesamiento"""
class FileNotFoundError(GlueJobError): """Error en lectura de archivo"""
```

## 🔒 Seguridad

- **AWS Secrets Manager**: Credenciales de BD almacenadas de forma segura
- **IAM Roles**: Permisos mínimos necesarios para cada componente
- **VPC**: Acceso controlado a recursos de red
- **Encriptación**: S3 y RDS con encriptación en reposo y en tránsito
- **Logging**: No se registran datos sensibles en logs

## 📝 Datos de Ejemplo

### Formato de Entrada (JSON)
```json
{
  "refService": "CONIN",
  "channels": "SAT",
  "data": {
    "tipotrj": "Visa",
    "valor": "89.99",
    "numtrj": "**** **** **** 0101",
    "nombre_cliente": "Luis Martínez",
    "fecha": "2025-05-27"
  },
  "addresses": [
    {
      "className": "email",
      "type": "to", 
      "ref": "cliente@example.com"
    }
  ]
}
```

### Estructura de Tabla Principal (MySQL)
```sql
-- Tabla de agradecimiento de pago
CREATE TABLE emi_noti_agradec_pago_inf (
    i_cod_mis INT,
    s_correo VARCHAR(100),
    i_num_cuenta BIGINT,
    s_tipo_identifica VARCHAR(2),
    s_identifica VARCHAR(20),
    s_nom_plas VARCHAR(50),
    s_descrip_bi VARCHAR(100),
    d_fecha_proceso DATE,
    -- ... otros campos
);
```

## 📈 Optimizaciones y Escalabilidad

- **Paralelización**: Spark maneja distribución automática
- **Particionamiento**: Datos segmentados por fecha y tipo
- **Auto-scaling**: Glue ajusta recursos según carga
- **Broadcast Joins**: Para tablas pequeñas de catálogo
- **Columnar Storage**: Optimización de lectura en S3
- **Connection Pooling**: Reutilización de conexiones a BD

## 🔄 Pipeline de CI/CD

El proyecto utiliza GitHub Actions para:
- Validación de sintaxis Python
- Pruebas unitarias
- Deployment a AWS Glue
- Actualización de dependencias con Dependabot

## 📞 Soporte y Contacto

Para soporte técnico o preguntas sobre el proyecto:
- **Repository**: [AngZambrIroute/notificaciones-y-colas-v5](https://github.com/AngZambrIroute/notificaciones-y-colas-v5)
- **Issues**: Utilizar GitHub Issues para reportar bugs
- **Documentation**: README.md y comentarios en código

## 📄 Licencia

Este proyecto es propietario y confidencial. Todos los derechos reservados.

---

**Última actualización**: Julio 2025  
**Versión**: 5.0  
**Mantenido por**: Equipo de Desarrollo Digital
