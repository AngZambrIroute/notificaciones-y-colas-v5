#!/bin/bash
# filepath: /home/hadoop/workspace/run_job.sh

spark-submit --jars ../resources/mysql-connector.jar,../resources/mssql-jdbc-12.10.1.jre11.jar  \
  job_mss_genera_fr_noti_ptos.py \
  --JOB_NAME job_mss_genera_fr_noti_ptos \
  --SYBASE_SECRET conn_cob_cuentas \
  --RDS_MYSQL_SECRET mysql_mock \
  --JAR_SYBASE_PATH ../resources/mssql-jdbc-12.10.1.jre11.jar \
  --REGION_NAME us-east-1 \
  --DRIVER_MYSQL com.mysql.cj.jdbc.Driver \
  --DRIVER_SYBASE com.microsoft.sqlserver.jdbc.SQLServerDriver \
  --NEMONICO NPGTC \
  --MAX_RECORDS 20000 \
  --PREFIJO TCR \
  --S3_BUCKET_NAME bb-emisormdp-datasource \
  --S3_PREFIX notificaciones/puntos \
  --TIPO_PROCESO B \
  --DIAS 20 \