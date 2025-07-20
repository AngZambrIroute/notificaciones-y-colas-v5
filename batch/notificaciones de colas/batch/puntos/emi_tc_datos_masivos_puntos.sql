-- Script para crear la tabla emi_tc_datos_masivos_puntos
-- Ejecutar este script antes de usar el stored procedure sp_proceso_extraccion_mastercard

-- Eliminar tabla si existe
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'emi_tc_datos_masivos_puntos') AND type IN (N'U')) 
    DROP TABLE emi_tc_datos_masivos_puntos;

-- Crear tabla emi_tc_datos_masivos_puntos
CREATE TABLE emi_tc_datos_masivos_puntos (
    i_fecha_proceso INT NULL,
    i_num_cuenta INT NULL,
    s_num_bin VARCHAR(10) NULL,
    i_cod_cliente INT NULL,
    ente INT NULL,
    tarjeta VARCHAR(21) NULL,
    PuntosGenerados INT NULL,
    PuntosGanados INT NULL,
    nombres VARCHAR(400) NULL,
    apellidos VARCHAR(400) NULL,
    identificacion VARCHAR(30) NULL,
    tipo_identificacion VARCHAR(3) NULL,
    correo_electronico VARCHAR(400) NULL,
    s_descripcion VARCHAR(400) NULL,
    s_marca VARCHAR(3) NULL
);

-- Crear índices para mejorar el rendimiento
CREATE INDEX IX_emi_tc_datos_masivos_puntos_fecha_proceso ON emi_tc_datos_masivos_puntos(i_fecha_proceso);
CREATE INDEX IX_emi_tc_datos_masivos_puntos_num_cuenta ON emi_tc_datos_masivos_puntos(i_num_cuenta);
CREATE INDEX IX_emi_tc_datos_masivos_puntos_cod_cliente ON emi_tc_datos_masivos_puntos(i_cod_cliente);
CREATE INDEX IX_emi_tc_datos_masivos_puntos_marca ON emi_tc_datos_masivos_puntos(s_marca);

PRINT 'Tabla emi_tc_datos_masivos_puntos creada exitosamente con índices de rendimiento.';
