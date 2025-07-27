-- Script para crear la tabla emi_tc_datos_masivos_puntos
-- Ejecutar este script antes de usar el stored procedure sp_proceso_extraccion_mastercard

-- Eliminar tabla si existe
IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'emi_tc_datos_masivos_puntos') AND type IN (N'U')) 
    DROP TABLE emi_noti_puntos_gen_inf;

-- Crear tabla emi_noti_puntos_gen_inf
CREATE TABLE emi_noti_puntos_gen_inf (
    i_fecha_proceso INT NULL,
    i_num_cuenta INT NULL,
    s_num_bin INT NULL,
    i_cod_cliente INT NULL,
    ente INT NULL,
    tarjeta VARCHAR(22) NULL,
    PuntosGenerados INT NULL,
    PuntosGanados INT NULL,
    nombres VARCHAR(100) NULL,
    apellidos VARCHAR(100) NULL,
    identificacion VARCHAR(30) NULL,
    tipo_identificacion VARCHAR(2) NULL,
    correo_electronico VARCHAR(100) NULL,
    s_descripcion VARCHAR(100) NULL,
    s_marca VARCHAR(3) NULL
);

-- Crear índices para mejorar el rendimiento
CREATE INDEX IX_emi_noti_puntos_gen_inf_fecha_proceso ON emi_noti_puntos_gen_inf(i_fecha_proceso);
CREATE INDEX IX_emi_noti_puntos_gen_inf_num_cuenta ON emi_noti_puntos_gen_inf(i_num_cuenta);
CREATE INDEX IX_emi_noti_puntos_gen_inf_cod_cliente ON emi_noti_puntos_gen_inf(i_cod_cliente);
CREATE INDEX IX_emi_noti_puntos_gen_inf_marca ON emi_noti_puntos_gen_inf(s_marca);

PRINT 'Tabla emi_noti_puntos_gen_inf creada exitosamente con índices de rendimiento.';
