CREATE TABLE emi_noti_renov_tc_info (
    t_tipo_alerta        VARCHAR(5)     NULL,
    t_id_cuenta          INT            NOT NULL,
    t_id_tarjeta         INT            NOT NULL,
    t_tipo_tarjeta       VARCHAR(50)    NULL,
    t_nro_tarjeta        VARCHAR(20)    NULL,
    t_id_cliente         INT            NOT NULL,
    t_nombre_cliente     VARCHAR(50)    NULL,
    t_identificacion     VARCHAR(20)    NULL,
    t_tipo_identifica    CHAR(2)        NULL,
    t_tipo_mov           VARCHAR(20)    NOT NULL,
    t_codigo_renov       VARCHAR(25)    NULL,
    t_motivo_renov       VARCHAR(64)    NULL,
    t_cod_bodega         VARCHAR(8)     NULL,
    t_bodega             VARCHAR(255)   NULL,
    t_id_fec_proc        INT            NULL,
    t_fecha_envio        DATETIME       NULL,
    t_telefono           VARCHAR(20)    NULL,
    t_mail               VARCHAR(40)    NULL
);

