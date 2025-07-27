CREATE TABLE emi_noti_agradec_pago_inf (
    i_num_cuenta INT NOT NULL,
    s_identifica VARCHAR(20) NOT NULL,
    s_tipo_identifica CHAR(2) NOT NULL,
    s_nom_plas VARCHAR(20) NOT NULL,
    s_descrip_bi VARCHAR(30) NOT NULL,
    d_fecha_proceso DATE NOT NULL,
    i_num_pago INT NOT NULL,
    m_monto DECIMAL(19,4) NOT NULL,
    m_saldo_pm DECIMAL(19,4) NOT NULL,
    m_saldo_rotativo DECIMAL(19,4) NOT NULL,
    m_saldo_diferido DECIMAL(19,4) NOT NULL,
    m_pago_contado DECIMAL(19,4) NOT NULL,
    m_sum_credito DECIMAL(19,4) NOT NULL,
    m_saldo_pc DECIMAL(19,4),
    d_fec_top_pag DATE NOT NULL,
    d_fec_corte DATE NOT NULL,
    d_fecha_transac DATE NOT NULL,
    i_cod_mis INT ,
    s_correo VARCHAR(50),
    s_movil VARCHAR(20)
);

create table emi_tmp_parcat (
    s_codigo varchar(50) not null,
    s_valor varchar(100) not null
)


CREATE TABLE emi_tcr_tmp_medio (
    identificacion VARCHAR(20) NOT NULL,
    tipo_identificacion CHAR(2) NOT NULL,
    mis INT(10) NOT NULL,
    telefono VARCHAR(15),
    correo VARCHAR(100)
);