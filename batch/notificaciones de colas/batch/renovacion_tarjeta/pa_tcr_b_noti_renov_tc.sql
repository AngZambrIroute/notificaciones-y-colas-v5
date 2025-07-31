create procedure pa_tcr_balr_renov_tc( IN e_fecha varchar(10))
BEGIN
    declare v_fecha_proceso datetime;
    declare v_id_fecha_proceso int;
    declare exit handler for sqlexception
    BEGIN
        ROLLBACK;
        DROP TEMPORARY TABLE IF EXISTS tmp_estados_excluidos;
        DROP TEMPORARY TABLE IF EXISTS tmp_pagos;
        DROP TEMPORARY TABLE IF EXISTS tmp_dato_agrp;
        DROP TEMPORARY TABLE IF EXISTS tmp_cliente;
        DROP TEMPORARY TABLE IF EXISTS tmp_tarjeta;
        DROP TEMPORARY TABLE IF EXISTS tmp_cartera;
        RESIGNAL;
    END;
    start transaction ;
    set v_fecha_proceso = STR_TO_DATE(e_fecha, '%Y-%m-%d');
    select i_id_fecha into v_id_fecha_proceso from emi_ods_fecha where d_fecha = v_fecha_proceso;


    CREATE TEMPORARY TABLE tmp_t_mov_inventario (
    i_id_tarjeta INT,
    i_num_cuenta INT,
    i_cod_bod_act INT,
    i_cod_bod_ant INT,
    i_id_fec_proc INT,
    i_id_fec_mov INT,
    d_fec_mov DATE,
    s_motivo VARCHAR(50),
    s_mv_tipo VARCHAR(10)
    );
    create temporary  table tmp_t_bin_membresia(
        s_bin_menbresia VARCHAR(6) NOT NULL,
        i_cod_pais INT NOT NULL,
        s_descripcion VARCHAR(100) NULL
    );
    INSERT INTO tmp_t_mov_inventario (
    i_id_tarjeta,
    i_num_cuenta,
    i_cod_bod_act,
    i_cod_bod_ant,
    i_id_fec_proc,
    i_id_fec_mov,
    d_fec_mov,
    s_motivo,
    s_mv_tipo
    )
    SELECT
        m.i_id_tarjeta,
        m.i_num_cuenta,
        m.i_cod_bod_act,
        m.i_cod_bod_ant,
        m.i_id_fec_proc,
        m.i_id_fec_mov,
        m.d_fec_mov,
        m.s_motivo,
        m.s_mv_tipo
    FROM emi_mov_inventario m
    WHERE m.i_id_fec_mov = v_id_fecha_proceso;

    -- truncate emi_noti_renov_tc_info

    INSERT INTO emi_noti_renov_tc_info (
    t_tipo_alerta,
    t_id_cuenta,
    t_id_tarjeta,
    t_tipo_tarjeta,
    t_nro_tarjeta,
    t_id_cliente,
    t_nombre_cliente,
    t_identificacion,
    t_tipo_identifica,
    t_tipo_mov,
    t_codigo_renov,
    t_motivo_renov,
    t_cod_bodega,
    t_bodega,
    t_id_fec_proc,
    t_fecha_envio,
    t_telefono,
    t_mail
)
SELECT
    CAST('' AS CHAR(5)) AS t_tipo_alerta,
    tm.i_num_cuenta AS t_id_cuenta,
    tm.i_id_tarjeta AS t_id_tarjeta,
    CAST('' AS CHAR(50)) AS t_tipo_tarjeta,
    p.s_tarj_unica AS t_nro_tarjeta,
    0 AS t_id_cliente,
    CAST('' AS CHAR(50)) AS t_nombre_cliente,
    CAST('' AS CHAR(20)) AS t_identificacion,
    CAST('' AS CHAR(2)) AS t_tipo_identifica,
    tm.s_motivo AS t_tipo_mov,
    c.s_codigo_banco AS t_codigo_renov,
    CAST('' AS CHAR(64)) AS t_motivo_renov,
    b.s_codigo_banco AS t_cod_bodega,
    b.s_descripcion_banco AS t_bodega,
    tm.i_id_fec_proc AS t_id_fec_proc,
    @v_fecha_dia AS t_fecha_envio,
    CAST('' AS CHAR(20)) AS t_telefono,
    CAST('' AS CHAR(40)) AS t_mail
    FROM tmp_t_mov_inventario tm
    INNER JOIN (
        SELECT DISTINCT
        pa.i_id_tarjeta,
        pa.i_rzn_gen_pla,
        pa.s_tarj_unica,
        ROW_NUMBER() OVER (PARTITION BY pa.i_id_tarjeta ORDER BY pa.i_id_fec_impresion DESC) as rn
    FROM emi_platico_tarjeta pa
    ) p ON p.i_id_tarjeta = tm.i_id_tarjeta AND p.rn = 1
    INNER JOIN adq_tc_m_catalogos c ON c.s_codigo_banco = p.i_rzn_gen_pla
    AND c.s_nom_catalogo = 'RazonGenPlasCla'
    INNER JOIN adq_tc_m_catalogos b ON b.s_codigo_banco = tm.i_cod_bod_ant
    AND b.s_nom_catalogo = 'BODEGA_TDC';

    insert into tmp_t_bin_membresia select bp_bin_membresia,bp_cod_pais,bp_descripcion from adq_tc_c_bin_propio where bp_cod_pais=125 and bp_bin_membresia!='';
    UPDATE emi_noti_renov_tc_info t
    LEFT JOIN tmp_t_bin_membresia bin ON bin.s_bin_menbresia = LEFT(t.t_nro_tarjeta, 6)
    SET t.t_tipo_tarjeta = COALESCE(bin.s_descripcion, '') WHERE t.t_tipo_tarjeta = '' OR t.t_tipo_tarjeta IS NULL;
end;

