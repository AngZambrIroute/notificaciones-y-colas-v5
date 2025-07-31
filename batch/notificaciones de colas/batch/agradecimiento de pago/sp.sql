create
    definer = aws_glue@`%` procedure pa_mdp_bnoti_agradec(IN e_fecha varchar(10))
begin
    declare v_fecha_proceso datetime;
    declare v_id_fecha_proceso int;
    declare v_cod_transac int;
    declare v_num_bin_excluye int;
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

    select cast(pa_valor as signed) into v_num_bin_excluye from adq_tc_m_parametros where pa_nombre='BinExclNotiAgradecPag';


    select cast(pa_valor as signed ) into v_cod_transac from adq_tc_m_parametros where pa_nombre='TipoFacPagos';
    

    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_estados_excluidos (
    i_id_estado INT,
        primary key (i_id_estado)

    ) ENGINE=MEMORY;

    CREATE TEMPORARY TABLE tmp_max_fechas (
        cuenta INT PRIMARY KEY,
        max_fecha INT
    ) ENGINE=MEMORY;

    -- 2. Tabla temporal para pagos iniciales
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_pagos (
        cuenta INT,
        monto DECIMAL(15,2),
        id_fec_proc INT,
        id_fec_tran INT,
        index idx_cuenta_proc (cuenta,id_fec_proc)

    ) ENGINE=MEMORY;

-- 4. Tabla temporal para datos agrupados
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_dato_agrp (
        cuenta INT PRIMARY KEY,
        numero_pago INT,
        monto_total DECIMAL(15,2),
        fec_tran DATE,
        fec_proc DATE,
        INDEX idx_cuenta (cuenta)
    ) ENGINE=MEMORY;

-- 5. Tabla temporal para datos de cliente
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_cliente (
        cuenta INT PRIMARY KEY,
        identifica VARCHAR(20),
        tipo_identifica CHAR(1),
        INDEX idx_cuenta (cuenta)
    ) ENGINE=MEMORY;

-- 6. Tabla temporal para datos de tarjeta
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_tarjeta (
        cuenta INT PRIMARY KEY,
        s_nom_plas1 VARCHAR(100),
        s_descripcion VARCHAR(100),
        INDEX idx_cuenta (cuenta)
    ) ENGINE=MEMORY;

-- 7. Tabla temporal para datos de cartera
    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_cartera (
        cuenta INT PRIMARY KEY,
        m_pago_min DECIMAL(15,2),
        m_sald_act_rot DECIMAL(15,2),
        m_sald_act_dif DECIMAL(15,2),
        m_sald_ant_rot DECIMAL(15,2),
        m_sum_creditos DECIMAL(15,2),
        d_fec_top_pag DATE,
        d_fecha DATE,
        INDEX idx_cuenta (cuenta)
    ) ENGINE=MEMORY;

    INSERT INTO tmp_estados_excluidos
    SELECT eb_id_estado
    FROM emi_h_estado_ctatarj_bb
    WHERE eb_cod_estado_hist IN (
        SELECT s_valor
        FROM emi_tmp_parcat
        WHERE s_codigo = 'tcr_excl_noti_agra'
    );




    INSERT INTO tmp_pagos
    SELECT DISTINCT
        tr.i_num_cuenta,
        tr.m_monto_trans,
        tr.i_id_fec_proc,
        tr.i_id_fec_tran
    FROM emi_transac_rotativas tr
    INNER JOIN emi_cuenta_cliente cc ON tr.i_num_cuenta = cc.i_num_cuenta
    INNER JOIN emi_ods_cliente cl ON cc.i_cod_cliente = cl.i_cod_cliente
    INNER JOIN emi_maestro_cartera_diaria cd ON tr.i_num_cuenta = cd.i_num_cuenta
        AND cd.i_id_fec_proceso = v_id_fecha_proceso
    WHERE tr.i_id_fec_proc = v_id_fecha_proceso
        AND tr.i_id_tip_trans = v_cod_transac
        AND tr.i_num_cuenta > 0
        AND cl.s_tipo_identifica IN ('C', 'P')
        AND cc.i_id_estado NOT IN (COALESCE((SELECT i_id_estado FROM tmp_estados_excluidos),0))
        AND cd.i_num_pagos_vcdos <= (
            SELECT CAST(s_valor AS SIGNED)
            FROM emi_tmp_parcat
            WHERE s_codigo = 'CPAEX'
        );

    insert into tmp_max_fechas select cuenta,max(id_fec_tran) from tmp_pagos group by cuenta;


    insert into tmp_dato_agrp
select
    p2.cuenta,
    count(1),
    sum(p2.monto),
    f.d_fecha,
    v_fecha_proceso
from tmp_pagos p2
join tmp_max_fechas mf on p2.cuenta=mf.cuenta join  emi_ods_fecha f on f.i_id_fecha = mf.max_fecha
group by p2.cuenta, f.d_fecha;


    insert into tmp_cliente select agr.cuenta,cl.S_identifica,cl.s_tipo_identifica from tmp_dato_agrp agr
        join emi_cuenta_cliente cc on agr.cuenta = cc.i_num_cuenta
        join emi_ods_cliente cl on cc.i_cod_cliente = cl.i_cod_cliente;

        insert into tmp_tarjeta select cuenta,mp.mp_nombenred,b.bp_descripcion
            from tmp_dato_agrp agr join emi_t_medio_pago_tarjeta mp on
            agr.cuenta = cast(mp.mp_cuenta as signed ) left join adq_tc_c_bin_propio
            b on b.bp_s_num_bin=cast(left(mp.mp_pan,6) as unsigned ) where mp.mp_calpart = 'TI';


    insert into tmp_cartera select cuenta, cd.m_pago_min,cd.m_sald_act_rot,cd.m_sald_act_dif,cd.m_sald_ant_rot,cd.m_pag_mes+cd.m_cred_mes + cd.m_dev_mes,
                                   cd.d_fec_top_pag,
                                   fc.d_fecha
                                   from tmp_dato_agrp agr
        join emi_maestro_cartera_diaria cd on agr.cuenta = cd.i_num_cuenta
        and cd.i_id_fec_proceso = v_id_fecha_proceso
        join emi_ods_fecha fc on cd.i_id_fec_cor_act = fc.i_id_fecha;



    insert into emi_noti_agradec_pago_inf (
        i_num_cuenta, s_identifica, s_tipo_identifica, s_nom_plas,
        s_descrip_bi, d_fecha_proceso, i_num_pago, m_monto,
        m_saldo_pm, m_saldo_rotativo, m_saldo_diferido,
        m_pago_contado, m_sum_credito,
        d_fec_top_pag, d_fec_corte, d_fecha_transac
    )
        select
            agr.cuenta,
            cl.identifica,
            cl.tipo_identifica,
            tp.s_nom_plas1,
            tp.s_descripcion,
            agr.fec_proc,
            agr.numero_pago,
            agr.monto_total,
            cd.m_pago_min,
            cd.m_sald_act_rot,
            cd.m_sald_act_dif,
            cd.m_sald_ant_rot,
            cd.m_sum_creditos,
            cd.d_fec_top_pag,
            cd.d_fecha,
            agr.fec_tran
        from tmp_dato_agrp agr
            join tmp_cliente cl on agr.cuenta = cl.cuenta
            join tmp_tarjeta tp on agr.cuenta = tp.cuenta
            join tmp_cartera cd on agr.cuenta = cd.cuenta;



    truncate table emi_tmp_parcat;
    truncate table emi_tcr_tmp_medio;
    DROP TEMPORARY TABLE IF EXISTS tmp_estados_excluidos;
    DROP TEMPORARY TABLE IF EXISTS tmp_pagos;
    DROP TEMPORARY TABLE IF EXISTS tmp_dato_agrp;
    DROP TEMPORARY TABLE IF EXISTS tmp_cliente;
    DROP TEMPORARY TABLE IF EXISTS tmp_tarjeta;
    drop temporary  table if exists tmp_max_fechas;
    DROP TEMPORARY TABLE IF EXISTS tmp_cartera;
end;

