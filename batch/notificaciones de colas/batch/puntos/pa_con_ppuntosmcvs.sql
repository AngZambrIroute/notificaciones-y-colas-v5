create
    definer = aws_glue@`%` procedure pa_con_ppuntosmcvs(IN fechaproceso varchar(10), IN i_marca varchar(10))
BEGIN
    DECLARE v_fecha_proceso DATE;
    DECLARE v_bines_marca VARCHAR(500);
    DECLARE v_error_msg VARCHAR(200);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        DROP TEMPORARY TABLE IF EXISTS PuntoGanados;
        RESIGNAL;
    END;

    IF i_marca IS NULL OR i_marca NOT IN ('MASTERCARD', 'VISA') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'El parámetro i_marca es obligatorio y debe ser MASTERCARD o VISA';
    END IF;

    IF fechaproceso IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'El parámetro fechaproceso es obligatorio';
    END IF;
    set v_fecha_proceso = STR_TO_DATE(fechaproceso, '%Y-%m-%d');

    -- Obtener los bines de la marca específica
    SELECT pa_valor INTO v_bines_marca
    FROM adq_tc_m_parametros
    WHERE pa_nombre = CONCAT('BINES_',i_marca) COLLATE utf8mb4_general_ci
      AND pa_parent = 'NOTIFICACION_PUNTOS' COLLATE utf8mb4_general_ci
    LIMIT 1;

    IF v_bines_marca IS NULL THEN
        SET v_error_msg = CONCAT('No se encontraron bines para la marca: ', i_marca);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_msg;
    END IF;

    START TRANSACTION;

    IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'emi_noti_puntos_gen_inf' AND table_schema = DATABASE()) THEN
        -- TRUNCATE TABLE emi_noti_puntos_gen_inf;
        DELETE from emi_noti_puntos_gen_inf WHERE i_fecha_proceso = v_fecha_proceso;
    ELSE
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'La tabla emi_noti_puntos_gen_inf no existe. Ejecute primero el script de creación de tabla.';
    END IF;

    DROP TEMPORARY TABLE IF EXISTS PuntoGanados;

    CREATE TEMPORARY TABLE PuntoGanados AS
    SELECT
        a.sp_fecproces,
        a.sp_cuenta,
        b.i_cod_cliente,
        b.s_num_bin AS Bin,
        a.sp_pan AS tarjeta,
        SUM(a.sp_generados) AS PuntosGenerados,
        SUM(a.sp_ganados) AS PuntosGanados,
        bi.bp_marca,
        bi.bp_descripcion
    FROM emi_t_saldo_puntos a
    INNER JOIN emi_cuenta_cliente b ON b.i_num_cuenta = a.sp_cuenta
    INNER JOIN emi_tarjeta_propia tar ON b.i_num_cuenta = tar.i_num_cuenta
                                      AND b.i_num_bin = LEFT(tar.s_NumTarjeta,6)
                                      AND tar.s_tipo_tarjeta = 'P'
    INNER JOIN emi_ods_fecha f ON f.d_fecha = STR_TO_DATE(a.sp_fecproces, '%Y-%m-%d')
    INNER JOIN adq_tc_c_bin_propio bi ON bi.bp_num_bin = CAST(b.s_num_bin as signed )
    -- EXCLUSIONES POR AFINIDAD NG
                                     
    WHERE f.d_fecha = v_fecha_proceso
      AND FIND_IN_SET(bi.bp_marca COLLATE utf8mb4_general_ci, v_bines_marca) > 0
      AND a.sp_generados > 0
    GROUP BY
        a.sp_fecproces,
        a.sp_cuenta,
        b.s_num_bin,
        a.sp_pan,
        b.i_cod_cliente,
        bi.bp_marca,
        bi.bp_descripcion;
    INSERT INTO emi_noti_puntos_gen_inf (
        i_fecha_proceso,
        i_num_cuenta,
        i_cod_cliente,
        ente,
        tarjeta,
        s_num_bin,
        PuntosGenerados,
        PuntosGanados,
        nombres,
        apellidos,
        identificacion,
        tipo_identificacion,
        s_marca,
        s_descripcion,
        correo_electronico
    )
    SELECT
        a.sp_fecproces,
        CAST(a.sp_cuenta AS SIGNED),
        CAST(a.i_cod_cliente AS SIGNED),
        b.i_cod_cliente,
        CAST(a.tarjeta AS CHAR(21)),
        CAST(a.Bin AS CHAR(10)),
        CAST(a.PuntosGenerados AS SIGNED),
        CAST(a.PuntosGanados AS SIGNED),
        NULL,
        NULL,
        CAST(b.s_identifica AS CHAR(13)),
        CAST(b.s_tipo_identifica AS CHAR(3)),
        CAST(a.bp_marca AS CHAR(3)),
        a.bp_descripcion,
        NULL
    FROM PuntoGanados a
    INNER JOIN emi_ods_cliente b ON a.i_cod_cliente = b.i_cod_cliente
    WHERE b.s_tipo_identifica COLLATE utf8mb4_general_ci <> 'R'
      AND a.PuntosGanados > 0
      AND a.PuntosGanados != 0;

    DELETE FROM emi_noti_puntos_gen_inf WHERE PuntosGanados = 0;

    DROP TEMPORARY TABLE PuntoGanados;
    truncate table emi_noti_puntos_dato_cliente;

    COMMIT;

    SELECT
        fechaproceso AS FechaProceso,
        ROW_COUNT() AS RegistrosProcesados,
        'Proceso de extracción completado exitosamente' AS Mensaje;



END;

