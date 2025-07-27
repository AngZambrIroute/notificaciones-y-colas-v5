DELIMITER $$

CREATE PROCEDURE pa_tcre_cciclofact(
    IN p_tipo_proceso VARCHAR(2),
    IN p_fechaproceso DATE,
    IN p_dia INT
)
BEGIN
    -- Validar parámetros obligatorios
    IF p_tipo_proceso IS NULL OR p_fechaproceso IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Los parámetros p_tipo_proceso y p_fechaproceso son obligatorios';
    END IF;
    
    IF p_tipo_proceso = 'A' THEN
        SELECT 
            a.s_cod_ciclo AS ciclo,
            DATE_FORMAT(fechaCierre.d_fecha, '%m/%d/%Y') AS fecha,
            DATE_FORMAT(fechaAntes.d_fecha, '%m/%d/%Y') AS fechaAntes
        FROM emi_tc_ciclo_facturacion a
        INNER JOIN emi_ods_fecha fechaCierre ON fechaCierre.i_id_fecha = a.i_id_fec_cierre
        INNER JOIN emi_tc_ciclo_facturacion antes ON antes.i_mes_cierre = CASE WHEN (a.i_mes_cierre - 1) = 0 THEN 12 ELSE (a.i_mes_cierre - 1) END
                                                  AND antes.i_anio_cierre = CASE WHEN (a.i_mes_cierre - 1) = 0 THEN (a.i_anio_cierre - 1) ELSE a.i_anio_cierre END
                                                  AND antes.s_cod_ciclo = a.s_cod_ciclo
        INNER JOIN emi_ods_fecha fechaAntes ON fechaAntes.i_id_fecha = antes.i_id_fec_cierre 
        INNER JOIN emi_tc_ciclo_facturacion despues ON despues.i_mes_cierre = CASE WHEN (a.i_mes_cierre + 1) = 13 THEN 1 ELSE (a.i_mes_cierre + 1) END
                                                    AND despues.i_anio_cierre = CASE WHEN (a.i_mes_cierre + 1) = 13 THEN (a.i_anio_cierre + 1) ELSE a.i_anio_cierre END
                                                    AND despues.s_cod_ciclo = a.s_cod_ciclo
        INNER JOIN emi_ods_fecha fechadespues ON fechadespues.i_id_fecha = despues.i_id_fec_cierre 
        INNER JOIN emi_ods_fecha fechatopepago ON fechatopepago.i_id_fecha = a.i_id_fec_prox_pago 
        WHERE fechaCierre.d_fecha = p_fechaproceso
        ORDER BY fechaCierre.d_fecha;
        
    ELSEIF p_tipo_proceso = 'B' THEN
        -- Validar parámetro específico para tipo B
        IF p_dia IS NULL THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'El parámetro p_dia es obligatorio para el tipo de proceso B';
        END IF;
        
        -- Calcular fecha de vencimiento
        SET @FechaVencimiento = DATE_ADD(p_fechaproceso, INTERVAL p_dia DAY);

        SELECT 
            a.s_cod_ciclo AS ciclo,
            DATE_FORMAT(fechaCierre.d_fecha, '%m/%d/%Y') AS fecha,
            a.i_mes_cierre,
            DATE_FORMAT(fechaAntes.d_fecha, '%m/%d/%Y') AS fechaAntes
        FROM emi_tc_ciclo_facturacion a 
        INNER JOIN emi_ods_fecha fechaCierre ON fechaCierre.i_id_fecha = a.i_id_fec_cierre
        INNER JOIN emi_tc_ciclo_facturacion antes ON antes.i_mes_cierre = CASE WHEN (a.i_mes_cierre - 1) = 0 THEN 12 ELSE (a.i_mes_cierre - 1) END
                                                  AND antes.i_anio_cierre = CASE WHEN (a.i_mes_cierre - 1) = 0 THEN (a.i_anio_cierre - 1) ELSE a.i_anio_cierre END
                                                  AND antes.s_cod_ciclo = a.s_cod_ciclo
        INNER JOIN emi_ods_fecha fechaAntes ON fechaAntes.i_id_fecha = antes.i_id_fec_cierre 
        WHERE fechaCierre.d_fecha = @FechaVencimiento
          AND a.s_cod_ciclo IN (1, 2)
        ORDER BY fechaCierre.d_fecha;
        
    ELSE
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Tipo de proceso no válido. Use ''A'' o ''B''';
    END IF;
    
END$$

DELIMITER ;  