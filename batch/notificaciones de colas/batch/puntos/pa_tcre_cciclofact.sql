CREATE PROCEDURE pa_tcre_cciclofact
    @tipo_proceso VARCHAR(2),
    @fechaproceso DATE,
    @dia INT
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @tipo_proceso IS NULL OR @fechaproceso IS NULL
    BEGIN
        RAISERROR('Los parámetros @tipo_proceso y @fechaproceso son obligatorios', 16, 1);
        RETURN;
    END
    
    
    IF @tipo_proceso = 'A'
    BEGIN
        SELECT 
            ciclo = a.s_cod_ciclo,
            fecha = CONVERT(VARCHAR(20), fechaCierre.d_fecha, 101),
            fechaAntes = CONVERT(VARCHAR(20), fechaAntes.d_fecha, 101)
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
        WHERE fechaCierre.d_fecha = @fechaproceso
        ORDER BY fechaCierre.d_fecha;
    END 
    
    ELSE IF @tipo_proceso = 'B'
    BEGIN	
        IF @dia IS NULL
        BEGIN
            RAISERROR('El parámetro @dia es obligatorio para el tipo de proceso B', 16, 1);
            RETURN;
        END
        
        DECLARE @FechaVencimiento DATE;
        SET @FechaVencimiento = DATEADD(DAY, @dia, @fechaproceso);

        SELECT 
            ciclo = a.s_cod_ciclo, 
            fecha = CONVERT(VARCHAR(20), fechaCierre.d_fecha, 101),
            i_mes_cierre = a.i_mes_cierre,
            fechaAntes = CONVERT(VARCHAR(20), fechaAntes.d_fecha, 101)
        FROM emi_tc_ciclo_facturacion a 
        INNER JOIN emi_ods_fecha fechaCierre ON fechaCierre.i_id_fecha = a.i_id_fec_cierre
        INNER JOIN emi_tc_ciclo_facturacion antes ON antes.i_mes_cierre = CASE WHEN (a.i_mes_cierre - 1) = 0 THEN 12 ELSE (a.i_mes_cierre - 1) END
                                                  AND antes.i_anio_cierre = CASE WHEN (a.i_mes_cierre - 1) = 0 THEN (a.i_anio_cierre - 1) ELSE a.i_anio_cierre END
                                                  AND antes.s_cod_ciclo = a.s_cod_ciclo
        INNER JOIN emi_ods_fecha fechaAntes ON fechaAntes.i_id_fecha = antes.i_id_fec_cierre 
        WHERE fechaCierre.d_fecha = @FechaVencimiento
          AND a.s_cod_ciclo IN (1, 2)
        ORDER BY fechaCierre.d_fecha;
    END
    ELSE
    BEGIN
        RAISERROR('Tipo de proceso no válido. Use ''A'' o ''B''', 16, 1);
        RETURN;
    END
END  