DELIMITER $$

CREATE PROCEDURE pa_con_ppuntosmcvs(
    IN fechaproceso DATE,
    IN i_marca VARCHAR(3) DEFAULT 'MASTERCARD'  
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        -- Limpiar tabla temporal si existe
        DROP TEMPORARY TABLE IF EXISTS PuntoGanados;
        RESIGNAL;
    END;
    IF i_marca IS NULL OR i_marca not in ('MASTERCARD', 'VISA') THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'El parámetro i_marca es obligatorio y debe ser MASTERCARD o VISA';
    END IF;
    -- Validación de parámetros de entrada
    IF fechaproceso IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'El parámetro fechaproceso es obligatorio';
    END IF;
    
    START TRANSACTION;
    
    -- Limpiar tabla antes de insertar nuevos datos
    IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'emi_tc_datos_masivos_puntos' AND table_schema = DATABASE()) THEN
        TRUNCATE TABLE emi_tc_datos_masivos_puntos;
    ELSE
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'La tabla emi_tc_datos_masivos_puntos no existe. Ejecute primero el script de creación de tabla.';
    END IF;
    -- obtener marcas de bines propios en parametro
    
    -- Crear tabla temporal con puntos ganados
    DROP TEMPORARY TABLE IF EXISTS PuntoGanados;
    
    CREATE TEMPORARY TABLE PuntoGanados AS
    SELECT 
        a.i_id_fec_proceso,
        a.i_num_cuenta,
        b.i_cod_cliente,
        b.s_num_bin AS Bin,
        a.s_NumTarjeta AS tarjeta,
        SUM(a.m_pun_disponible) AS PuntosGenerados,
        SUM(a.m_pun_gan_mes) AS PuntosGanados,
        bi.s_marca,
        bi.s_descripcion 
    FROM db_tc_ods.emi_t_saldo_puntos a
    INNER JOIN emi_cuenta_cliente b ON b.i_num_cuenta = a.cuenta
    INNER JOIN ods_tarjeta_propia tar ON b.i_num_cuenta = tar.i_num_cuenta
                                      AND b.i_num_bin = tar.i_num_bin
                                      AND tar.s_tipo_tarjeta = 'P'
    INNER JOIN emi_ods_fecha f ON f.i_id_fecha = a.i_id_fec_proceso
    INNER JOIN adq_tc_c_bin_propio bi ON bi.s_num_bin = b.s_num_bin
    -- EXCLUSION POR AFINIDAD
    INNER JOIN adq_tc_m_catalogos car ON car.s_codigo_banco = b.i_id_afinidad
                                      AND car.s_nom_catalogo = 'Afinidades'
                                      AND car.s_codigo_banco NOT IN ('NG')
    INNER JOIN (
        SELECT dt.nemonico
        FROM dw_tablas dt
        INNER JOIN dw_catalogo dc ON dt.codigo_tabla = dc.codigo_tabla
                                  AND dc.estado = 'V'
        WHERE dt.titulo_tabla = 'ESTADO_CUENTA_PUNTOS'
    ) c ON c.nemonico = b.s_codigo_banco
    -- busqueda de marcas de bines propios
    WHERE f.d_fecha = fechaproceso
      AND FIND_IN_SET(
        bi.s_marca,
        (SELECT s_descripcion from adq_tc_m_parametros
         where pa_nombre = CONCAT('BINES_',i_marca) and pa_parent = 'NOTIFICACION_PUNTOS') LIMIT 1
      )
      AND a.ss_generados > 0
    GROUP BY 
        a.i_id_fec_proceso,
        a.i_num_cuenta,
        b.s_num_bin,
        a.s_NumTarjeta,
        b.i_cod_cliente,
        bi.s_marca,
        bi.s_descripcion;
    
    -- Insertar datos en la tabla principal
    INSERT INTO emi_tc_datos_masivos_puntos (
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
        a.i_id_fec_proceso,
        CAST(a.i_num_cuenta AS SIGNED),
        CAST(a.i_cod_cliente AS SIGNED),
        b.i_Nro_Cliente_Cobis,
        CAST(a.tarjeta AS CHAR(21)),
        CAST(a.Bin AS CHAR(10)),
        CAST(a.PuntosGenerados AS SIGNED),
        CAST(a.PuntosGanados AS SIGNED),
        CAST(b.s_nombres AS CHAR(400)),
        CAST(b.s_apellidos AS CHAR(400)),
        CAST(b.s_identifica AS CHAR(13)),
        CAST(b.s_tipo_identifica AS CHAR(3)),
        CAST(a.s_marca AS CHAR(3)),
        a.s_descripcion,
        CAST(IFNULL(b.s_email, '') AS CHAR(400))
    FROM PuntoGanados a 
    INNER JOIN emi_ods_cliente b ON a.i_cod_cliente = b.i_cod_cliente 
    WHERE b.s_tipo_identifica <> 'R'
      AND a.PuntosGanados > 0
      AND a.PuntosGanados != 0;
    
    -- Depuración adicional de puntos 0 (por seguridad)
    DELETE FROM emi_tc_datos_masivos_puntos WHERE PuntosGanados = 0;
    
    -- Limpiar tabla temporal
    DROP TEMPORARY TABLE PuntoGanados;
    
    COMMIT;
    
    -- Retornar información del proceso
    SELECT 
        fechaproceso AS FechaProceso,
        ROW_COUNT() AS RegistrosProcesados,
        'Proceso de extracción completado exitosamente' AS Mensaje;

END$$

DELIMITER ;