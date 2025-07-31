drop procedure if exists pa_tcr_bnotiemptcemas;

DELIMITER //

CREATE PROCEDURE pa_tcr_bnotiemptcemas(
    IN e_fechaProceso VARCHAR(10),
    IN e_filename VARCHAR(50),
    IN e_fileadd VARCHAR(50),
    IN e_secuencial VARCHAR(3),
    IN e_nemonico VARCHAR(8),
    IN e_test_mode BOOLEAN
)
BEGIN
    DECLARE v_rows INT;
    DECLARE v_id INT;
    DECLARE v_filename VARCHAR(50);
    DECLARE v_fechaProceso DATETIME;

    SET v_fechaProceso = DATE_SUB(STR_TO_DATE(e_fechaProceso, '%Y-%m-%d'), INTERVAL 1 DAY);

    DROP TABLE IF EXISTS v_t_bines_afinidades;
    CREATE TEMPORARY TABLE v_t_bines_afinidades (
        id INT AUTO_INCREMENT PRIMARY KEY,
        s_num_bin VARCHAR(10),
        afinidad VARCHAR(10),
        i_id_afinidad BIGINT,
        destinatarios VARCHAR(255)
    );

    DROP TABLE IF EXISTS v_t_bines_afin_toline;
    CREATE TEMPORARY TABLE v_t_bines_afin_toline (
        _id INT AUTO_INCREMENT PRIMARY KEY,
        id INT,
        s_num_bin VARCHAR(10),
        afinidad VARCHAR(10),
        i_id_afinidad BIGINT,
        toline VARCHAR(255)
    );

    DROP TABLE IF EXISTS v_t_bines_afin_des;
    CREATE TEMPORARY TABLE v_t_bines_afin_des (
        s_num_bin VARCHAR(10),
        afinidad VARCHAR(10),
        i_id_afinidad BIGINT,
        descripcion VARCHAR(255)
    );

    DROP TABLE IF EXISTS v_t_destinatarios;
    CREATE TEMPORARY TABLE v_t_destinatarios (
        id INT,
        destinatario VARCHAR(255)
    );

    DROP TABLE IF EXISTS v_t_filesname;
    CREATE TEMPORARY TABLE v_t_filesname (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre_archivo VARCHAR(250)
    );

    DROP TABLE IF EXISTS v_t_files_result;
    CREATE TEMPORARY TABLE v_t_files_result (
        id INT AUTO_INCREMENT PRIMARY KEY,
        dato VARCHAR(800)
    );

    DROP TABLE IF EXISTS v_t_catalogo;
    CREATE TABLE v_t_catalogo (
        _id INT AUTO_INCREMENT PRIMARY KEY,
        i_id_catalogo_nuevo BIGINT,
        s_codigo_miembro VARCHAR(25),
        s_nom_miembro VARCHAR(100)
    );

    SET v_rows = 0;

    IF e_test_mode THEN
        -- Insertar datos quemados para pruebas
        INSERT INTO v_t_catalogo (i_id_catalogo_nuevo, s_codigo_miembro, s_nom_miembro)
        VALUES
        (1, '001', 'Afinidad 1'),
        (2, '002', 'Afinidad 2');


        INSERT INTO v_t_bines_afin_des (s_num_bin, afinidad, i_id_afinidad, descripcion)
        VALUES
        ('123456', '001', 1, 'Descripción 1'),
        ('789012', '002', 2, 'Descripción 2');

        INSERT INTO v_t_bines_afinidades (s_num_bin, afinidad, i_id_afinidad, destinatarios)
        VALUES
        ('123456', '001', 1, 'destinatario1@example.com,destinatario2@example.com'),
        ('789012', '002', 2, 'destinatario3@example.com');
    ELSE
        -- Insertar datos reales
        INSERT INTO v_t_catalogo (i_id_catalogo_nuevo, s_codigo_miembro, s_nom_miembro)
        SELECT s_codigo_banco, s_codigo_banco, s_nom_catalogo
        FROM adq_tc_m_catalogos
        WHERE s_nom_catalogo = 'Afinidades';

        INSERT INTO v_t_bines_afin_des (s_num_bin, afinidad, i_id_afinidad, descripcion)
        SELECT
            SUBSTRING_INDEX(b.nemonico, '-', 1) AS bin,
            SUBSTRING_INDEX(b.nemonico, '-', -1) AS afinidad,
            d.i_id_catalogo_nuevo,
            b.valor AS descripcion
        FROM emi_dw_tablas a
        INNER JOIN emi_dw_catalogo b ON a.codigo_tabla = b.codigo_tabla
        INNER JOIN adq_tc_c_bin_propio c ON SUBSTRING_INDEX(b.nemonico, '-', 1) = c.bp_num_bin
        INNER JOIN v_t_catalogo d ON SUBSTRING_INDEX(b.nemonico, '-', -1) = d.s_codigo_miembro
        WHERE a.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPRESARIAL'
        AND SUBSTRING_INDEX(b.nemonico, '-', -1) != 'XX'
        GROUP BY SUBSTRING_INDEX(b.nemonico, '-', 1), SUBSTRING_INDEX(b.nemonico, '-', -1), d.i_id_catalogo_nuevo, b.valor

        UNION ALL

        SELECT
            SUBSTRING_INDEX(f.nemonico, '-', 1) AS bin,
            i.s_codigo_miembro AS afinidad,
            h.i_id_afinidad,
            f.valor AS descripcion
        FROM emi_dw_tablas e
        INNER JOIN emi_dw_catalogo f ON e.codigo_tabla = f.codigo_tabla
        INNER JOIN adq_tc_c_bin_propio g ON SUBSTRING_INDEX(f.nemonico, '-', 1) = g.bp_num_bin
        INNER JOIN emi_cuenta_cliente h ON g.bp_num_bin = h.s_num_bin
        INNER JOIN v_t_catalogo i ON h.i_id_afinidad = i.i_id_catalogo_nuevo
        WHERE e.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPRESARIAL'
        AND SUBSTRING_INDEX(f.nemonico, '-', -1) = 'XX'
        GROUP BY SUBSTRING_INDEX(f.nemonico, '-', 1), i.s_codigo_miembro, h.i_id_afinidad, f.valor;

        INSERT INTO v_t_bines_afinidades (s_num_bin, afinidad, i_id_afinidad, destinatarios)
        SELECT
            SUBSTRING_INDEX(b.nemonico, '-', 1) AS bin,
            SUBSTRING_INDEX(b.nemonico, '-', -1) AS afinidad,
            d.i_id_catalogo_nuevo,
            b.valor AS destinatarios
        FROM emi_dw_tablas a
        INNER JOIN emi_dw_catalogo b ON a.codigo_tabla = b.codigo_tabla
        INNER JOIN adq_tc_c_bin_propio c ON SUBSTRING_INDEX(b.nemonico, '-', 1) = c.bp_num_bin
        INNER JOIN v_t_catalogo d ON SUBSTRING_INDEX(b.nemonico, '-', -1) = d.s_codigo_miembro
        WHERE a.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPR_NOTIF'
        AND SUBSTRING_INDEX(b.nemonico, '-', -1) != 'XX'
        GROUP BY SUBSTRING_INDEX(b.nemonico, '-', 1), SUBSTRING_INDEX(b.nemonico, '-', -1), d.i_id_catalogo_nuevo, b.valor

        UNION ALL

        SELECT
            SUBSTRING_INDEX(f.nemonico, '-', 1) AS bin,
            i.s_codigo_miembro AS afinidad,
            h.i_id_afinidad,
            f.valor AS destinatarios
        FROM emi_dw_tablas e
        INNER JOIN emi_dw_catalogo f ON e.codigo_tabla = f.codigo_tabla
        INNER JOIN adq_tc_c_bin_propio g ON CAST(SUBSTRING_INDEX(f.nemonico, '-', 1) as signed ) = g.bp_num_bin
        INNER JOIN emi_cuenta_cliente h ON g.bp_num_bin = CAST(LEFT(h.s_num_bin,6) as SIGNED )
        INNER JOIN v_t_catalogo i ON h.i_id_afinidad = i.i_id_catalogo_nuevo
        WHERE e.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPR_NOTIF'
        AND SUBSTRING_INDEX(f.nemonico, '-', -1) = 'XX'
        GROUP BY SUBSTRING_INDEX(f.nemonico, '-', 1), i.s_codigo_miembro, h.i_id_afinidad, f.valor;
    END IF;

    SET v_rows = ROW_COUNT();
    IF v_rows > 0 THEN
        SET v_id = 1;
        WHILE v_id <= v_rows DO
            INSERT INTO v_t_destinatarios (id, destinatario)
            SELECT v_id, TRIM(BOTH ',' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(destinatarios, ',', help_topic_id), ',', -1))
            FROM v_t_bines_afinidades
            JOIN mysql.help_topic
            WHERE id = v_id;

            INSERT INTO v_t_bines_afin_toline (id, s_num_bin, afinidad, i_id_afinidad, toline)
            SELECT a.id, a.s_num_bin, a.afinidad, a.i_id_afinidad, TRIM(BOTH ' ' FROM b.destinatario)
            FROM v_t_bines_afinidades a
            INNER JOIN v_t_destinatarios b ON a.id = b.id;

            DELETE FROM v_t_destinatarios;
            SET v_id = v_id + 1;
        END WHILE;
    END IF;

    DROP TABLE IF EXISTS tmp_datos;
    CREATE TEMPORARY TABLE tmp_datos AS
    SELECT
        e.s_identifica AS Identificacion,
        CAST(b.i_num_cuenta AS CHAR(25)) AS NumeroCuenta,
        REPLACE(CONCAT(pf.p2_apelli1, ' ', pf.p2_apelli2, ' ', pf.p2_nombre), ',', '') AS NombreCliente,
        CAST(b.m_monto_autorizado AS CHAR(25)) AS CupoActual,
        CAST(b.m_monto_disponible AS CHAR(25)) AS CupoDisponible,
        CAST((h.m_sald_act_rot + h.m_sald_act_dif) AS CHAR(25)) AS SaldoTotal,
        CONCAT(bb.eb_cod_estado_hist, ' - ', bb.eb_descripcion_estado) AS EstadoTarjeta,
        CAST(g.i_dias_vcdo_total AS CHAR(10)) AS DiasVencido,
        CAST(h.m_deuda_vencida AS CHAR(25)) AS MontoVencido,
        CAST(b.s_num_bin AS CHAR(10)) AS Bin,
        CONCAT(f.s_codigo_banco, ' - ', f.s_descripcion_banco) AS Afinidad,
        e.i_cod_cliente AS ente,
        a.id AS id,
        b.s_num_bin,
        f.s_codigo_banco AS s_afinidad,
        CONCAT(
            e_secuencial,
            REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''),
            REPLACE(DATE_FORMAT(NOW(), '%H:%i:%s'), ':', ''),
            RIGHT(CONCAT('000', CAST(a.id AS CHAR(10))), 3)
        ) AS secuencial,
        CONCAT(
            e_fileadd,
            REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''),
            RIGHT(CONCAT('00000000', CAST(b.s_num_bin AS CHAR(10))), 8),
            RIGHT(CONCAT('000', CAST(f.s_codigo_banco AS CHAR(10))), 3),
            RIGHT(CONCAT('0000', CAST(a.id AS CHAR(10))), 4),
            '.csv'
        ) AS NombreArchivo
    FROM v_t_bines_afinidades a
    INNER JOIN emi_cuenta_cliente b ON a.s_num_bin = LEFT(b.s_num_bin,6) AND a.i_id_afinidad = CAST(b.i_id_afinidad as signed )
    INNER JOIN emi_t_medio_pago_tarjeta c ON b.i_num_cuenta = c.mp_cuenta AND c.mp_calpart = 'TI'
    INNER JOIN emi_t_dato_persona_fisica pf ON pf.p2_identcli = c.mp_identcli
    INNER JOIN emi_h_estado_procesadora ep ON LEFT(ep.ep_cod_procesadora, 2) = c.mp_codblq
    RIGHT JOIN emi_h_estado_ctatarj_bb bb ON ep.ep_cod_estado_bb = bb.eb_id_estado
    INNER JOIN emi_ods_cliente e ON b.i_cod_cliente = e.i_cod_cliente
    INNER JOIN adq_tc_m_catalogos f ON b.i_id_afinidad COLLATE utf8mb4_general_ci = f.s_codigo_banco COLLATE utf8mb4_general_ci
    INNER JOIN emi_deuda_rotativa g ON b.i_num_cuenta = g.i_num_cuenta
    INNER JOIN emi_maestro_cartera_diaria h ON b.i_num_cuenta = h.i_num_cuenta AND g.i_id_fec_proceso = h.i_id_fec_proceso
    INNER JOIN emi_ods_fecha i ON g.i_id_fec_proceso = i.i_id_fecha
    WHERE i.d_fecha = v_fechaProceso
    ORDER BY b.s_num_bin, f.s_codigo_banco, e.s_identifica;

    SET v_rows = 0;

    DROP TABLE IF EXISTS tmp_file_reader;


    CREATE TEMPORARY TABLE tmp_file_reader AS
    SELECT
        toline,
        descripcion,
        CONCAT(secuencial, RIGHT(CONCAT('0000', CAST(b._id AS CHAR(10))), 4)) AS secuencial,
        MIN(ente) AS ente,
        DATE_FORMAT(v_fechaProceso, '%Y/%m/%d') AS fechaProceso,
        a.NombreArchivo
    FROM tmp_datos a
    INNER JOIN v_t_bines_afin_toline b ON a.id = b.id
    INNER JOIN v_t_bines_afin_des c ON a.s_afinidad COLLATE utf8mb4_general_ci = c.afinidad COLLATE utf8mb4_general_ci AND a.s_num_bin COLLATE utf8mb4_general_ci = c.s_num_bin COLLATE utf8mb4_general_ci
    GROUP BY CONCAT(secuencial, RIGHT(CONCAT('0000', CAST(b._id AS CHAR(10))), 4)), descripcion, toline, NombreArchivo
    ORDER BY CONCAT(secuencial, RIGHT(CONCAT('0000', CAST(b._id AS CHAR(10))), 4)), descripcion, toline;

    SET v_rows = ROW_COUNT();

    INSERT INTO v_t_files_result (dato)
    SELECT CONCAT('1', e_filename, REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''), REPLACE(DATE_FORMAT(NOW(), '%H:%i:%s'), ':', ''), '.build') AS dato
    UNION ALL
    SELECT CONCAT('2BOLIVARIANO|', e_nemonico, '||inot2|Avisos24|', CAST(v_rows AS CHAR(10))) AS dato
    UNION ALL
    SELECT CONCAT('2', secuencial, '||', CAST(ente AS CHAR(10)), '|', 'email=', toline, '||', e_nemonico, '|', 'empresa_alianza=', descripcion, '##fecha_corte=', fechaProceso, '##file=', NombreArchivo, '||') AS dato
    FROM tmp_file_reader
    UNION ALL
    SELECT '2<EOF>' AS dato;

    INSERT INTO v_t_files_result (dato)
    SELECT CONCAT('3<File= ', e_filename, REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''), REPLACE(DATE_FORMAT(NOW(), '%H:%i:%s'), ':', ''), ' info=', e_nemonico, '|', CAST(v_rows AS CHAR(10)), '>') AS dato
    UNION ALL
    SELECT '3<content>' AS dato
    UNION ALL
    SELECT CONCAT('3<', secuencial, '|', LEFT(toline, 12), '|', NombreArchivo, '>') AS dato
    FROM tmp_file_reader
    UNION ALL
    SELECT '3</content>' AS dato
    UNION ALL
    SELECT '3</file>' AS dato;

    DROP TABLE IF EXISTS tmp_csv;
    CREATE TEMPORARY TABLE tmp_csv AS
    SELECT
        CONCAT(NumeroCuenta, ',', CupoActual, ',', CupoDisponible, ',', SaldoTotal, ',', EstadoTarjeta, ',', DiasVencido, ',', MontoVencido) AS dato,
        NombreArchivo
    FROM tmp_datos
    ORDER BY NombreArchivo, NumeroCuenta ASC;

    INSERT INTO v_t_filesname (nombre_archivo)
    SELECT DISTINCT NombreArchivo
    FROM tmp_csv;

    SET v_rows = ROW_COUNT();
    SET v_id = 1;
    WHILE v_id <= v_rows DO
        SELECT nombre_archivo INTO @v_filename
        FROM v_t_filesname
        WHERE id = v_id;

        INSERT INTO v_t_files_result (dato)
        SELECT CONCAT('1', @v_filename) AS dato
        UNION ALL
        SELECT '2NumeroCuenta,CupoActual,CupoDisponible,SaldoTotal,EstadoTarjeta,DiasVencido,MontoVencido' AS dato
        UNION ALL
        SELECT CONCAT('2', dato) AS dato
        FROM tmp_csv
        WHERE NombreArchivo = @v_filename;

        SET v_id = v_id + 1;
    END WHILE;

    SELECT dato
    FROM v_t_files_result
    ORDER BY id;
END //

DELIMITER ;


select * from adq_tc_m_catalogos
select * from adq_tc_c_bin_propio
select * from emi_cuenta_cliente

set @e_fechaProceso = '2025-07-23';
set @e_filename = 'TCRMC';
set @e_fileadd = 'MDPMC';
set @e_secuencial = 'TCR';
set @e_nemonico = 'TCRMC';
set @e_test_mode = true;
call pa_tcr_bnotiemptcemas(
        @e_fechaProceso,
        @e_filename,
        @e_fileadd,
        @e_secuencial,
        @e_nemonico,
        @e_test_mode
     );
