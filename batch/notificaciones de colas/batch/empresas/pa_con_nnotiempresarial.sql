DELIMITER $$

DROP PROCEDURE IF EXISTS pa_con_nnotiempresarial$$

CREATE PROCEDURE pa_con_nnotiempresarial(
    IN e_fechaProceso DATETIME,
    IN e_filename VARCHAR(50) DEFAULT 'TCRMC',
    IN e_fileadd VARCHAR(50) DEFAULT 'MDPMC',
    IN e_secuencial VARCHAR(3) DEFAULT 'TCR',
    IN e_nemonico VARCHAR(8) DEFAULT 'TCRMC'
)
BEGIN
    DECLARE v_rows INT DEFAULT 0;
    DECLARE v_id INT DEFAULT 0;
    DECLARE v_filename VARCHAR(50) DEFAULT '';
    DECLARE v_fechaProceso DATETIME;
    DECLARE done INT DEFAULT FALSE;
    
    -- Cursor para procesar registros
    DECLARE cur_bines CURSOR FOR 
        SELECT id FROM tmp_bines_afinidades ORDER BY id;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    SET v_fechaProceso = DATE_SUB(e_fechaProceso, INTERVAL 1 DAY);
    
    -- Crear tablas temporales
    DROP TEMPORARY TABLE IF EXISTS tmp_catalogo;
    CREATE TEMPORARY TABLE tmp_catalogo (
        _id INT AUTO_INCREMENT PRIMARY KEY,
        i_id_catalogo BIGINT,
        s_codigo_banco VARCHAR(25),
        s_descripcion_banco VARCHAR(100)
    );
    
    DROP TEMPORARY TABLE IF EXISTS tmp_bines_afinidades;
    CREATE TEMPORARY TABLE tmp_bines_afinidades (
        id INT AUTO_INCREMENT PRIMARY KEY,
        s_num_bin VARCHAR(10),
        afinidad VARCHAR(10),
        i_id_afinidad BIGINT,
        destinatarios VARCHAR(255)
    );
    
    DROP TEMPORARY TABLE IF EXISTS tmp_bines_afin_toline;
    CREATE TEMPORARY TABLE tmp_bines_afin_toline (
        _id INT AUTO_INCREMENT PRIMARY KEY,
        id INT,
        s_num_bin VARCHAR(10),
        afinidad VARCHAR(10),
        i_id_afinidad BIGINT,
        toline VARCHAR(255)
    );
    
    DROP TEMPORARY TABLE IF EXISTS tmp_bines_afin_des;
    CREATE TEMPORARY TABLE tmp_bines_afin_des (
        s_num_bin VARCHAR(10),
        afinidad VARCHAR(10),
        i_id_afinidad BIGINT,
        descripcion VARCHAR(255)
    );
    
    DROP TEMPORARY TABLE IF EXISTS tmp_destinatarios;
    CREATE TEMPORARY TABLE tmp_destinatarios (
        id INT,
        destinatario VARCHAR(255)
    );
    
    DROP TEMPORARY TABLE IF EXISTS tmp_filesname;
    CREATE TEMPORARY TABLE tmp_filesname (
        id INT AUTO_INCREMENT PRIMARY KEY,
        nombre_archivo VARCHAR(250)
    );
    
    DROP TEMPORARY TABLE IF EXISTS tmp_files_result;
    CREATE TEMPORARY TABLE tmp_files_result (
        id INT AUTO_INCREMENT PRIMARY KEY,
        dato VARCHAR(800)
    );
    
    -- Carga de catálogo de afinidades
    INSERT INTO tmp_catalogo (i_id_catalogo, s_codigo_banco, s_descripcion_banco)
    SELECT i_id, s_codigo_banco, s_descripcion_banco 
    FROM adq_tc_m_catalogos 
    WHERE s_nom_catalogo = 'Afinidades';
    
    -- Carga de bines y afinidades para descripción
    INSERT INTO tmp_bines_afin_des (s_num_bin, afinidad, i_id_afinidad, descripcion)
    SELECT 
        SUBSTRING(b.nemonico, 1, LOCATE('-', b.nemonico) - 1) AS bin,
        SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1) AS afinidad,
        d.i_id_catalogo_nuevo AS i_id_afinidad,
        b.valor AS descripcion
    FROM dw_tablas a
    INNER JOIN dw_catalogo b ON a.codigo_tabla = b.codigo_tabla
    INNER JOIN adq_tc_c_bin_propio c ON SUBSTRING(b.nemonico, 1, LOCATE('-', b.nemonico) - 1) = c.bp_num_bin
    INNER JOIN tmp_catalogo d ON SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1) = d.s_codigo_miembro
    WHERE a.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPRESARIAL'
        AND SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1) != 'XX'
    GROUP BY 
        SUBSTRING(b.nemonico, 1, LOCATE('-', b.nemonico) - 1),
        SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1),
        d.i_id_catalogo_nuevo,
        b.valor
    
    UNION ALL
    
    SELECT 
        SUBSTRING(f.nemonico, 1, LOCATE('-', f.nemonico) - 1) AS bin,
        i.s_codigo_miembro AS afinidad,
        h.i_id_afinidad,
        f.valor AS descripcion
    FROM db_tc_ods.dw_tablas e
    INNER JOIN db_tc_ods.dw_catalogo f ON e.codigo_tabla = f.codigo_tabla
    INNER JOIN adq_tc_c_bin_propio g ON SUBSTRING(f.nemonico, 1, LOCATE('-', f.nemonico) - 1) = g.bp_num_bin
    INNER JOIN emi_cuenta_cliente h ON g.bp_num_bin = h.s_num_bin
    INNER JOIN tmp_catalogo i ON h.i_id_afinidad = i.i_id_catalogo_nuevo
    WHERE e.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPRESARIAL'
        AND SUBSTRING(f.nemonico, LOCATE('-', f.nemonico) + 1) = 'XX'
    GROUP BY 
        SUBSTRING(f.nemonico, 1, LOCATE('-', f.nemonico) - 1),
        i.s_codigo_miembro,
        h.i_id_afinidad,
        f.valor;
    
    -- Carga de bines y afinidades principales
    INSERT INTO tmp_bines_afinidades (s_num_bin, afinidad, i_id_afinidad, destinatarios)
    SELECT 
        SUBSTRING(b.nemonico, 1, LOCATE('-', b.nemonico) - 1) AS bin,
        SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1) AS afinidad,
        d.i_id_catalogo_nuevo AS i_id_afinidad,
        b.valor AS destinatarios
    FROM db_tc_ods.dw_tablas a
    INNER JOIN db_tc_ods.dw_catalogo b ON a.codigo_tabla = b.codigo_tabla
    INNER JOIN adq_tc_c_bin_propio c ON SUBSTRING(b.nemonico, 1, LOCATE('-', b.nemonico) - 1) = c.bp_num_bin
    INNER JOIN tmp_catalogo d ON SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1) = d.s_codigo_miembro
    WHERE a.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPR_NOTIF'
        AND SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1) != 'XX'
    GROUP BY 
        SUBSTRING(b.nemonico, 1, LOCATE('-', b.nemonico) - 1),
        SUBSTRING(b.nemonico, LOCATE('-', b.nemonico) + 1),
        d.i_id_catalogo_nuevo,
        b.valor
    
    UNION ALL
    
    SELECT 
        SUBSTRING(f.nemonico, 1, LOCATE('-', f.nemonico) - 1) AS bin,
        i.s_codigo_miembro AS afinidad,
        h.i_id_afinidad,
        f.valor AS destinatarios
    FROM db_tc_ods.dw_tablas e
    INNER JOIN db_tc_ods.dw_catalogo f ON e.codigo_tabla = f.codigo_tabla
    INNER JOIN adq_tc_c_bin_propio g ON SUBSTRING(f.nemonico, 1, LOCATE('-', f.nemonico) - 1) = g.bp_num_bin
    INNER JOIN emi_cuenta_cliente h ON g.s_num_bin = h.s_num_bin
    INNER JOIN tmp_catalogo i ON h.i_id_afinidad = i.i_id_catalogo_nuevo
    WHERE e.titulo_tabla = 'MDP_BIN_AFINIDAD_EMPR_NOTIF'
        AND SUBSTRING(f.nemonico, LOCATE('-', f.nemonico) + 1) = 'XX'
    GROUP BY 
        SUBSTRING(f.nemonico, 1, LOCATE('-', f.nemonico) - 1),
        i.s_codigo_miembro,
        h.i_id_afinidad,
        f.valor;
    
    SELECT ROW_COUNT() INTO v_rows;
    
    IF v_rows > 0 THEN
        -- Procesar destinatarios usando cursor
        OPEN cur_bines;
        read_loop: LOOP
            FETCH cur_bines INTO v_id;
            IF done THEN
                LEAVE read_loop;
            END IF;
            
            -- Limpiar tabla temporal
            DELETE FROM tmp_destinatarios;
            
            -- Simular función SplitMultivaluedString usando SUBSTRING_INDEX
            SET @destinatarios = (SELECT destinatarios FROM tmp_bines_afinidades WHERE id = v_id);
            SET @pos = 1;
            SET @delim_pos = LOCATE(',', @destinatarios);
            
            WHILE @delim_pos > 0 DO
                INSERT INTO tmp_destinatarios (id, destinatario)
                VALUES (v_id, TRIM(SUBSTRING(@destinatarios, @pos, @delim_pos - @pos)));
                
                SET @pos = @delim_pos + 1;
                SET @destinatarios = SUBSTRING(@destinatarios, @delim_pos + 1);
                SET @delim_pos = LOCATE(',', @destinatarios);
            END WHILE;
            
            -- Insertar el último elemento
            IF LENGTH(TRIM(@destinatarios)) > 0 THEN
                INSERT INTO tmp_destinatarios (id, destinatario)
                VALUES (v_id, TRIM(@destinatarios));
            END IF;
            
            -- Insertar en tabla de líneas
            INSERT INTO tmp_bines_afin_toline (id, s_num_bin, afinidad, i_id_afinidad, toline)
            SELECT a.id, a.s_num_bin, a.afinidad, a.i_id_afinidad, TRIM(b.destinatario)
            FROM tmp_bines_afinidades a
            INNER JOIN tmp_destinatarios b ON a.id = b.id
            WHERE a.id = v_id;
            
        END LOOP;
        CLOSE cur_bines;
    END IF;
    
    -- Crear tabla temporal principal de datos
    DROP TEMPORARY TABLE IF EXISTS tmp_datos;
    CREATE TEMPORARY TABLE tmp_datos AS
    SELECT 
        e.s_identifica AS Identificacion,
        CAST(b.i_num_cuenta AS CHAR(25)) AS NumeroCuenta,
        REPLACE(CONCAT(COALESCE(e.s_apellidos, ''), ' ', COALESCE(e.s_nombres, '')), ',', '') AS NombreCliente,
        CAST(b.m_monto_autorizado AS CHAR(25)) AS CupoActual,
        CAST(b.m_disponible AS CHAR(25)) AS CupoDisponible,
        CAST((h.m_sald_act_rot + h.m_sald_act_dif) AS CHAR(25)) AS SaldoTotal,
        CONCAT(d.s_codigo_banco, ' - ', d.s_descripcion_banco) AS EstadoTarjeta,
        CAST(g.i_dias_vcdo_total AS CHAR(10)) AS DiasVencido,
        CAST(h.m_deuda_vcda AS CHAR(25)) AS MontoVencido,
        CAST(b.s_num_bin AS CHAR(10)) AS Bin,
        CONCAT(f.s_codigo_miembro, ' - ', f.s_nom_miembro) AS Afinidad,
        e.i_Nro_Cliente_Cobis AS ente,
        a.id,
        b.s_num_bin,
        f.s_codigo_miembro AS s_afinidad,
        CONCAT(e_secuencial, 
               REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''),
               REPLACE(DATE_FORMAT(NOW(), '%H:%i:%s'), ':', ''),
               LPAD(a.id, 3, '0')) AS secuencial,
        CONCAT(e_fileadd,
               REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''),
               LPAD(CAST(b.s_num_bin AS CHAR), 8, '0'),
               LPAD(CAST(f.s_codigo_miembro AS CHAR), 3, '0'),
               LPAD(CAST(a.id AS CHAR), 4, '0'),
               '.csv') AS NombreArchivo
    FROM tmp_bines_afinidades a
    INNER JOIN emi_cuenta_cliente b ON a.s_num_bin = b.s_num_bin AND a.i_id_afinidad = b.i_id_afinidad
    INNER JOIN emi_tarjeta_prop c ON b.i_num_cuenta = c.i_num_cuenta 
        AND c.s_tipo_tarjeta = 'P' 
        AND COALESCE(c.s_nuevo_num_tarj, '0') = '0'
    INNER JOIN adq_tc_m_catalogos d ON c.i_id_estado_tarj = d.s_codigo_banco
    INNER JOIN emi_ods_cliente e ON b.i_cod_cliente = e.i_cod_cliente
    INNER JOIN adq_tc_m_catalogos f ON b.i_id_afinidad = f.s_codigo_banco
    INNER JOIN emi_deuda_rotativa g ON b.i_num_cuenta = g.i_num_cuenta
    INNER JOIN emi_maestro_cartera_diaria h ON b.i_num_cuenta = h.i_num_cuenta 
        AND g.i_id_fec_proceso = h.i_id_fec_proceso
    INNER JOIN emi_ods_fecha i ON g.i_id_fec_proceso = i.i_id_fecha
    WHERE i.d_fecha = v_fechaProceso
    ORDER BY b.s_num_bin, f.s_codigo_banco, e.s_identifica;
    
    -- Crear tabla de archivos de lectura
    DROP TEMPORARY TABLE IF EXISTS tmp_file_reader;
    CREATE TEMPORARY TABLE tmp_file_reader AS
    SELECT 
        b.toline,
        c.descripcion,
        CONCAT(a.secuencial, LPAD(b._id, 4, '0')) AS secuencial,
        MIN(a.ente) AS ente,
        DATE_FORMAT(v_fechaProceso, '%Y/%m/%d') AS fechaProceso,
        a.NombreArchivo
    FROM tmp_datos a
    INNER JOIN tmp_bines_afin_toline b ON a.id = b.id
    INNER JOIN tmp_bines_afin_des c ON a.s_afinidad = c.afinidad AND a.s_num_bin = c.s_num_bin
    GROUP BY 
        CONCAT(a.secuencial, LPAD(b._id, 4, '0')),
        c.descripcion,
        b.toline,
        a.NombreArchivo
    ORDER BY 
        CONCAT(a.secuencial, LPAD(b._id, 4, '0')),
        c.descripcion,
        b.toline;
    
    SELECT COUNT(*) INTO v_rows FROM tmp_file_reader;
    
    -- Generar archivos de resultado
    INSERT INTO tmp_files_result (dato)
    SELECT CONCAT('1', e_filename, 
                  REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''),
                  REPLACE(DATE_FORMAT(NOW(), '%H:%i:%s'), ':', ''),
                  '.build') AS dato
    UNION ALL
    SELECT CONCAT('2BOLIVARIANO|', e_nemonico, '||inot2|Avisos24|', CAST(v_rows AS CHAR)) AS dato
    UNION ALL
    SELECT CONCAT('2', secuencial, '||', 
                  CAST(ente AS CHAR), '|',
                  'email=', toline, '||',
                  e_nemonico, '|',
                  'empresa_alianza=', descripcion, '##fecha_corte=', fechaProceso, '##file=', NombreArchivo, '||') AS dato
    FROM tmp_file_reader
    UNION ALL
    SELECT '2<EOF>' AS dato;
    
    INSERT INTO tmp_files_result (dato)
    SELECT CONCAT('3<File= ', e_filename, 
                  REPLACE(DATE_FORMAT(NOW(), '%Y/%m/%d'), '/', ''),
                  REPLACE(DATE_FORMAT(NOW(), '%H:%i:%s'), ':', ''),
                  ' info=', e_nemonico, '|', CAST(v_rows AS CHAR), '>') AS dato
    UNION ALL
    SELECT '3<content>' AS dato
    UNION ALL
    SELECT CONCAT('3<', secuencial, '|', LEFT(toline, 12), '|', NombreArchivo, '>') AS dato
    FROM tmp_file_reader
    UNION ALL
    SELECT '3</content>' AS dato
    UNION ALL
    SELECT '3</file>' AS dato;
    
    -- Crear datos CSV
    DROP TEMPORARY TABLE IF EXISTS tmp_csv;
    CREATE TEMPORARY TABLE tmp_csv AS
    SELECT 
        CONCAT(NumeroCuenta, ',',
               CupoActual, ',',
               CupoDisponible, ',',
               SaldoTotal, ',',
               EstadoTarjeta, ',',
               DiasVencido, ',',
               MontoVencido) AS dato,
        NombreArchivo
    FROM tmp_datos
    ORDER BY NombreArchivo, NumeroCuenta ASC;
    
    -- Procesar nombres de archivos
    INSERT INTO tmp_filesname (nombre_archivo)
    SELECT DISTINCT NombreArchivo FROM tmp_csv;
    
    SELECT COUNT(*) INTO v_rows FROM tmp_filesname;
    SET v_id = 1;
    
    WHILE v_id <= v_rows DO
        SELECT nombre_archivo INTO v_filename 
        FROM tmp_filesname 
        WHERE id = v_id;
        
        INSERT INTO tmp_files_result (dato)
        SELECT CONCAT('1', v_filename) AS dato
        UNION ALL
        SELECT '2NumeroCuenta,CupoActual,CupoDisponible,SaldoTotal,EstadoTarjeta,DiasVencido,MontoVencido' AS dato
        UNION ALL
        SELECT CONCAT('2', dato) AS dato
        FROM tmp_csv
        WHERE NombreArchivo = v_filename;
        
        SET v_id = v_id + 1;
        SET v_filename = '';
    END WHILE;
    
    -- Resultado final
    SELECT dato FROM tmp_files_result ORDER BY id ASC;
    
    -- Limpiar tablas temporales
    DROP TEMPORARY TABLE IF EXISTS tmp_catalogo;
    DROP TEMPORARY TABLE IF EXISTS tmp_bines_afinidades;
    DROP TEMPORARY TABLE IF EXISTS tmp_bines_afin_toline;
    DROP TEMPORARY TABLE IF EXISTS tmp_bines_afin_des;
    DROP TEMPORARY TABLE IF EXISTS tmp_destinatarios;
    DROP TEMPORARY TABLE IF EXISTS tmp_filesname;
    DROP TEMPORARY TABLE IF EXISTS tmp_files_result;
    DROP TEMPORARY TABLE IF EXISTS tmp_datos;
    DROP TEMPORARY TABLE IF EXISTS tmp_file_reader;
    DROP TEMPORARY TABLE IF EXISTS tmp_csv;
    
END$$

DELIMITER ;