DELIMITER $$

CREATE PROCEDURE `pa_tc_iactionlogger`(
     e_lo_usuario     VARCHAR(50),
     e_lo_maquina     VARCHAR(60),
     e_lo_opcion      VARCHAR(100),
     e_lo_operacion   VARCHAR(100),
     e_lo_resultado   VARCHAR(15),
     e_lo_mensaje     TEXT
)
BEGIN
    DECLARE v_return INT DEFAULT 0;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
       SELECT CONCAT_WS(':' , 'Error al intentar ejecutar sentencia Insert ');
    END;

    INSERT INTO adq_m_logs (lo_usuario,lo_maquina,lo_opcion,lo_operacion,lo_resultado,lo_mensaje)
    VALUES (e_lo_usuario, e_lo_maquina, e_lo_opcion, e_lo_operacion, e_lo_resultado, e_lo_mensaje);
    SET v_return = 1;
    SELECT v_return;
END$$
DELIMITER ;