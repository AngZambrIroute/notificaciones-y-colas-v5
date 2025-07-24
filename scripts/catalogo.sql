DELIMITER $$

CREATE PROCEDURE t_insert_cat_nemonico_gestion()
BEGIN
    DECLARE v_new_id_catalogo INT;

    -- Obtener el valor máximo actual y sumarle 1
    SELECT IFNULL(MAX(i_id_catalogo), 0) + 1 INTO v_new_id_catalogo
    FROM adq_tc_m_catalogos;

    -- Insertar las gestiones
    INSERT INTO adq_tc_m_catalogos(i_empresa, d_fec_proceso, i_id_catalogo, s_nom_catalogo, s_tip_catalogo, s_codigo_banco, s_descripcion_banco, s_codigo_mps, s_descripcion_mps, s_codigo_mnet, s_descripcion_mnet, s_estado)
    VALUES
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'B', 'RETCC', 'Aviso de renovación TC entregada por el Courier', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'B', 'RETCO', 'Aviso de renovación TC con entrega en oficina', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'B', 'TCRMC', 'Reporte Tarjeta de crédito Marca cerrada', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'B', 'NPGTC', 'Notificacion Puntos Ganados Tarjeta de crédito', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'B', 'NAPTC', 'Notificación de agradecimiento de pagos tarjetas', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'DIFCO', 'Diferimiento de consumo y avance', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'DIFC2', 'Consumo diferido y avances tc adicional', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'CONIN', 'Consumo por internet', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'CONI2', 'Consumo por internet tc adicional', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'LIMCA', 'Límite de cupo alcanzado', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'TCACT', 'Aviso tarjeta crédito activada', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'TCBLQ', 'Bloqueo de tarjeta de crédito', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'BQTCT', 'Bloqueo TC temporal autogestión', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'DESTC', 'Desbloqueo TC temporal autogestión', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'ERRTC', 'Negación por bloqueo temporal de TC', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'TCPDD', 'Tarjeta de crédito precancelación diferidos', '', '', NULL, NULL, 'A'),
    (1, NOW(3), v_new_id_catalogo, 'NemonicoNotificacion', 'L', 'PAGTC', 'Pago tarjetas de crédito', '', '', NULL, NULL, 'A');
END $$

DELIMITER ;


INSERT INTO adq_tc_m_parametros (pa_parent, pa_nombre, pa_valor, pa_descripcion)
VALUES
  ('NotificacionesLinea', 'NotiEmpresa', 'Bolivariano', 'Nombre de la empresa para envio de notificacion');

INSERT INTO adq_tc_m_parametros (pa_parent, pa_nombre, pa_valor, pa_descripcion)
VALUES
  ('NotificacionesLinea', 'NotiRefMessageLabel', 'Avisos24', 'Nombre de la empresa para envio de notificacion')



create procedure pa_tcr_obtener_param_noti()
begin
  select pa_nombre, pa_valor, pa_descripcion
  from adq_tc_m_parametros
  where pa_parent = 'NotificacionesLinea';
end;