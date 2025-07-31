INSERT INTO adq_tc_m_parametros (pa_parent, pa_nombre, pa_valor, pa_descripcion)
VALUES
  ('NotificacionesBatch', 'TipoFacPagos', '67', 'tipo de factura para pagos');

INSERT INTO adq_tc_m_parametros (pa_parent, pa_nombre, pa_valor, pa_descripcion)
VALUES
  ('NotificacionesBatch', 'BinExclNotiAgradecPag', '422249', 'Visa prepago Bankard');



INSERT INTO adq_tc_m_catalogos(i_empresa, d_fec_proceso, i_id_catalogo, s_nom_catalogo, s_tip_catalogo, s_codigo_banco, s_descripcion_banco, s_codigo_mps, s_descripcion_mps, s_codigo_mnet, s_descripcion_mnet, s_estado)
    VALUES
    (1, NOW(3), 5, 'TipoFactura', 'B', '67', 'tipo de factura de pagos de tarjeta', '', '', NULL, NULL, 'A'),
    (1, NOW(3), 5, 'TipoFactura', 'B', '134', 'consumos con tarjeta de credito', '', '', NULL, NULL, 'A');