ALTER procedure [dbo].[pa_tcr_balr_renov_tc] (  
 @e_fecha_proceso datetime  
)  
as  
begin  
    set nocount on  

    /*-----------------------------------------------------------------------------------------*/  
    declare  
    @v_fecha_dia datetime, 
    @v_cod_fecha int,    
    @v_msg_error varchar(127)  

    select  
    @v_fecha_dia = dateadd(dd, datediff(dd, 0, coalesce(@e_fecha_proceso, getdate())), 0)  
    /*-----------------------------------------------------------------------------------------*/  

    select @v_cod_fecha = i_id_fecha   
    from emi_ods_fecha   
    where d_fecha = @e_fecha_proceso 
    if @@ROWCOUNT = 0  
    begin  
        set @v_cod_fecha = 0  
    end  

    select 
        m.i_id_tarjeta,
        m.i_num_cuenta,
        m.i_cod_bod_act,
        m.i_cod_bod_ant,
        m.i_id_fec_proc,
        m.i_id_fec_mov,
        m.d_fec_mov, 
        m.s_motivo,
        m.s_mv_tipo  
    into #tmp_t_mov_inventario
    from ods_mov_inventario  m  
    where m.i_id_fec_mov =  @v_cod_fecha

    if @@error != 0  
    begin  
        select @v_msg_error = 'Error al consultar los movimientos del inventario'  
        goto manejo_errores  
    end 

    
    truncate emi_tcre_tmp_alr_renovtc

    select 
        t_tipo_alerta = convert(varchar(5), ''),
        t_id_cuenta = tm.i_num_cuenta,
        t_id_tarjeta = tm.i_id_tarjeta,
        t_tipo_tarjeta = convert(varchar(50), ''),
        t_nro_tarjeta = p.s_tarj_unica,
        t_id_cliente = 0,
        t_nombre_cliente = convert(varchar(50), ''),
        t_identificacion = convert(varchar(20), ''),
        t_tipo_identifica = convert(char(2), ''),
        t_tipo_mov      = tm.s_motivo,
        t_codigo_renov = c.s_codigo_miembro,
        t_motivo_renov = convert(varchar(64), ''),
        t_cod_bodega = b.s_cod_bod_credimatic,
        t_bodega = b.s_bodega,
        t_id_fec_proc = tm.i_id_fec_proc,
        t_fecha_envio = @v_fecha_dia,
        t_telefono = convert(varchar(20), ''),
        t_mail  = convert(varchar(40), '')       
    into emi_tcre_tmp_alr_renovtc
    from #tmp_t_mov_inventario  tm  
    CROSS APPLY 
    ( 
        SELECT  top 1 i_rzn_gen_pla, s_tarj_unica FROM emi_ods_plastico pa
        WHERE   pa.i_id_tarjeta = tm.i_id_tarjeta
        order by i_id_fec_impresion desc
    ) p 
    inner join  adq_tc_m_catalogos   c on c.i_id_catalogo_nuevo = p.i_rzn_gen_pla  where s_nom_catalogo='RazonGenPlasCla'
    inner join  adq_tc_m_catalogos  b on b.s_codigo_banco  = tm.i_cod_bod_ant where s_nom_catalogo="BODEGA_TDC"

    if @@error != 0  
    begin  
        select @v_msg_error = 'Error al generar los registros para el envio de notificaciones'  
        goto manejo_errores  
    end 


    select 
        s_bin_membresia, 
        i_cod_pais, 
        s_descripcion 
    into #tmp_t_bin_membresia
    from ods_bin_propio
    where i_cod_pais = 125
    and s_bin_membresia != ''

    if @@error != 0  
    begin  
        select @v_msg_error = 'Error al consultar los bines de Membresía'  
        goto manejo_errores  
    end 

    UPDATE t
        SET t.t_tipo_tarjeta = isnull(bin.s_descripcion, '')
    FROM emi_tcre_tmp_alr_renovtc t
    LEFT JOIN #tmp_t_bin_membresia bin ON bin.s_bin_membresia = right(t.t_nro_tarjeta, 6) 

    if @@error != 0  
    begin  
        select @v_msg_error = 'Error al actualizar las tarjetas de Membresía'  
        goto manejo_errores  
    end 

    manejo_errores:    
        print @v_msg_error  
        set nocount off  
        return 1  

end  