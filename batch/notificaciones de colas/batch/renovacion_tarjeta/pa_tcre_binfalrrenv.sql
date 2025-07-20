
create procedure dbo.pa_tcre_binfalrrenv
as
begin
	set nocount on  
	declare
	@v_msg_error varchar(200),
	@v_es_error char


	select   c.codigo,   c.valor,   c.estado
	into #tmp_tipo_mov_renov_tc 
    from cobis..cl_tabla t
    inner join  cob 
is..cl_catalogo c on  c.tabla = t.codigo 
    where t.tabla = 'tcre_tipo_mov_renov_tc'
    and c.estado = 'V'

	if @@error != 0
	begin
		select @v_msg_error = 'Error seleccionando Tipos de movimientos inventario de plásticos que se notifican'
		got 
o manejo_errores
	end

	delete db_general..tcre_tmp_alr_renov_tc
	from db_general..tcre_tmp_alr_renov_tc a
	where not exists 
	(
		select 1
		from #tmp_tipo_mov_renov_tc m
		where m.codigo = a.ar_tipo_mov
	)

	if @@error != 0
	begin
		select 
 @v_msg_error = 'Error eliminando Tipos de movimientos  excluidas'
		goto manejo_errores
	end

	select   c.codigo,   c.valor,   c.estado
	into #tmp_motivo_renovacion_tc 
    from cobis..cl_tabla t
    inner join  cobis..cl_catalogo c on  c.tabla =  
t.codigo 
    where t.tabla = 'tcre_motivo_renovacion_tc'
    and c.estado = 'V'

	if @@error != 0
	begin
		select @v_msg_error = 'Error seleccionando razones de Generación de plásticos que se notifican'
		goto manejo_errores
	end

	delete db_ge 
neral..tcre_tmp_alr_renov_tc
	from db_general..tcre_tmp_alr_renov_tc a
	where not exists 
	(
		select 1
		from #tmp_motivo_renovacion_tc m
		where m.codigo = a.ar_codigo_renov
	)

	if @@error != 0
	begin
		select @v_msg_error = 'Error eliminand 
o razones de generación de plasticos excluidas'
		goto manejo_errores
	end

	UPDATE db_general..tcre_tmp_alr_renov_tc
		SET    ar_motivo_renov = m.valor
	FROM   db_general..tcre_tmp_alr_renov_tc a
	INNER JOIN #tmp_motivo_renovacion_tc m ON  m.codig 
o = a.ar_codigo_renov

	if @@error != 0
	begin
		select @v_msg_error = 'Error al actualizar la Razón de Generación de plásticos'
		goto manejo_errores
	end

	select   c.codigo,   c.valor,   c.estado,	t.tabla
	into #tmp_tipo_bodega_tc
    from co 
bis..cl_tabla t
    inner join  cobis..cl_catalogo c on  c.tabla = t.codigo 
    where t.tabla in ('tcre_bodega_ofc_renov_tc', 'tcre_bodega_cour_renov_tc')
    and c.estado = 'V'

	if @@error != 0
	begin
		select @v_msg_error = 'Error seleccionando 
 tipos de bodegas'
		goto manejo_errores
	end

	delete db_general..tcre_tmp_alr_renov_tc
	from db_general..tcre_tmp_alr_renov_tc a
	where not exists 
	(
		select 1	 
			from #tmp_tipo_bodega_tc m
		where m.codigo = a.ar_cod_bodega
	)

	if @@e 
rror != 0
	begin
		select @v_msg_error = 'Error eliminando registros de bogdegas excluidas'
		goto manejo_errores
	end

	UPDATE db_general..tcre_tmp_alr_renov_tc
		SET    ar_tipo_alerta = case when (t.tabla = 'tcre_bodega_ofc_renov_tc') then 'RETCO 
' 
									 when (t.tabla = 'tcre_bodega_cour_renov_tc') then 'RETCC' end,
				ar_bodega = t.valor 
	FROM   db_general..tcre_tmp_alr_renov_tc a
	INNER JOIN #tmp_tipo_bodega_tc t ON  t.codigo = a.ar_cod_bodega

	if @@error != 0
	begin
		select @v_ 
msg_error = 'Error al actualizar las bodegas'
		goto manejo_errores
	end
	declare
		@v_cliente				int,
		@v_id_cuenta			int, 
    	@v_nro_tarjeta 			varchar(20),
		@v_tipo_tarjeta			varchar(50),
		@v_identificacion 		varchar(20),
		@v_tipo_identi 
fica 		varchar(2),
		@v_nombre_cliente		varchar(50), 
		@v_telefono				varchar(20),
		@v_mail					varchar(50) 
 
	declare tcr_cur_aviso_renov_tc cursor
	for
	select
	ar_id_cuenta, ar_nro_tarjeta, ar_tipo_tarjeta
	from db_general..tcre_tmp_alr_ren 
ov_tc
	for update of ar_motivo_renov, ar_nro_tarjeta, ar_tipo_tarjeta,ar_id_cliente, ar_identificacion, ar_nombre_cliente, ar_telefono, ar_mail, ar_tipo_identifica

	open tcr_cur_aviso_renov_tc

	select @v_es_error = 'N'

	fetch tcr_cur_aviso_renov 
_tc
	into
		@v_id_cuenta, @v_nro_tarjeta, @v_tipo_tarjeta

	while (@@fetch_status = 0) and not (@v_es_error = 'S')
	begin
		select @v_telefono = null, @v_mail = null,  @v_tipo_tarjeta = null, @v_cliente = 0

		select top 1
			@v_tipo_tarjeta = m. 
ta_des_tiptar
		from db_tarjeta_bb..tc_tarjetas_credito m
		where ta_clave_unica   = @v_nro_tarjeta

		select top 1 
			@v_identificacion	= ma_co_clie,
			@v_tipo_identifica	= ma_ti_iden
		from db_tarjeta_bb..tc_maestro_bb m
		inner join db_tarjet 
a_bb..tc_bin b on b.tc_nbin = m.ma_co_binn
		where ma_c_nta  = @v_id_cuenta

		if @v_tipo_identifica ='C'
			begin
				exec @v_cliente  = cobis..pa_tcre_cente_cedruc  @v_identificacion, @v_cliente output
			end
		if @v_tipo_identifica ='P'
			begi 
n
				exec @v_cliente  = cobis..pa_tcre_cente_pasp  @v_identificacion, @v_cliente output
			end

		if @v_cliente is null
			begin
				fetch tcr_cur_aviso_renov_tc into
				@v_id_cuenta, @v_nro_tarjeta, @v_tipo_tarjeta
			end

		select top 1
--< 
ref 3
			@v_nombre_cliente = en_nombre + ' ' + p_p_apellido + ' ' + p_s_apellido
--ref 3>
		from cobis..cl_ente
		where en_ente  = @v_cliente

		select top 1
			@v_telefono = de_descripcion
		from cobis..cl_direccion_email
		where de_ente = @v_cl 
iente
			and de_tipo = 'M'
		order by de_fecha_modificacion desc

		select top 1
			@v_mail = de_descripcion
		from cobis..cl_direccion_email
		where de_ente = @v_cliente
			and de_tipo = 'E'
		order by de_fecha_modificacion desc

		update db_g 
eneral..tcre_tmp_alr_renov_tc
		set ar_id_cliente = @v_cliente,
			ar_nro_tarjeta = isnull(RIGHT(@v_nro_tarjeta, 3), ''),
			ar_tipo_tarjeta =  isnull(@v_tipo_tarjeta,''),
			ar_telefono = @v_telefono,
			ar_mail = @v_mail,
			ar_nombre_cliente = is 
null(@v_nombre_cliente,''),
			ar_tipo_identifica = isnull(@v_tipo_identifica, ''),
			ar_identificacion = left(@v_identificacion, 2) + '******' + RIGHT(@v_identificacion, 2)
		where current of tcr_cur_aviso_renov_tc

		if @@error != 0
		begin
			s 
elect
				@v_es_error = 'S',
				@v_msg_error = 'Error actualizando datos complementados del medio de envio'
			goto cerrar_cursor
		end

		fetch tcr_cur_aviso_renov_tc
		into
			@v_id_cuenta, @v_nro_tarjeta, @v_tipo_tarjeta
	end


cerrar_curs 
or:
	close tcr_cur_aviso_renov_tc
	deallocate tcr_cur_aviso_renov_tc

	delete db_general..tcre_tmp_alr_renov_tc 
	where ar_id_cliente <= 0 or ar_nro_tarjeta is null or  (ar_telefono is null and ar_mail is null) 
	
	if @@error != 0
	begin
		select 

			@v_es_error = 'S',
			@v_msg_error = 'Error al eliminar los registros de la tcre_tmp_alr_renov_tc incompletos'
		goto cerrar_cursor
	end

	if @v_es_error = 'S'
	goto manejo_errores

	return 0

manejo_errores:
	print @v_msg_error
	return 1 



end