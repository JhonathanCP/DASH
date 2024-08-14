from sqlalchemy import create_engine, text

engine = create_engine('postgresql://postgres:SdDd3v@10.0.0.131:5433/sgd')

def get_data(co_red, nu_expediente):
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    CONCAT(tdtx_ani_simil.denom, ' ', tdtx_ani_simil.deapp, ' ', tdtx_ani_simil.deapm) as NOM_OP_1,
                    CONCAT(tdtr_otro_origen.de_nom_otr, ' ', tdtr_otro_origen.de_ape_pat_otr, ' ', tdtr_otro_origen.de_ape_mat_otr) as NOM_OP_2,    
                    lg_pro_proveedor.cpro_razsoc as NOM_OP_3,    
                    tdtc_expediente.nu_expediente as NRO_EXPEDIENTE,
                    si_mae_tipo_doc.cdoc_desdoc as CLASE_DOCUMENTO,
                    tdtv_remitos.de_asu as ASUNTO,
                    tdtv_remitos.fe_emi as FECHA_ENVIO,
                    rhtm_dependencia_or.de_dependencia as ORIGEN,
                    tdtv_destinos.fe_rec_doc as FECHA_ACEPTACION,
                    rhtm_dependencia_dest.de_dependencia as DESTINO,
                    si_redes.des_red                   
                FROM 
                    idosgd.tdtv_destinos 
                LEFT JOIN idosgd.rhtm_dependencia rhtm_dependencia_dest 
                    ON tdtv_destinos.co_dep_des = rhtm_dependencia_dest.co_dependencia
                LEFT JOIN idosgd.si_redes 
                    ON rhtm_dependencia_dest.co_red = si_redes.co_red
                LEFT JOIN idosgd.tdtv_remitos 
                    ON CONCAT(tdtv_destinos.nu_ann, tdtv_destinos.nu_emi) = CONCAT(tdtv_remitos.nu_ann, tdtv_remitos.nu_emi)
                LEFT JOIN idosgd.tdtc_expediente 
                    ON CONCAT(tdtv_remitos.nu_ann, tdtv_remitos.nu_sec_exp) = CONCAT(tdtc_expediente.nu_ann_exp, tdtc_expediente.nu_sec_exp)
                LEFT JOIN idosgd.tdtx_ani_simil 
                    ON tdtv_remitos.nu_dni_emi = tdtx_ani_simil.nulem
                LEFT JOIN idosgd.tdtr_otro_origen 
                    ON tdtv_remitos.co_otr_ori_emi = tdtr_otro_origen.co_otr_ori
                LEFT JOIN idosgd.lg_pro_proveedor 
                    ON tdtv_remitos.nu_ruc_emi = lg_pro_proveedor.cpro_ruc
                LEFT JOIN idosgd.si_mae_tipo_doc 
                    ON tdtv_remitos.co_tip_doc_adm = si_mae_tipo_doc.cdoc_tipdoc
                LEFT JOIN idosgd.tdtr_grupo_documento 
                    ON tdtc_expediente.co_gru = tdtr_grupo_documento.co_gru
                LEFT JOIN idosgd.rhtm_dependencia rhtm_dependencia_or 
                    ON tdtv_remitos.co_dep_emi = rhtm_dependencia_or.co_dependencia
                WHERE 
                    tdtc_expediente.co_gru = '3'
                    AND si_redes.co_red = :co_red
                    AND tdtc_expediente.nu_expediente = :nu_expediente
                ORDER BY FECHA_ENVIO ASC
            """)
            result = conn.execute(query, {'co_red': co_red, 'nu_expediente': nu_expediente})
            data = [dict(row) for row in result]
            return data
    except Exception as e:
        return {'error': str(e)}