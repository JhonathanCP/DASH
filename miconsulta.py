from flask import Flask, send_file
import pandas as pd
import io
from sqlalchemy import create_engine
from datetime import datetime

def create_csv_export_route(server):
    @server.route('/administrativo/miconsulta')
    def export_csv():
        # Crear la conexión a la base de datos
        engine = create_engine('postgresql://postgres:AKindOfMagic@10.0.1.228:5432/dw_essalud')
        engine1 = create_engine('postgresql://postgres:AKindOfMagic@10.0.1.228:5432/dl_essi')
        try:
            with engine1.connect() as conn1:
                # Ejecutar las consultas y cargar los DataFrames
                # NO CONSIDERAR POL. COMPL. CRECIENTE SAN NICOLAS
                query = """
                        SELECT DISTINCT(c1.cas_cod) FROM cas_pobafil c1 INNER JOIN CMCAS10 c2 on c1.cas_cod = c2.cenasicod  
                        WHERE c1.cuenta_registros > 0
                            AND	c2.ESTREGCOD = '1' 
                            AND c2.NIVCENTASISCOD IN ('1','2') 
                            AND c2.CENASISISFLG = '1' 
                            AND c2.ORICENASICOD IN ('1', '2')
                            AND c2.cenasicod <> '824'
                        """
                ipress = pd.read_sql(query, conn1)

            cas_cod_list = ipress['cas_cod'].tolist()

            with engine.connect() as conn:
                # Ejecutar las consultas y cargar los DataFrames
                query = "SELECT * FROM public.servicios_ipress_total WHERE cod_area = '01'"
                servicios_ipress_total = pd.read_sql(query, conn)
                
                query = "SELECT * FROM public.servicios_ipress"
                servicios_ipress_miconsulta = pd.read_sql(query, conn)

            # Filtrar ambos DataFrames para mantener sólo los registros cuyo 'cod_centro' está en 'cas_cod'
            servicios_ipress_total = servicios_ipress_total[servicios_ipress_total['cod_centro'].isin(cas_cod_list)]
            servicios_ipress_miconsulta = servicios_ipress_miconsulta[servicios_ipress_miconsulta['cod_centro'].isin(cas_cod_list)]

            # Comparar los DataFrames y añadir la columna 'habilitado_mi_consulta'
            columnas_comunes = list(servicios_ipress_total.columns)
            servicios_ipress_total['habilitado_mi_consulta'] = servicios_ipress_total.apply(
                lambda row: 'Si' if (servicios_ipress_miconsulta[columnas_comunes] == row[columnas_comunes]).all(1).any() else 'No',
                axis=1
            )

            # Eliminar la columna de origen (ajustar según la columna específica que desees eliminar)
            servicios_ipress_total.drop(columns=['cod_red_asistencial'], inplace=True, errors='ignore')
            servicios_ipress_total.drop(columns=['cod_centro'], inplace=True, errors='ignore')
            servicios_ipress_total.drop(columns=['cod_area'], inplace=True, errors='ignore')
            servicios_ipress_total.drop(columns=['nivel'], inplace=True, errors='ignore')
            servicios_ipress_total.drop(columns=['codservicio'], inplace=True, errors='ignore')
            servicios_ipress_total.drop(columns=['origen'], inplace=True, errors='ignore')
            # Renombrar la columna 'centro' a 'ipress'
            servicios_ipress_total.rename(columns={"centro": "ipress"}, inplace=True)

            # Agregar la columna 'fecha_corte' con el formato adecuado (YYYY-MM-DD HH:MM:SS)
            servicios_ipress_total['fecha_corte'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            servicios_ipress_total.columns = servicios_ipress_total.columns.str.upper()           

            # Convertir el DataFrame a un archivo CSV en memoria
            str_io = io.StringIO()
            servicios_ipress_total.to_csv(str_io, sep=';', index=False, encoding='utf-8-sig')
            str_io.seek(0)

            # Enviar el archivo CSV al cliente
            return send_file(
                io.BytesIO(str_io.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name='Avance_mi_consulta.csv'
            )
        finally:
            # Liberar memoria y cerrar la conexión a la base de datos
            del servicios_ipress_total, servicios_ipress_miconsulta
            engine.dispose()
            


