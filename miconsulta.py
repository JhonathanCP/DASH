from flask import Flask, send_file
import pandas as pd
import io
from sqlalchemy import create_engine


def create_csv_export_route(server):
    @server.route('/administrativo/miconsulta')
    def export_csv():
        # Crear la conexión a la base de datos
        engine = create_engine('postgresql://postgres:AKindOfMagic@10.0.1.228:5432/dw_essalud')
        try:
            with engine.connect() as conn:
                # Ejecutar las consultas y cargar los DataFrames
                query = "SELECT * FROM public.servicios_ipress_total WHERE cod_area = '01'"
                servicios_ipress_total = pd.read_sql(query, conn)
                
                query = "SELECT * FROM public.servicios_ipress"
                servicios_ipress_miconsulta = pd.read_sql(query, conn)

            # Comparar los DataFrames y añadir la columna 'habilitado_mi_consulta'
            columnas_comunes = list(servicios_ipress_total.columns)
            servicios_ipress_total['habilitado_mi_consulta'] = servicios_ipress_total.apply(
                lambda row: 'Sí' if (servicios_ipress_miconsulta[columnas_comunes] == row[columnas_comunes]).all(1).any() else 'No',
                axis=1
            )

            # Eliminar la columna de origen (ajustar según la columna específica que desees eliminar)
            servicios_ipress_total.drop(columns=['origen'], inplace=True, errors='ignore')

            # Convertir el DataFrame a un archivo CSV en memoria
            str_io = io.StringIO()
            servicios_ipress_total.to_csv(str_io, sep=';', index=False)
            str_io.seek(0)

            # Enviar el archivo CSV al cliente
            return send_file(
                io.BytesIO(str_io.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name='reporte.csv'
            )
        finally:
            # Liberar memoria y cerrar la conexión a la base de datos
            del servicios_ipress_total, servicios_ipress_miconsulta
            engine.dispose()
