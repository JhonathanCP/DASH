import dash
from dash import dcc, html, Input, Output, State, dash_table
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
import numpy as np
from jupyter_dash import JupyterDash
import pdfkit
import base64
from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import pyodbc
from urllib.parse import quote_plus
import io
import oracledb
from datetime import datetime
from dash.exceptions import PreventUpdate
from concurrent.futures import ThreadPoolExecutor
import os

# Parámetros de conexión
server = '10.0.0.133'
database = 'DB_GENERAL'
database2= 'DB_TRAMITE_DOCUMENTARIO'  # Reemplaza con el nombre de tu base de datos
username = 'sa'
password = 'Essalud23**'
def create_connection():
    conn = None
    try:
        # Establecer conexión
        conn = pyodbc.connect(r'DRIVER={ODBC Driver 18 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        'TrustServerCertificate=yes;')
        print("Conexión exitosa.")
    except Exception as e:
        print(f"Error al conectar: {e}")
    return conn

var="GG"

def fetch_data(var):
    conn = create_connection()
    if conn is None:
        raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
    try:
        query = f"""
SELECT 
    d.hoja_tramite,
    d.tipo_hoja,
    cdi.DESCRIPCION,
    d.INDICATIVO_OFICIO,
    p.RAZON_SOCIAL,
    d.FECHA_RECEPCION,
    d.ASUNTO,
    md.AUDIT_REC,
    do.SIGLAS AS SIGLAS_ORIGEN,
    do.DEPENDENCIA AS DEPENDENCIA_ORIGEN,
    dd.SIGLAS AS SIGLAS_DESTINO,
    dd.DEPENDENCIA AS DEPENDENCIA_DESTINO,
    ht.APELLIDOS_TRABAJADOR,
    ht.NOMBRES_TRABAJADOR,
    ed.ID_ESTADO_DOCUMENTO,
    ed.DESCRIPCION AS ESTADO_DOCUMENTO,
    md.AUDIT_TRAB_DER AS FECHA_DELEGADO,
    md.derivado,
    md.finalizado,

    -- Columna DIFERENCIA_DIAS
    DATEDIFF(DAY, md.AUDIT_TRAB_DER, GETDATE()) AS DIFERENCIA_DIAS,

    -- Columna ESTADO
    CASE
        WHEN ed.ID_ESTADO_DOCUMENTO <> 2 
          AND ed.ID_ESTADO_DOCUMENTO <> 10 
          AND md.derivado = 0 
          AND md.finalizado = 0 THEN 'Pendiente'
        ELSE 'Otros'
    END AS ESTADO

FROM 
    DB_TRAMITE_DOCUMENTARIO.web_tramite.MOVIMIENTO_DOCUMENTO md
LEFT OUTER JOIN 
    DB_TRAMITE_DOCUMENTARIO.web_tramite.DOCUMENTO d ON md.ID_DOCUMENTO = d.ID_DOCUMENTO
LEFT OUTER JOIN 
    DB_GENERAL.dbo.PERSONA p ON d.ID_PERSONA = p.ID
LEFT OUTER JOIN 
    DB_TRAMITE_DOCUMENTARIO.dbo.CLASE_DOCUMENTO_INTERNO cdi ON d.ID_CLASE_DOCUMENTO_INTERNO = cdi.ID_CLASE_DOCUMENTO_INTERNO
LEFT OUTER JOIN 
    DB_TRAMITE_DOCUMENTARIO.dbo.ESTADO_DOCUMENTO ed ON ed.ID_ESTADO_DOCUMENTO = d.ID_ESTADO_DOCUMENTO
LEFT OUTER JOIN 
    DB_GENERAL.jcardenas.H_DEPENDENCIA do ON do.CODIGO_DEPENDENCIA = md.ID_DEPENDENCIA_ORIGEN 
LEFT OUTER JOIN 
    DB_GENERAL.jcardenas.H_DEPENDENCIA dd ON dd.CODIGO_DEPENDENCIA = md.ID_DEPENDENCIA_DESTINO
LEFT JOIN 
    DB_GENERAL.jcardenas.H_TRABAJADOR ht ON ht.CODIGO_TRABAJADOR = md.codigo_trabajador
WHERE
    (do.SIGLAS LIKE '{var}' OR dd.SIGLAS LIKE '{var}')
    AND
    md.AUDIT_TRAB_DER = (
        SELECT MAX(sub_md.AUDIT_TRAB_DER)
        FROM DB_TRAMITE_DOCUMENTARIO.web_tramite.MOVIMIENTO_DOCUMENTO sub_md
        LEFT OUTER JOIN DB_TRAMITE_DOCUMENTARIO.web_tramite.DOCUMENTO sub_d ON sub_md.ID_DOCUMENTO = sub_d.ID_DOCUMENTO
        WHERE sub_d.hoja_tramite = d.hoja_tramite
    )
        """
        result = pd.read_sql(query, conn)
        result["Hoja de trámite"]=result["hoja_tramite"]+"-"+result["tipo_hoja"]
        result = result[result["ESTADO"] == "Pendiente"]
        result["Trabajador"]= result["NOMBRES_TRABAJADOR"]+ ", "+result["APELLIDOS_TRABAJADOR"]
        result[['parte_izquierda', 'Destino']] = result['SIGLAS_DESTINO'].str.rsplit('-', n=1, expand=True)
        result['Destino_vf'] = np.where(
        result['Destino'].isna(),  # Si 'Destino' está vacío (equivalente a BLANK() en DAX)
        result['SIGLAS_DESTINO'] + " - " + result['DEPENDENCIA_DESTINO'],  # Si está vacío
        result['Destino'] + " - " + result['DEPENDENCIA_DESTINO'])  # Si no está vacío
        result['tip_hoja'] = result['tipo_hoja'].apply(lambda x: 'Interno' if x == 'I' else ('Externo' if x == 'E' else None))
        result['FECHA_DELEGADO'] = pd.to_datetime(result['FECHA_DELEGADO'])
        result['FECHA_DELEGADO'] = result['FECHA_DELEGADO'].dt.strftime('%Y-%m-%d %H:%M:%S')
        result['Estado días'] = np.select([result['DIFERENCIA_DIAS'] <= 7, (result['DIFERENCIA_DIAS'] > 7) & (result['DIFERENCIA_DIAS'] <= 15), result['DIFERENCIA_DIAS'] > 15],['🟢 1 a 7 días', '🟡 8 a 15 días', '🔴 Mayor a 15 días'])
        
        
        result[['parte_izquierda2', 'Origen']] = result['SIGLAS_ORIGEN'].str.rsplit('-', n=1, expand=True)
        result['Origen_vf'] = np.where(
        result['Origen'].isna(),  # Si 'Destino' está vacío (equivalente a BLANK() en DAX)
        result['SIGLAS_ORIGEN'] + " - " + result['DEPENDENCIA_ORIGEN'],  # Si está vacío
        result['Origen'] + " - " + result['DEPENDENCIA_ORIGEN'])  # Si no está vacío

    finally:
        conn.close()  # Cerrar la conexión al final
    return result

data=fetch_data(var)

tipdoc= data["tip_hoja"].unique()
options_razon_social=data["RAZON_SOCIAL"].dropna().unique()
options_estado_dias=data["Estado días"].unique()
options_dependencia_destino= data["Destino_vf"].unique()
options_dependencia_origen= data["Origen_vf"].unique()

def get_unique_options():
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            query = """SELECT DEPENDENCIA
                       FROM DB_GENERAL.jcardenas.H_DEPENDENCIA"""
            cursor.execute(query)
            # Retornar solo los valores de la columna 'DEPENDENCIA'
            options = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return options
        finally:
            connection.close()
    return []

# Llamada a la función para obtener los valores de la columna 'DEPENDENCIA'
options = get_unique_options()


external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css',  # Bootstrap CSS
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.1/css/all.min.css'  # Bootstrap Icons
]

# Crear la aplicación Dash
app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=external_stylesheets)

app.layout = dbc.Container([
    
    # Título
dbc.Row([
    # Título y logotipo en la misma fila
    dbc.Col([
        html.Div(style={'height': '12px'}),
        html.H1("Seguimiento de trámites pendientes", style={'color': '#0064AF', 'fontSize': '28px', 'textAlign': 'Left', 'font-weight': 'bold', 'fontFamily': 'Calibri'}),
        html.H2("Fuente: SGD Sede Central", style={'color': '#0064AF', 'fontSize': '12px'}),
    ], width=10),  # Ajusta el ancho según sea necesario
    dbc.Col([
        html.Img(src="/assets/Logotipo sin Slogan_Horizontal_Color Transparente.png", alt="Essalud", width="150"),
    ], width=2, className="d-none d-lg-flex align-items-center justify-content-end"),
]),
    html.Hr(style={'border': '2px solid #0064AF'}),
    dbc.Row([
        # Date Picker for Start Date
        dbc.Col([
            html.Label("Hoja de trámite", style={'font-size': '16px', 'color': '#0064AF'}),
            dcc.Input(id='HT',style={'font-size': '12px', 'height': '40px'}),
                ], width=9, md=3, lg=1, className='mr-4'),

        dbc.Col([
            html.Label("Tipo de documento", style={'font-size': '16px', 'color': '#0064AF'}),
            dcc.Dropdown(
                id='tipdoc',
                options=tipdoc,
                placeholder="Seleccione tipo",
                optionHeight=35,
                style={'height': '41px', 'width': '100%'}
        ),],width=9, md=3, lg=2),

        dbc.Col([
            html.Label("Razón social", style={'font-size': '16px', 'color': '#0064AF', 'margin-bottom':'10px'}),
            dcc.Dropdown(
                id='razsoc',
                options=options_razon_social,
                placeholder="Seleccione R.S.",
                optionHeight=60,
                style={'height': '41px', 'width': '100%', 'font-size': '16px'}
        ),],width=9, md=3, lg=3),

        dbc.Col([
            html.Label("Fecha delegado", style={'font-size': '16px', 'color': '#0064AF'}),
            dcc.DatePickerRange(
                id='date-picker-range',
                minimum_nights=0,
                start_date_placeholder_text="Fecha inicio",
                end_date_placeholder_text="Fecha fin",
                display_format='DD-MM-YYYY',
                style={'height': '100px','font-size': '13px' },),
                
            html.Div(id='output-container')],width=9, md=3, lg=2),
            dbc.Col([
            html.Label("Estado días", style={'font-size': '16px', 'color': '#0064AF'}),
            dcc.Dropdown(
                id='estdias',
                options=options_estado_dias,
                placeholder="Seleccione",
                optionHeight=60,
                style={'height': '41px', 'width': '80%', 'font-size': '16px'}
        ),],width=9, md=3, lg=2

        ),

    ], style={"height": "90px",'margin-top': '5px'}),

    dbc.Row([
            dbc.Col([
            html.Label("Dependencia destino", style={'font-size': '16px', 'color': '#0064AF'}),
            dcc.Dropdown(
                id='depdest',
                options=options_dependencia_destino,
                placeholder="Seleccione tipo",
                optionHeight=50,
                style={'height': '41px', 'width': '100%'}
        ),],width=9, md=3, lg=4),

            dbc.Col([
            html.Label("Dependencia origen", style={'font-size': '16px', 'color': '#0064AF'}),
            dcc.Dropdown(
                id='depori',
                options=options_dependencia_origen,
                placeholder="Seleccione tipo",
                optionHeight=50,
                style={'height': '41px', 'width': '100%'}
        ),],width=9, md=3, lg=4),

    ], style={"height": "90px",'margin-bottom': '5px'}),

        # Salida de datos con dbc.Spinner
        dbc.Spinner(
            id="loading-output1",
            size="md",
            color="primary",
            type="border",
            fullscreen=False,
            children=html.Div(id='output-data-table-tab1')
        ),
        dbc.Alert(id="error-alert1", is_open=False, dismissable=True, color="danger"),

], fluid=True)



@app.callback(
    Output('output-data-table-tab1', 'children'),
    Output('error-alert1', 'is_open'),
    Output('error-alert1', 'children'), 
    Input('HT', 'value'),
    Input('tipdoc', 'value'),
    Input('razsoc', 'value'),
    Input('estdias', 'value'),
    Input('depori', 'value'),
    Input('depdest', 'value'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_table(hoja_tramite, tipo_doc, razon_social,estado_dias,dependencia_ori,dependencia_dest,start_date, end_date):
    # Copiamos el DataFrame original
    df_filtered = data

    # Filtros
    if hoja_tramite:
        df_filtered = df_filtered[df_filtered['hoja_tramite'].str.contains(hoja_tramite, na=False, case=False)]    
    if tipo_doc:
        df_filtered = df_filtered[df_filtered['tip_hoja'] == tipo_doc]
    
    if razon_social:
        df_filtered = df_filtered[df_filtered['RAZON_SOCIAL'] == razon_social]
    if estado_dias:
        df_filtered = df_filtered[df_filtered['Estado días'] == estado_dias]
    if dependencia_ori:
        df_filtered = df_filtered[df_filtered['Origen_vf'] == dependencia_ori]
    if dependencia_dest:
        df_filtered = df_filtered[df_filtered['Destino_vf'] == dependencia_dest]

    if start_date and end_date:
        df_filtered = df_filtered[
            (df_filtered['FECHA_DELEGADO'] >= start_date) & 
            (df_filtered['FECHA_DELEGADO'] <= end_date)
        ]

    # Si no hay datos, muestra el alerta de error
    if df_filtered.empty:
        error_message = "No se encontraron registros"
        return html.Div(),True,error_message
    
    # Crea la tabla con el DataFrame filtrado
    table = dash_table.DataTable(
      id='data-table',
                    columns=[{"name": i, "id": i} for i in ["Hoja de trámite","DESCRIPCION","INDICATIVO_OFICIO","FECHA_DELEGADO","RAZON_SOCIAL","ASUNTO","DEPENDENCIA_ORIGEN","Destino_vf","Trabajador","DIFERENCIA_DIAS", "Estado días"]],
                    data=df_filtered.to_dict('records'),
                    style_table={'overflowX': 'auto', 'width': '100%',},
                    page_size=10,
                    style_cell={
                            'textAlign': 'left',
                            'fontFamily': 'Calibri',
                            'padding': '5px',
                            'height': 'auto',
                            'maxWidth': '120px',
                            'whiteSpace': 'normal',
                            'color': '#606060',
                            'fontSize': '14px'
                    },
                    style_header={
                        'backgroundColor': '#0064AF',
                        'fontWeight': 'bold',
                        'color': 'white'
                    },
                    style_cell_conditional=[
                        {'if': {'column_id': 'Hoja de trámite'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '200px', 'textAlign': 'center'},
                        {'if': {'column_id': 'DESCRIPCION'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '250px', 'textAlign': 'center'},
                        {'if': {'column_id': 'INDICATIVO_OFICIO'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'FECHA_DELEGADO'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'RAZON_SOCIAL'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'ASUNTO'}, 'minWidth': '200px', 'width': '200px', 'maxWidth': '250px', 'textAlign': 'center'},
                        {'if': {'column_id': 'DEPENDENCIA_ORIGEN'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Destino_vf'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Trabajador'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'DIFERENCIA_DIAS'}, 'minWidth': '60px', 'width': '60px', 'maxWidth': '65px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Estado días'}, 'minWidth': '60px', 'width': '60px', 'maxWidth': '65px', 'textAlign': 'center'}
                    ],
                    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(244, 250, 253)'}],
    )

    return table, False, ""


if __name__ == '__main__':
    app.run_server(debug=True)
