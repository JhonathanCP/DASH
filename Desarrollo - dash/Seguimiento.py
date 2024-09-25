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

# Variable global para almacenar el valor extraído de la URL
var = "CEABE"

def fetch_data(var):
    conn = create_connection()
    if conn is None:
        raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
    try:
        query = f"""
WITH MaxAudit AS (
    SELECT 
        sub_d.hoja_tramite,
        MAX(sub_md.AUDIT_TRAB_DER) AS Max_Audit_Trab_Der
    FROM 
        DB_TRAMITE_DOCUMENTARIO.web_tramite.MOVIMIENTO_DOCUMENTO sub_md
    LEFT OUTER JOIN 
        DB_TRAMITE_DOCUMENTARIO.web_tramite.DOCUMENTO sub_d ON sub_md.ID_DOCUMENTO = sub_d.ID_DOCUMENTO
    GROUP BY 
        sub_d.hoja_tramite
)
SELECT 
    d.hoja_tramite,
    d.tipo_hoja,
    cdi.DESCRIPCION,
    d.INDICATIVO_OFICIO,
    p.RAZON_SOCIAL,
    d.ASUNTO,
    do.DEPENDENCIA AS DEPENDENCIA_ORIGEN,
    dd.SIGLAS AS SIGLAS_DESTINO,
    dd.DEPENDENCIA AS DEPENDENCIA_DESTINO,
    ht.APELLIDOS_TRABAJADOR,
    ht.NOMBRES_TRABAJADOR,
    ed.DESCRIPCION AS ESTADO_DOCUMENTO,
    md.AUDIT_TRAB_DER AS FECHA_DELEGADO,
    md.derivado,
    md.finalizado,

    -- Columna DIFERENCIA_DIAS
    DATEDIFF(DAY, md.AUDIT_TRAB_DER, GETDATE()) AS DIFERENCIA_DIAS,

    -- Columna ESTADO
    CASE
        WHEN ed.ID_ESTADO_DOCUMENTO NOT IN (2, 10) 
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
JOIN 
    MaxAudit ma ON d.hoja_tramite = ma.hoja_tramite AND md.AUDIT_TRAB_DER = ma.Max_Audit_Trab_Der
WHERE
    (do.SIGLAS LIKE '{var}' OR dd.SIGLAS LIKE '{var}');
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
        result = result.sort_values(by='DIFERENCIA_DIAS', ascending=False)
    finally:
        conn.close()  # Cerrar la conexión al final
    return result

data=fetch_data(var)

tipdoc= data["tip_hoja"].unique()
options_razon_social=data["RAZON_SOCIAL"].dropna().unique()
options_razon_social


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
        html.Div([
            dcc.Location(id='url', refresh=False),
            html.Div(id='page-content', style={"margin": "0", "padding": "0"})
        ]),
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
                ], width=9, md=3, lg=1),

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
            html.Label("Razón social", style={'font-size': '16px', 'color': '#0064AF'}),
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
            dbc.Col(
            
        ),

    ], className="mb-4"),

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
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date')
)
def update_table(hoja_tramite, tipo_doc, razon_social, start_date, end_date):
    # Copiamos el DataFrame original
    df_filtered = data

    # Filtros
    if hoja_tramite:
        df_filtered = df_filtered[df_filtered['hoja_tramite'].str.contains(hoja_tramite, na=False, case=False)]    
    if tipo_doc:
        df_filtered = df_filtered[df_filtered['tip_hoja'] == tipo_doc]
    
    if razon_social:
        df_filtered = df_filtered[df_filtered['RAZON_SOCIAL'] == razon_social]
    
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
                    columns=[{"name": i, "id": i} for i in ["Hoja de trámite","DESCRIPCION","INDICATIVO_OFICIO","FECHA_DELEGADO","RAZON_SOCIAL","ASUNTO","DEPENDENCIA_ORIGEN","Destino_vf","Trabajador","DIFERENCIA_DIAS"]],
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
                        {'if': {'column_id': 'Hoja de trámite'}, 'minWidth': '70px', 'width': '70px', 'maxWidth': '80px', 'textAlign': 'center'},
                        {'if': {'column_id': 'DESCRIPCION'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100  px', 'textAlign': 'center'},
                        {'if': {'column_id': 'INDICATIVO_OFICIO'}, 'minWidth': '70px', 'width': '70px', 'maxWidth': '70px', 'textAlign': 'center'},
                        {'if': {'column_id': 'FECHA_DELEGADO'}, 'minWidth': '70px', 'width': '70px', 'maxWidth': '80px', 'textAlign': 'center'},
                        {'if': {'column_id': 'RAZON_SOCIAL'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'ASUNTO'}, 'minWidth': '200px', 'width': '200px', 'maxWidth': '250px', 'textAlign': 'center'},
                        {'if': {'column_id': 'DEPENDENCIA_ORIGEN'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Destino_vf'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Trabajador'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                        {'if': {'column_id': 'DIFERENCIA_DIAS'}, 'minWidth': '60px', 'width': '60px', 'maxWidth': '65px', 'textAlign': 'center'},
                    ],
                    style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(244, 250, 253)'}],
    )

    return table, False, ""

# Callback para extraer el parámetro de la URL
@app.callback(
    Output('output-param', 'children'),
    [Input('url', 'pathname')]
)
def extract_param_from_url(pathname):
    if pathname:
        # Extraer el parámetro después del "/"
        param = pathname.split('/')[-1]
        return f"El parámetro extraído de la URL es: {param}"
    return "No se encontró parámetro en la URL"


if __name__ == '__main__':
        app.run_server(debug=True)
