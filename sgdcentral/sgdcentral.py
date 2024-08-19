import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import dash_table
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
        conn = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        'TrustServerCertificate=yes;')
        print("Conexión exitosa.")
    except Exception as e:
        print(f"Error al conectar: {e}")
    return conn

def create_connection2():
    conn = None
    try:
        # Establecer conexión
        conn = pyodbc.connect(r'DRIVER={ODBC Driver 17 for SQL Server};'
        f'SERVER={server};'
        f'DATABASE={database2};'
        f'UID={username};'
        f'PWD={password};'
        'TrustServerCertificate=yes;')
        print("Conexión exitosa.")
    except Exception as e:
        print(f"Error al conectar: {e}")
    return conn

def fetch_data(par1,par2):
    conn = create_connection()
    if conn is None:
        raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
    try:
        query = f"""
SELECT
    CASE 
        WHEN LTRIM(RTRIM(p.RAZON_SOCIAL)) <> '' AND p.RAZON_SOCIAL IS NOT NULL THEN p.RAZON_SOCIAL
        WHEN LTRIM(RTRIM(CONCAT(p.NOMBRES, ' ', p.APELLIDOS))) <> '' AND CONCAT(p.NOMBRES, ' ', p.APELLIDOS) IS NOT NULL THEN CONCAT(p.NOMBRES, ' ', p.APELLIDOS)
        ELSE 'No disponible' -- Puedes poner un valor predeterminado si todos los campos son vacíos
    END AS NOMBRE,
    p.NRO_DOCUMENTO,
    CLATEMP.DESCRIPCION AS CLASE_ORIGEN,
    td.DESCRIPCION AS TIPO_DOC,
    DTEMP.INDICATIVO_OFICIO AS DOC_REGISTRADO,
    DTEMP.NUM_TRAM_DOCUMENTARIO AS HOJA_TRAMITE,
    CLA.DESCRIPCION AS CLASE,
    ed.DESCRIPCION AS ESTADO,
    D.ASUNTO,
    DTEMP.ASUNTO AS ASUNTO2,
    FORMAT(MV.AUDIT_MOD, 'dd/MM/yyyy HH:mm:ss') AS FEC_ENVIO,
    O.DEPENDENCIA AS ORIGEN,
    FORMAT(MV.AUDIT_REC, 'dd/MM/yyyy HH:mm:ss') AS FEC_ACEPTACION,
    DE.DEPENDENCIA AS DESTINO,
    tt.descripcion AS ACCION
FROM DB_TRAMITE_DOCUMENTARIO.web_tramite.MOVIMIENTO_DOCUMENTO MV
    LEFT OUTER JOIN DB_TRAMITE_DOCUMENTARIO.web_tramite.DOCUMENTO DTEMP
        ON DTEMP.ID_DOCUMENTO = MV.ID_DOCUMENTO
    LEFT OUTER JOIN DB_GENERAL.dbo.PERSONA p 
        ON DTEMP.ID_PERSONA = p.ID
    LEFT OUTER JOIN DB_TRAMITE_DOCUMENTARIO.web_tramite.DOCUMENTO D
        ON D.ID_DOCUMENTO = MV.ID_OFICIO
    LEFT JOIN DB_TRAMITE_DOCUMENTARIO.dbo.CLASE_DOCUMENTO_INTERNO CLATEMP
        ON DTEMP.ID_CLASE_DOCUMENTO_INTERNO = CLATEMP.ID_CLASE_DOCUMENTO_INTERNO
    LEFT JOIN DB_TRAMITE_DOCUMENTARIO.dbo.CLASE_DOCUMENTO_INTERNO CLA 
        ON D.ID_CLASE_DOCUMENTO_INTERNO = CLA.ID_CLASE_DOCUMENTO_INTERNO
    LEFT OUTER JOIN DB_TRAMITE_DOCUMENTARIO.web_tramite.doc_tipo_tratamiento dtt 
        ON D.ID_DOCUMENTO = dtt.id_documento
    LEFT OUTER JOIN DB_TRAMITE_DOCUMENTARIO.web_tramite.TIPO_TRATAMIENTO tt 
        ON dtt.id_tipo_tratamiento = tt.id_tipo_tratamiento
    LEFT OUTER JOIN DB_GENERAL.JCARDENAS.H_DEPENDENCIA O 
        ON MV.ID_DEPENDENCIA_ORIGEN = O.CODIGO_DEPENDENCIA
    LEFT OUTER JOIN DB_GENERAL.JCARDENAS.H_DEPENDENCIA DE 
        ON MV.ID_DEPENDENCIA_DESTINO = DE.CODIGO_DEPENDENCIA
    LEFT OUTER JOIN DB_TRAMITE_DOCUMENTARIO.dbo.TIPO_DOCUMENTO td
        ON DTEMP.ID_TIPO_DOCUMENTO = td.ID_TIPO_DOCUMENTO
    LEFT OUTER JOIN DB_TRAMITE_DOCUMENTARIO.dbo.ESTADO_DOCUMENTO ed
        ON DTEMP.ID_ESTADO_DOCUMENTO  = ed.ID_ESTADO_DOCUMENTO
WHERE DTEMP.NUM_TRAM_DOCUMENTARIO = '{par2}'
AND p.NRO_DOCUMENTO = '{par1}'
        """
        result = pd.read_sql(query, conn)

        result['DNI-EXP']=result['NRO_DOCUMENTO']+"-"+result['HOJA_TRAMITE']
        result['CLASE'] = result.apply(lambda row: row['CLASE_ORIGEN'] if pd.isna(row['CLASE']) or row['CLASE'] == '' else row['CLASE'], axis=1)
        result['ASUNTO']= result.apply(lambda row: row['ASUNTO2'] if pd.isna(row['ASUNTO']) or row['ASUNTO'] == '' else row['ASUNTO'], axis=1)
        result['ACCION'] = result['ACCION'].fillna('NO REGISTRA ACCIÓN')
        result['ACCION'] = result['ACCION'].replace('','NO REGISTRA ACCIÓN')
        result=result.rename(columns={'NOMBRE':'Razón Social',
       'HOJA_TRAMITE':'Hoja de trámite', 'CLASE': 'Clase de documento', 'ASUNTO':'Asunto', 'FEC_ENVIO':'Fecha de envío', 'ORIGEN':'Origen',
       'FEC_ACEPTACION':'Fecha de aceptación', 'DESTINO':'Destino', 'ACCION':'Acción'})
    finally:
        conn.close()  # Cerrar la conexión al final
    return result

columns=('Hoja de trámite', 'Clase de documento','Asunto','Fecha de envío','Origen', 'Fecha de aceptación','Destino','Acción')

layout = dbc.Container([

    dbc.Container(fluid=True, className="p-0", children=[
    dbc.Navbar(
        dbc.Container(fluid=True, className="d-flex justify-content-between align-items-center p-0", children=[
            # Logo SGD Redes
            dbc.Row([
                dbc.Col([
                    html.Img(src="/assets/sgd-central.png", alt="SGD", className="d-block d-lg-none", style={"width": "80px", "height": "auto"}),
                    html.Img(src="/assets/sgd-central.png", alt="SGD", className="d-none d-lg-block", style={"width": "180px", "height": "auto"})
                ], className="d-flex align-items-center"),
            ]),

            # Título Centrado
            dbc.Row([
                dbc.Col([
                    html.H2("Seguimiento del trámite", className="mb-0 d-none d-lg-block",style={"fontFamily": "Calibri" ,"color": '#0064AF'}),
                    html.H3("Seguimiento del trámite", className="mb-0 d-block d-lg-none", style={"fontFamily": "Calibri" ,"fontSize": "1.35rem", "color": '#0064AF'})
                ], className="text-center flex-grow-1"),
            ]),

            # Logo Essalud (Visible solo en pantallas grandes)
            dbc.Row([
                dbc.Col([
                    html.Img(src="/assets/Logotipo sin Slogan_Horizontal_Color Transparente.png", alt="Essalud", width="140")
                ], className="d-none d-lg-flex align-items-center justify-content-end"),
            ])
        ]),
        color="sgd",
        dark=True,
        className="navbar-expand-lg bg-sgd",
        style={"background": "white"},

    ),
    html.Div(style={"width": "100%", "height": "5px", "backgroundColor": "#0064AF"})
]),
    dbc.Row([
        dbc.Col([
            html.H6("# Documento (DNI/RUC) - # Hoja de tramite", style={'font-size': '14px', 'color': '#606060', 'fontWeight': 'normal', 'fontFamily': 'Calibri'}),
            dcc.Input(
                id='nu_expediente_input',
                type='text',
                placeholder='Número de Expediente',
                style={'width': '85%', 'fontSize': '14px'},
                className='mr-2',
                debounce=True  # Esto permite activar el callback al presionar Enter
            ),
            dbc.Button(
                html.I(className="fas fa-search"),
                id='search-button',
                style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white'},
                className='align-middle'
            )
        ], width=12, md=12, lg=5, className='mb-2'),
        
        dbc.Col([
            dbc.Button(
                [html.I(className="fas fa-file-excel"), html.Span(" Descargar datos")],
                id='download-button',
                style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white'},
                className='align-middle'
            ),
            dcc.Download(id="download-csv")  # Añadimos este componente para la descarga
        ], width=12, md=12, lg=2, className='mb-2 mt-4 text-center'),

    ], className='px-4 pt-3'),

    # Tarjetas para los valores de la primera fila
    dbc.Row([
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.P(id="razon-social-central", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                    html.P(id="min-fecha", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                    html.P(id="tipdoc", className="card-text", style={'font-size': '14px', 'color': '#606060'})
                ]),
                style={"margin-top": "10px", "padding": "0px", "border": "none"}
            ),

        ], width=8, md=6, lg=7,),


        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H6("Estado documento", id="estado_title", style={'font-size': '14px', 'color': 'white', 'fontWeight': 'normal', 'fontFamily': 'Calibri', "textAlign": 'center'}),
                    html.P(id="estado", className="card-text", style={'font-size': '18px', 'color': '#666666',"textAlign":'center'}),

                ]),
                style={"margin-top": "10px", "padding": "0px", "border": "none"}, className="offset-md-4"
            ),
        ], width=2, md=4, lg=4,)

    ], style={'margin': '0'}, className='px-4'),


    # Tarjeta del asunto
    dbc.Row([
        dbc.Col([
            html.Div(
                id="asunto_container",
                children=[
                    html.H6("Asunto", id="asunto_title", style={'font-size': '14px', 'color': 'white', 'fontWeight': 'normal', 'fontFamily': 'Calibri', 'textAlign': 'center'}),
                    dbc.Card(
                        dbc.CardBody([
                            html.P("Especificar DNI/RUC y hoja de trámite para ver detalle", id="asunto", className="card-text", style={'font-size': '16px', 'color': '#606060', 'fontFamily': 'Calibri'}),
                        ]),
                        style={"margin-top": "10px", "padding": "0px", "border": "none", "text-align": "center", 'font-weight': 'bold', 'background-color': '#F4FAFD', 'margin-bottom': '10px'}
                    )
                ]
            )
        ], width=12)
    ], style={'margin': '0'}, className='px-4'),

    # Tabla de resultados
    dbc.Row([
        dbc.Col([
            html.Div(id='table_container-central'),
        ], width=12)
    ], style={'margin-top': '20px'}, className='px-4 pt-0'),

], fluid=True, className='p-0 m-0')

def register_callbacks(app):
    @app.callback(
        [Output('table_container-central', 'children', allow_duplicate=True),
        Output('razon-social-central', 'children', allow_duplicate=True),
        Output('min-fecha', 'children', allow_duplicate=True),
        Output('tipdoc', 'children', allow_duplicate=True),
        Output('asunto', 'children', allow_duplicate=True),
        Output('asunto_title', 'style', allow_duplicate=True),
        Output('estado', 'children', allow_duplicate=True),
        Output('estado_title', 'style', allow_duplicate=True)],
        [Input('search-button', 'n_clicks'), Input('nu_expediente_input', 'n_submit')],
        [State('nu_expediente_input', 'value')],
        prevent_initial_call=True,
    )
    def update_table(n_clicks,n_submit, nu_expediente):
        if (n_clicks is None or n_clicks == 0) and (n_submit is None or n_submit == 0):
            return ["", "", "", "", "Especificar DNI/RUC y hoja de trámite para ver detalle", {'display': 'none'}, "", {'display': 'none'}]
        
        if nu_expediente:
            try:
                parts = nu_expediente.split('-', 1)
                if len(parts) != 2:
                    return ["Formato incorrecto. Use el formato 'DNI - Código - Número'", "", "", "", "Especificar DNI/RUC y hoja de trámite para ver detalle", {'color': 'white', 'display': 'block'}, "", {'display': 'none'}]
                
                par1 = parts[0].strip()
                rest = parts[1].strip()
                par2_parts = rest.split('-', 1)
                if len(par2_parts) != 2:
                    return ["Formato incorrecto en el segundo parámetro.", "", "", "", "Especificar DNI/RUC y hoja de trámite para ver detalle", {'color': 'white', 'display': 'block'}, "", {'display': 'none'}]
                
                par2 = f"{par2_parts[0].strip()}-{par2_parts[1].strip()}"

                print(f"Parámetros recibidos - par1: {par1}, par2: {par2}")
            except Exception as e:
                return [f"Error al procesar los parámetros: {e}", "", "", "", "Especificar DNI/RUC y hoja de trámite para ver detalle", {'color': 'white', 'display': 'block'}, "", {'display': 'none'}]

            try:
                data = fetch_data(par1, par2)
                print(f"Datos recibidos: {data}")
            except Exception as e:
                return [f"Error al buscar los datos: {e}", "", "", "", "Especificar DNI/RUC y hoja de trámite para ver detalle", {'color': 'white', 'display': 'block'}, "", {'display': 'none'}]

            if data is not None and not data.empty:
                last_5_data = data.tail(5)
                first_row = data.iloc[0]
                last_row = data.iloc[-1]
                razon_social = first_row['Razón Social']
                fecha_envio = first_row['Fecha de envío']
                clase_documento = first_row['Clase de documento']
                asunto = first_row['Asunto']
                estado = last_row['ESTADO']

                table = dash_table.DataTable(
                    id='table',
                    columns=[{"name": i, "id": i} for i in ['Hoja de trámite', 'Clase de documento', 'Asunto', 'Fecha de envío', 'Origen', 'Fecha de aceptación', 'Destino', 'Acción']],
                    data=last_5_data.to_dict('records'),
                    style_table={
                        'overflowX': 'auto',
                        'border': 'thin lightgrey solid',
                        'fontFamily': 'Calibri',
                        'fontSize': '12px',
                        'width': '100%',
                        'height': '100%'
                    },
                    style_cell={
                        'fontFamily': 'Calibri',
                        'height': 'auto',
                        'maxWidth': '120px',
                        'whiteSpace': 'normal',
                        'color': '#606060',
                        'fontSize': '14px',
                        'textAlign': 'left'
                    },
                    style_header={
                        'backgroundColor': '#0064AF',
                        'color': 'white',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                    fixed_rows={'headers': True},
                    style_cell_conditional=[
                        {'if': {'column_id': 'Hoja de trámite'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Clase de documento'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '200px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Asunto'}, 'minWidth': '230px', 'width': '230px', 'maxWidth': '250px'},
                        {'if': {'column_id': 'Fecha de envío'}, 'minWidth': '120px', 'width': '120px', 'maxWidth': '150px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Origen'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px'},
                        {'if': {'column_id': 'Fecha de aceptación'}, 'minWidth': '120px', 'width': '120px', 'maxWidth': '150px', 'textAlign': 'center'},
                        {'if': {'column_id': 'Destino'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px'},
                        {'if': {'column_id': 'Razón Social'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px'},
                        {'if': {'column_id': 'Acción'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px'},
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': 'rgb(244, 250, 253)',
                        }
                    ],
                )
                return table, f"Razón social: {razon_social}", f"Fecha de envío: {fecha_envio}", f"Tipo de documento {clase_documento}", f"{asunto}", {'color': '#0064AF', 'display': 'block', 'textAlign': 'center', 'fontFamily': 'Calibri'}, estado, {'display': 'block','color': '#0064AF', 'textAlign': 'center', 'fontFamily': 'Calibri'}  # Mostrar título "Asunto"
            else:
                return ["No se encontró información con los datos proporcionados. Intente nuevamente", "", "", "", "Especificar DNI/RUC y hoja de trámite para ver detalle", {'color': 'white', 'display': 'block'}, "", {'display': 'none'}]  # Mostrar título "Asunto" cuando no hay datos
        else:
            return ["", "", "", "", "Especificar DNI/RUC y hoja de trámite para ver detalle", {'display': 'none'}, "", {'display': 'none'}]  # Ocultar título "Asunto" cuando no hay datos

    @app.callback(
        Output('download-csv', 'data', allow_duplicate=True),
        [Input('download-button', 'n_clicks')],
        [State('nu_expediente_input', 'value')],
        prevent_initial_call=True
    )
    def download_table(n_clicks, nu_expediente):
        if n_clicks and nu_expediente:
            try:
                parts = nu_expediente.split('-', 1)
                par1 = parts[0].strip()
                rest = parts[1].strip()
                par2_parts = rest.split('-', 1)
                par2 = f"{par2_parts[0].strip()}-{par2_parts[1].strip()}"

                data = fetch_data(par1, par2)  # Llama a la función que obtiene los datos
                if data is not None and not data.empty: 
                    data=data[['Hoja de trámite', 'Clase de documento','Asunto','Fecha de envío','Origen', 'Fecha de aceptación','Destino','Acción']] 
                    return dcc.send_data_frame(data.to_csv, "seguimiento_tramite.csv", index=False, encoding='utf-8')
            except Exception as e:
                print(f"Error al procesar la descarga: {e}")

        return None
