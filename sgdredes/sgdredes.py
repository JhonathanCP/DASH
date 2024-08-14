import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
import sys
import plotly.graph_objects as go
import json
import dash_bootstrap_components as dbc
from dash import dash_table
import numpy as np

# Función para crear una conexión a la base de datos
def create_connection():
    try:
        engine = create_engine('postgresql://postgres:SdDd3v@10.0.0.131:5433/sgd')
        with engine.connect() as conn:
            print("Connection to the database was successful!")
        return engine
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

# Función para obtener las opciones del desplegable
def get_red_options():
    engine = create_connection()
    if engine is not None:
        try:
            with engine.connect() as conn:
                query = "SELECT des_red FROM idosgd.si_redes"
                df = pd.read_sql(query, conn)
                options = [{'label': row['des_red'], 'value': row['des_red']} for _, row in df.iterrows()]
                return options
        except Exception as e:
            print(f"Failed to fetch red options: {e}")
            return []
    else:
        return []

# Función para obtener los datos según el filtro
def fetch_data(des_red, nu_expediente):
    engine = create_connection()
    if engine is not None:
        try:
            with engine.connect() as conn:
                query = f"""
                SELECT 
                    CONCAT(
                        COALESCE(CONCAT(tdtx_ani_simil.denom, ' ', tdtx_ani_simil.deapp, ' ', tdtx_ani_simil.deapm), ''),
                        ' ',
                        COALESCE(CONCAT(tdtr_otro_origen.de_nom_otr, ' ', tdtr_otro_origen.de_ape_pat_otr, ' ', tdtr_otro_origen.de_ape_mat_otr), ''),
                        ' ',
                        COALESCE(lg_pro_proveedor.cpro_razsoc, '')
                    ) as NOMBRE_COMPLETO,
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
                    AND si_redes.des_red = '{des_red}'
                    AND tdtc_expediente.nu_expediente = '{nu_expediente}'                
                ORDER BY 
                    FECHA_ENVIO ASC
                """
                print(f"Executing query: {query}")  # Debugging line
                result = pd.read_sql(query, conn)
                # Renombrar columnas
                result.columns = [
                    'Razón Social', 
                    'N° Expediente', 
                    'Clase de documento', 
                    'Asunto', 
                    'Fecha de envío', 
                    'Origen', 
                    'Fecha de aceptación', 
                    'Destino', 
                    'Red'
                ]
                print(f"Query result: {result}")  # Debugging line
                return result
        except Exception as e:
            print(f"Failed to fetch data: {e}")
            return None
    else:
        return None
    

# Obtener las opciones para el desplegable
red_options = get_red_options()
print(f"Red options: {red_options}")  # Debugging line

layout = dbc.Container([
    # Encabezado
    dbc.Row([
        dbc.Col([
            html.Img(src='/assets/encabezado.png', style={'width': '101%'}),
        ], width=12)
    ], style={'position': 'relative', 'width': '101%'}),
    
    dbc.Row([
        dbc.Col([
            html.H1("Seguimiento del trámite", style={'color': '#FFFFFF', 'fontSize': '28px', 'textAlign': 'center', 'font-weight': 'bold', 'fontFamily': 'Calibri'}),
        ], width=12, style={'position': 'absolute', 'top': '5.5%', 'left': '50%', 'transform': 'translate(-50%, -50%)'}),
    ], style={'height': '12px'}, className='px-4'),

    # Filtros y botón en una sola fila
    dbc.Row([
        dbc.Col([
            html.H6("Red", style={'font-size': '16px', 'color': '#606060', 'fontWeight': 'normal', 'fontFamily': 'Calibri'}),
            dcc.Dropdown(
                id='co_red_dropdown',
                options=red_options,
                placeholder='Seleccionar Código de Red',
                style={'width': '100%'}
            )
        ], width=3),
        
        dbc.Col([
            html.H6("# Expediente", style={'font-size': '16px', 'color': '#606060', 'fontWeight': 'normal', 'fontFamily': 'Calibri'}),
            dcc.Input(
                id='nu_expediente_input',
                type='text',
                placeholder='Número de Expediente',
                style={'width': '100%'}
            )
        ], width=3),
        
        dbc.Col([
                dbc.Button(
                    html.I(className="fas fa-search"),
                    id='search-button',
                    style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white', 'width': '15%', 'height': '60%','margin-top': '23px'}
                )
            ], width=3)
    ], style={'margin-bottom': '20px'}, className='px-4'),

    # Tarjetas para los valores de la primera fila
    dbc.Row([
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.P(id="razon-social", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                    html.P(id="min-fecha", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                    html.P(id="tipdoc", className="card-text", style={'font-size': '14px', 'color': '#606060'})
                ]),
                style={"margin-top": "0px", "padding": "0px", "border": "none"}
            )
        ], width=12)
    ], style={'width': '100%', 'margin': '0'}, className='px-4'),

    # Tarjeta del asunto
    dbc.Row([
        dbc.Col([
            html.H6("Asunto", style={'font-size': '16px', 'color': '#0064AF', 'fontWeight': 'normal', 'fontFamily': 'Calibri', 'textAlign': 'center'}),
            dbc.Card(
                dbc.CardBody([
                    html.P(id="asunto", className="card-text", style={'font-size': '18px', 'color': '#606060', 'fontFamily': 'Calibri'}),
                ]),
                style={"margin-top": "0px", "padding": "0px", "border": "none", "text-align":"center", 'font-weight': 'bold', 'background-color': '#F4FAFD', 'fontFamily': 'Calibri', 'margin-bottom': '10px'}
            )
        ], width=12)
    ], style={'width': '100%', 'margin': '0'}, className='px-4'),

    # Tabla de resultados
    dbc.Row([
        dbc.Col([
            html.Div(id='table_container'),
        ], width=12)
    ], style={'margin-top': '20px'}, className='px-4'),
    
], fluid=True, className='p-0 m-0')

def register_callbacks(app):
    @app.callback(
        [Output('table_container', 'children', allow_duplicate=True),
        Output('razon-social', 'children', allow_duplicate=True),
        Output('min-fecha', 'children', allow_duplicate=True),
        Output('tipdoc', 'children', allow_duplicate=True),
        Output('asunto', 'children', allow_duplicate=True)],
        [Input('search-button', 'n_clicks')],
        [State('co_red_dropdown', 'value'),
        State('nu_expediente_input', 'value')],
        prevent_initial_call=True,
    )
    def update_table(n_clicks, co_red, nu_expediente):
        # Verificar si n_clicks es None o igual a 0
        if n_clicks is None or n_clicks == 0:
            return "", "", "", "", ""
        
        print(f"Button clicked with co_red: {co_red} and nu_expediente: {nu_expediente}")  # Línea para depuración
        data = fetch_data(co_red, nu_expediente)
        
        if data is not None and not data.empty:
            # Obtener las últimas 5 filas del DataFrame
            last_5_data = data.tail(5)
            
            # Extraer los valores de la primera fila
            first_row = data.iloc[0]
            razon_social = first_row['Razón Social']
            fecha_envio = first_row['Fecha de envío']
            clase_documento = first_row['Clase de documento']
            asunto = first_row['Asunto']

            # Convertir DataFrame a formato de diccionario para DataTable
            data_dict = last_5_data.to_dict('records')

            # Crear la tabla con estilo
            table = dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in ['N° Expediente', 'Clase de documento', 'Asunto', 'Fecha de envío', 'Origen', 'Fecha de aceptación', 'Destino', 'Red']],
                data=data_dict,
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
                    {'if': {'column_id': 'N° Expediente'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Clase de documento'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '200px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Asunto'}, 'minWidth': '230px', 'width': '230px', 'maxWidth': '250px'},
                    {'if': {'column_id': 'Fecha de envío'}, 'minWidth': '120px', 'width': '120px', 'maxWidth': '150px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Origen'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px'},
                    {'if': {'column_id': 'Fecha de aceptación'}, 'minWidth': '120px', 'width': '120px', 'maxWidth': '150px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Destino'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px'},
                    {'if': {'column_id': 'Red'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px'},
                    {'if': {'column_id': 'Razón Social'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px'}
                ],
            )
            return table, f"Razón social: {razon_social}", f"Fecha de Envío: {fecha_envio.strftime('%d/%m/%Y')}", f"Tipo de documento: {clase_documento}", asunto
        else:
            return "No se encontró información con los datos proporcionados. Intente nuevamente","","","",""