import dash
from dash import dcc, html, Output, Input, State
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import create_engine
from dash import dash_table
import io
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import base64
from urllib.parse import parse_qs
import csv

# Configuración de cifrado
clave = b'ESSALUDSGD2024$$'
iv = b'ESSALUDSGD2024$$'  # El IV debe ser de 16 bytes

def descifrar_codigo(codigo_cifrado):
    if not codigo_cifrado:
        print("Código cifrado es None o una cadena vacía")
        return None

    try:
        # Decodificar el código cifrado de Base64
        codigo_cifrado_bytes = base64.b64decode(codigo_cifrado)

        # Crear el cifrador AES
        cipher = AES.new(clave, AES.MODE_CBC, iv)

        # Desencriptar los datos
        datos_descifrados = unpad(cipher.decrypt(codigo_cifrado_bytes), AES.block_size)
        texto_descifrado = datos_descifrados.decode('utf-8')
        return texto_descifrado
    except (ValueError, KeyError) as e:
        print("Error al descifrar:", e)
        return None


    
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

# Función para obtener los datos según el filtro
def fetch_data(co_red, nu_expediente):
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
                    ) AS NOMBRE_COMPLETO,
                    tdtc_expediente.nu_expediente AS NRO_EXPEDIENTE,
                    si_mae_tipo_doc.cdoc_desdoc AS CLASE_DOCUMENTO,
                    tdtv_remitos.de_asu AS ASUNTO,
                    TO_CHAR(tdtv_remitos.fe_emi, 'DD/MM/YY HH24:MI') AS FECHA_ENVIO,
                    rhtm_dependencia_or.de_dependencia AS ORIGEN,
                    TO_CHAR(tdtv_destinos.fe_rec_doc, 'DD/MM/YY HH24:MI') AS FECHA_ACEPTACION,
                    rhtm_dependencia_dest.de_dependencia AS DESTINO,
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
                    AND si_redes.co_red = '{co_red}'
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

# Definir el layout como función
def layout(codigo=None):
    # Descifra el código y obtiene los valores necesarios
    texto_descifrado = descifrar_codigo(codigo)
    co_red = texto_descifrado[:4]
    nu_expediente = texto_descifrado[5:]
    data = fetch_data(co_red, nu_expediente)
    last_5_data = data.tail(5)
    first_row = data.iloc[0]
    razon_social = first_row['Razón Social']
    fecha_envio = first_row['Fecha de envío']
    clase_documento = first_row['Clase de documento']
    asunto_redes = first_row['Asunto']
    red=first_row['Red']
    expediente=first_row['N° Expediente']
    # data_dict = last_5_data.to_dict('records')

    return dbc.Container([
    dbc.Container(fluid=True, className="p-0 m-0", children=[
        dbc.Navbar(
            dbc.Container(fluid=True, className="d-flex justify-content-between align-items-center p-0", children=[
                # Logo SGD Redes
                dbc.Row([
                    dbc.Col([
                        html.Img(src="/assets/logoSGDredes-blanco.png", alt="SGD", className="d-block d-lg-none", style={"width": "60px", "height": "auto"}),
                        html.Img(src="/assets/logoSGDredes-blanco.png", alt="SGD", className="d-none d-lg-block", style={"width": "120px", "height": "auto"})
                    ], className="d-flex align-items-center"),
                ]),

                # Título Centrado
                dbc.Row([
                    dbc.Col([
                        html.H2("Seguimiento del trámite", className="mb-0 text-white d-none d-lg-block", style={'fontWeight': 'bold'}),
                        html.H3("Seguimiento del trámite", className="mb-0 text-white d-block d-lg-none", style={"fontSize": "1.25rem", 'fontWeight': 'bold'})
                    ], className="text-center flex-grow-1"),
                ]),

                # Logo Essalud (Visible solo en pantallas grandes)
                dbc.Row([
                    dbc.Col([
                        html.Img(src="/assets/logo-essalud-blanco.svg", alt="Essalud", width="110", height="24")
                    ], className="d-none d-lg-flex align-items-center justify-content-end"),
                ])
            ]),
            color="sgd",
            dark=True,
            className="navbar-expand-lg bg-sgd mb-3",
            style={"background": "linear-gradient(90deg, #013B84 0%, #1E9ADA 100%)"}
        )
    ]),
    # Tarjetas para los valores de la primera fila
    dbc.Row([
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.P(f"Red: {red}", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                    html.P(f"Num. Expediente: {expediente}", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                ]),
                style={"margin-top": "7px", "padding": "0px", "border": "none"}
            )
        ], width=12, md=6, lg=5),
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.P(f"Razón Social: {razon_social}", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                    html.P(f"Fecha de Envío: {fecha_envio}", className="card-text", style={'font-size': '14px', 'color': '#606060'}),
                    html.P(f"Clase de Documento: {clase_documento}", className="card-text", style={'font-size': '14px', 'color': '#606060'})
                ]),
                style={"margin-top": "7px", "padding": "0px", "border": "none"}
            )
        ], width=12, md=6, lg=5),
         dbc.Col([
                    dbc.Button(
                        [html.I(className="fas fa-file-excel"), html.Span(" Descargar datos")],
                        id='download-button',
                        style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white'},
                        className='align-middle'
                    ),
                    dcc.Download(id="download-csv-redes")
                ], width=12, md=12, lg=2, className='mb-2 mt-4 text-center')
            ], style={'margin': '0', 'width': '100%'}, className='px-4'),

    # Tarjeta del asunto_redes
    dbc.Row([
        dbc.Col([
            html.H6("Asunto", style={'font-size': '14px', 'color': '#0064AF', 'fontWeight': 'normal', 'fontFamily': 'Calibri', 'textAlign': 'center'}),
            dbc.Card(
                dbc.CardBody([
                    html.P(asunto_redes, id="asunto_redes", className="card-text", style={'font-size': '16px', 'color': '#606060', 'fontFamily': 'Calibri'}),
                ]),
                style={"margin-top": "10px", "padding": "0px", "border": "none", "text-align": "center", 'font-weight': 'bold', 'background-color': '#F4FAFD', 'margin-bottom': '10px'}
            )
        ], width=12)
    ], style={'margin': '0', 'width': '100%'}, className='px-4'),

    # Tabla de resultados
    dbc.Row([
        dbc.Col([
            dash_table.DataTable(
                data=data.to_dict('records'),
                id='table',
                columns=[{"name": i, "id": i} for i in
                        ['N° Expediente', 'Clase de documento', 'Asunto', 'Fecha de envío', 'Origen', 'Fecha de aceptación', 'Destino', 'Red']],
                style_table={
                    'overflowX': 'auto',
                    'border': 'none',
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
                    'textAlign': 'center',
                    'border': '1px solid white'
                },
                fixed_rows={'headers': True},
                style_data={ 'border': '1px solid white' },
                style_cell_conditional=[
                    {'if': {'column_id': 'N° Expediente'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Clase de documento'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '200px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Asunto'}, 'minWidth': '230px', 'width': '230px', 'maxWidth': '250px'},
                    {'if': {'column_id': 'Fecha de envío'}, 'minWidth': '120px', 'width': '120px', 'maxWidth': '150px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Origen'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px'},
                    {'if': {'column_id': 'Fecha de aceptación'}, 'minWidth': '120px', 'width': '120px', 'maxWidth': '150px', 'textAlign': 'center'},
                    {'if': {'column_id': 'Destino'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px'},
                    {'if': {'column_id': 'Red'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px'},
                    {'if': {'column_id': 'Razón Social'}, 'minWidth': '80px', 'width': '80px', 'maxWidth': '100px'},
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(244, 250, 253)',
                    }
                ],
            ),
        ], width=12)
    ], style={'margin-top': '20px', 'width': '100%'}, className='px-4 pt-0'),
], fluid=True, className='p-0 m-0')


def register_callbacks(app):
    @app.callback(
        Output("download-csv-redes", "data"),
        Input("download-button", "n_clicks"),
        State('store-co-red', 'data'),
        State('store-nu-expediente', 'data'),
        prevent_initial_call=True,
    )
    def download_csv(n_clicks, co_red, nu_expediente):
        if n_clicks is None:
            return None

        # Llama a la función fetch_data usando los valores obtenidos del dcc.Store
        data = fetch_data(co_red, nu_expediente)

        # Verifica si se obtuvo algún dato
        if data is not None and not data.empty:
            return dcc.send_data_frame(
                data.to_csv,
                "resultados.csv",
                sep=';',          # Establece el separador como punto y coma
                index=False,      # No incluye el índice en el archivo CSV
                quoting=csv.QUOTE_ALL,  # Sin delimitadores alrededor de los campos
                escapechar=''     # Asegura que no haya caracteres de escape
            )
        
        return None
