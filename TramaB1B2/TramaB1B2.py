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

def create_oracle_connection1():
    try:
        oracledb.init_oracle_client(lib_dir=r"C:\Users\kings\Downloads\Nueva carpeta\instantclient_23_4")
        connection = oracledb.connect(
            user="User_oper",
            password="TmLQL$Yq.1",
            dsn="10.56.1.76:1527/WNET"
        )
        print("Conexión exitosa.")
        return connection
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"Error al conectar: {error.message}")
        return None

def fetch_data1():
    try:
        conn = create_oracle_connection1()
        if conn is None:
            raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
        
                # Asegúrate de que las fechas estén en el formato correcto
        query = f"""

SELECT A.ATENAMBORICENASICOD AS ORIGEN,
       A.ATENAMBCENASICOD AS CENTRO,
       TO_CHAR(A.ATENAMBATENFEC, 'yyyymm') AS PERIODO,
       DECODE(Y.PERSEXOCOD, '0', '2', '1', '1') AS SEXO,
       CB.GRPETA1COD AS GETAREO,
       Y.PERSECNUM AS PACSECNUM,
       SR.SERVHOSCOD AS CODSER,
       SR.SERVHOSTIPPROCOD AS TIPPROF,
       'ATENME' AS ORIGEN_TIPO
  FROM CTAAM10 A
  LEFT OUTER JOIN CMAME10 X
    ON X.ORICENASICOD = A.ATENAMBORICENASICOD
   AND X.CENASICOD = A.ATENAMBCENASICOD
   AND X.ACTMEDNUM = A.ATENAMBNUM
  LEFT OUTER JOIN CMPER10 Y
    ON X.ACTMEDPACSECNUM = Y.PERSECNUM
  LEFT OUTER JOIN SGSS.CBGPE10 CB
    ON CB.GRPETAEDADCOD = X.ACTMEDEDADATEN
  LEFT OUTER JOIN CMSHO10 SR
    ON SR.SERVHOSCOD = X.ACTMEDSERVHOSCOD
 WHERE TRUNC(A.ATENAMBATENFEC) BETWEEN TO_DATE('01/08/2024', 'DD/MM/YYYY') AND
       TO_DATE('31/08/2024', 'DD/MM/YYYY')
   AND A.ATENAMBESTREGCOD = '1'
   AND Y.PERSECNUM IS NOT NULL
   
UNION ALL

--DETALLE_B1_ATENNOMEDICAS:
SELECT B.ATENOMORICENASICOD AS ORIGEN,
       B.ATENOMCENASICOD AS CENTRO,
       TO_CHAR(B.ATENOMFEC, 'yyyymm') AS PERIODO,
       DECODE(Y1.PERSEXOCOD, '0', '2', '1', '1') AS SEXO,
       CB1.GRPETA1COD AS GETAREO,
       Y1.PERSECNUM AS PACSECNUM,
       SR1.SERVHOSCOD AS CODSER,
       SR1.SERVHOSTIPPROCOD AS TIPPROF,
       'ATENOME' AS ORIGEN_TIPO
  FROM CTANM10 B
  LEFT OUTER JOIN CMAME10 X1
    ON X1.ORICENASICOD = B.ATENOMORICENASICOD
   AND X1.CENASICOD = B.ATENOMCENASICOD
   AND X1.ACTMEDNUM = B.ATENOMACTMEDNUM
  LEFT OUTER JOIN CMPER10 Y1
    ON X1.ACTMEDPACSECNUM = Y1.PERSECNUM
  LEFT OUTER JOIN SGSS.CBGPE10 CB1
    ON CB1.GRPETAEDADCOD = X1.ACTMEDEDADATEN
  LEFT OUTER JOIN CMSHO10 SR1
    ON SR1.SERVHOSCOD = X1.ACTMEDSERVHOSCOD
 WHERE B.ATENOMFEC BETWEEN TO_DATE('01/08/2024', 'DD/MM/YYYY') AND
       TO_DATE('31/08/2024', 'DD/MM/YYYY')
   AND B.ATENOMESTREGCOD = '1'
   AND Y1.PERSECNUM IS NOT NULL
  
UNION ALL

--DETALLE_B1_ATENODONTO:
SELECT CT.CITAMBORICENASICOD AS ORIGEN,
       CT.CITAMBCENASICOD AS CENTRO,
       TO_CHAR(CT.CITAMBPROCONFEC, 'YYYYMM') AS PERIODO,
       DECODE(Y2.PERSEXOCOD, '0', '2', '1', '1') AS SEXO,
       CB2.GRPETA1COD AS GETAREO,
       Y2.PERSECNUM AS PACSECNUM,
       SR2.SERVHOSCOD AS CODSER,
       SR2.SERVHOSTIPPROCOD AS TIPPROF,
       'ATEODON' AS ORIGEN_TIPO
  FROM CTCAM10 CT
  LEFT OUTER JOIN CMAME10 X2
    ON CT.CITAMBORICENASICOD = X2.ORICENASICOD
   AND CT.CITAMBCENASICOD = X2.CENASICOD
   AND CT.CITAMBNUM = X2.ACTMEDNUM
  LEFT OUTER JOIN CMPER10 Y2
    ON X2.ACTMEDPACSECNUM = Y2.PERSECNUM
  LEFT OUTER JOIN SGSS.CBGPE10 CB2
    ON CB2.GRPETAEDADCOD = X2.ACTMEDEDADATEN
  LEFT OUTER JOIN CMSHO10 SR2
    ON SR2.SERVHOSCOD = X2.ACTMEDSERVHOSCOD
 WHERE CT.CITAMBPROCONFEC BETWEEN TO_DATE('01/09/2024', 'DD/MM/YYYY') AND
       TO_DATE('30/09/2024', 'DD/MM/YYYY')
   AND CT.CITAMBSERVHOSCOD IN ('E11', 'E12', 'E19')
   AND CT.ESTCITCOD = '4'
   AND Y2.PERSECNUM IS NOT NULL
        """
        print("Consulta SQL generada:")
        print(query)  # Agrega esto para depurar la consulta SQL

        df = pd.read_sql(query, conn)
        print(f"Cantidad de filas obtenidas: {len(df)}")  # Agrega esto para ver cuántas filas se obtienen
    except Exception as e:
        print(f"Error: {e}")
        df = pd.DataFrame()
    finally:
        if conn:
            conn.close()  # Cerrar la conexión al final
    return df

def fetch_data2():
    try:
        conn = create_oracle_connection1()
        if conn is None:
            raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
        
                # Asegúrate de que las fechas estén en el formato correcto
        query = f"""-- First Query
      SELECT A.ATENAMBORICENASICOD AS ORIGEN,
             A.ATENAMBCENASICOD AS CENTRO,
             TO_CHAR(A.ATENAMBATENFEC, 'yyyymm') AS PERIODO,
             DECODE(Y.PERSEXOCOD, '0', '2', '1', '1') AS SEXO,
             CB.GRPETA1COD AS GETAREO,
             Y.PERSECNUM AS PACSECNUM,
             DECODE(INSTR(DX.DIAGCOD, '.'), 0, DX.DIAGCOD || '.X', DX.DIAGCOD) AS CODDX,
             SR.SERVHOSTIPPROCOD AS TIPPROF,
             'ATENME' AS ORIGEN_TIPO
        FROM CTAAM10 A
        LEFT OUTER JOIN CMAME10 X
          ON X.ORICENASICOD = A.ATENAMBORICENASICOD
         AND X.CENASICOD = A.ATENAMBCENASICOD
         AND X.ACTMEDNUM = A.ATENAMBNUM
        LEFT OUTER JOIN CMPER10 Y
          ON X.ACTMEDPACSECNUM = Y.PERSECNUM
        LEFT OUTER JOIN SGSS.CBGPE10 CB
          ON CB.GRPETAEDADCOD = X.ACTMEDEDADATEN
        LEFT OUTER JOIN CMSHO10 SR
          ON SR.SERVHOSCOD = X.ACTMEDSERVHOSCOD
        LEFT OUTER JOIN CTDAA10 DX
          ON A.ATENAMBORICENASICOD = DX.ATENAMBORICENASICOD
         AND A.ATENAMBCENASICOD = DX.ATENAMBCENASICOD
         AND A.ATENAMBNUM = DX.ATENAMBNUM
       WHERE TRUNC(A.ATENAMBATENFEC) BETWEEN TO_DATE('01/08/2024', 'DD/MM/YYYY') AND
             TO_DATE('31/08/2024', 'DD/MM/YYYY')
         AND A.ATENAMBESTREGCOD = '1'
         AND Y.PERSECNUM IS NOT NULL
         AND '2' = DX.ATENAMBTIPODIAGCOD

      UNION ALL

      -- Second Query
      SELECT B.ATENOMORICENASICOD AS ORIGEN,
             B.ATENOMCENASICOD AS CENTRO,
             TO_CHAR(B.ATENOMFEC, 'yyyymm') AS PERIODO,
             DECODE(Y1.PERSEXOCOD, '0', '2', '1', '1') AS SEXO,
             CB1.GRPETA1COD AS GETAREO,
             Y1.PERSECNUM AS PACSECNUM,
             DECODE(INSTR(DX.ATENMDDIAGCOD, '.'), 0, DX.ATENMDDIAGCOD || '.X', DX.ATENMDDIAGCOD) AS CODDX,
             SR1.SERVHOSTIPPROCOD AS TIPPROF,
             'ATENOME' AS ORIGEN_TIPO
        FROM CTANM10 B
        LEFT OUTER JOIN CMAME10 X1
          ON X1.ORICENASICOD = B.ATENOMORICENASICOD
         AND X1.CENASICOD = B.ATENOMCENASICOD
         AND X1.ACTMEDNUM = B.ATENOMACTMEDNUM
        LEFT OUTER JOIN CMPER10 Y1
          ON X1.ACTMEDPACSECNUM = Y1.PERSECNUM
        LEFT OUTER JOIN SGSS.CBGPE10 CB1
          ON CB1.GRPETAEDADCOD = X1.ACTMEDEDADATEN
        LEFT OUTER JOIN CMSHO10 SR1
          ON SR1.SERVHOSCOD = X1.ACTMEDSERVHOSCOD
        LEFT OUTER JOIN CTDAN10 DX
          ON B.ATENOMORICENASICOD = DX.ATENOMORICENASICOD
         AND B.ATENOMCENASICOD = DX.ATENOMCENASICOD
         AND B.ATENOMACTMEDNUM = DX.ATENOMACTMEDNUM
       WHERE B.ATENOMFEC BETWEEN TO_DATE('01/08/2024', 'DD/MM/YYYY') AND
             TO_DATE('31/08/2024', 'DD/MM/YYYY')
         AND B.ATENOMESTREGCOD = '1'
         AND Y1.PERSECNUM IS NOT NULL
         AND '2' = DX.ATENMDTIPODIAGCOD
         
      UNION ALL
      
	      --DETALLE_B2_ATENODONTO:
	SELECT CT.CITAMBORICENASICOD AS ORIGEN,
	       CT.CITAMBCENASICOD AS CENTRO,
	       TO_CHAR(CT.CITAMBPROCONFEC, 'YYYYMM') AS PERIODO,
	       DECODE(Y1.PERSEXOCOD, '0', '2', '1', '1') AS SEXO,
	       CB1.GRPETA1COD AS GETAREO,
	       Y1.PERSECNUM AS PACSECNUM,
	       DECODE(INSTR(DX1.DIAGCOD, '.'), 0, DX1.DIAGCOD || '.X', DX1.DIAGCOD) AS CODDX,
	       SR1.SERVHOSTIPPROCOD AS TIPPROF,
	       'ATEODO' AS ORIGEN_TIPO
	  FROM CTCAM10 CT
	  LEFT OUTER JOIN CMAME10 X1
	    ON CT.CITAMBORICENASICOD = X1.ORICENASICOD
	   AND CT.CITAMBCENASICOD = X1.CENASICOD
	   AND CT.CITAMBNUM = X1.ACTMEDNUM
	  LEFT OUTER JOIN CMPER10 Y1
	    ON X1.ACTMEDPACSECNUM = Y1.PERSECNUM
	  LEFT OUTER JOIN SGSS.CBGPE10 CB1
	    ON CB1.GRPETAEDADCOD = X1.ACTMEDEDADATEN
	  LEFT OUTER JOIN CMSHO10 SR1
	    ON SR1.SERVHOSCOD = X1.ACTMEDSERVHOSCOD
	  LEFT OUTER JOIN CTDAO10 DX1
	    ON CT.CITAMBORICENASICOD = DX1.ATENODOORICENASICOD
	   AND CT.CITAMBCENASICOD = DX1.ATENODOCENASICOD
	   AND CT.CITAMBNUM = DX1.ATENODONUM
	 WHERE CT.CITAMBPROCONFEC BETWEEN TO_DATE('01/08/2024', 'DD/MM/YYYY') AND
	       TO_DATE('01/08/2024', 'DD/MM/YYYY')
	   AND CT.CITAMBSERVHOSCOD IN ('E11', 'E12', 'E19')
	   AND CT.ESTCITCOD = '4'
	   AND Y1.PERSECNUM IS NOT NULL
	   AND '2' = DX1.TIPODIAGCOD
        """

        print("Consulta SQL generada:")
        print(query)  # Agrega esto para depurar la consulta SQL

        df = pd.read_sql(query, conn)
        print(f"Cantidad de filas obtenidas: {len(df)}")  # Agrega esto para ver cuántas filas se obtienen
    except Exception as e:
        print(f"Error: {e}")
        df = pd.DataFrame()
    finally:
        if conn:
            conn.close()  # Cerrar la conexión al final
    return df

# Saber el número de procesadores lógicos disponibles
num_hilos_default = os.cpu_count()

print(f"Número de hilos por defecto: {num_hilos_default}")

# Si quieres, puedes también fijar explícitamente el número de hilos:
with ThreadPoolExecutor(max_workers=4) as executor:  # Aquí se utilizarán 4 hilos
    future1 = executor.submit(fetch_data1)
    future2 = executor.submit(fetch_data2)

    data1 = future1.result()
    data2 = future2.result()


def get_unique_options():
    connection = create_oracle_connection1()
    if connection:
        try:
            cursor = connection.cursor()
            query = "SELECT DISTINCT CENASIDES, CENASICOD FROM CMCAS10 WHERE ESTREGCOD='1' AND ORICENASICOD='1'"
            cursor.execute(query)
            options = [{'label': row[0], 'value': row[1]} for row in cursor.fetchall()]
            cursor.close()
            return options
        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Error al ejecutar la consulta: {error.message}")
            return []
        finally:
            connection.close()
    return []


options = get_unique_options()

# Reemplazar fetch_data por una función de ejemplo
def CAS():
    try:
        conn = create_oracle_connection1()
        if conn is None:
            raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
        
                # Asegúrate de que las fechas estén en el formato correcto
        query = f"""

SELECT CENASICOD AS CENTRO, CENASIRENAESCOD AS COD_IPRESS, CENASIRENAESUNIGESCOD AS COD_UGIPRESS FROM CMCAS10 WHERE ESTREGCOD='1' AND ORICENASICOD ='1'
        """
        print("Consulta SQL generada:")
        print(query)  # Agrega esto para depurar la consulta SQL

        df = pd.read_sql(query, conn)
        print(f"Cantidad de filas obtenidas: {len(df)}")  # Agrega esto para ver cuántas filas se obtienen
    except Exception as e:
        print(f"Error: {e}")
        df = pd.DataFrame()
    finally:
        if conn:
            conn.close()  # Cerrar la conexión al final
    return df

cas=CAS()


# Pivot table para contar los valores de PACSECNUM en cada ORIGEN_TIPO
pivoted_df = pd.pivot_table(data1, 
                            values='PACSECNUM', 
                            index=['ORIGEN', 'CENTRO', 'PERIODO', 'SEXO', 'GETAREO'], 
                            columns='ORIGEN_TIPO', 
                            aggfunc='count', 
                            fill_value=0)

# Resetear el índice del DataFrame pivotado
pivoted_df_reset = pivoted_df.reset_index()

# Crear la columna con el conteo distintivo de PACSECNUM
distinct_count = data1.groupby(['ORIGEN', 'CENTRO', 'PERIODO', 'SEXO', 'GETAREO'])['PACSECNUM'].nunique().reset_index(name='ATENDIDOS')

# Unir la nueva columna Distinct_PACSECNUM al DataFrame pivoted_df_reset
pivoted_df_final = pd.merge(pivoted_df_reset, distinct_count, on=['ORIGEN', 'CENTRO', 'PERIODO', 'SEXO', 'GETAREO'])

# Mostrar el DataFrame final con la nueva columna de conteo distintivo
pivoted_df_final_f=pd.merge(pivoted_df_final, cas, on='CENTRO', how='left')

pivoted_df_final_f=pivoted_df_final_f[['COD_IPRESS','COD_UGIPRESS','CENTRO','PERIODO','SEXO','GETAREO','ATENME','ATENOME','ATEODON','ATENDIDOS']]

# Pivot table para contar los valores de PACSECNUM en cada ORIGEN_TIPO
pivoted_df2 = pd.pivot_table(data2, 
                            values='PACSECNUM', 
                            index=['ORIGEN', 'CENTRO', 'PERIODO', 'SEXO', 'GETAREO', 'CODDX'], 
                            columns='ORIGEN_TIPO', 
                            aggfunc='count', 
                            fill_value=0)

# Resetear el índice del DataFrame pivotado
pivoted_df_reset2 = pivoted_df2.reset_index()

# Crear la columna con el conteo distintivo de PACSECNUM
distinct_count2 = data2.groupby(['ORIGEN', 'CENTRO', 'PERIODO', 'SEXO', 'GETAREO','CODDX'])['PACSECNUM'].nunique().reset_index(name='ATENDIDOS')

# Unir la nueva columna Distinct_PACSECNUM al DataFrame pivoted_df_reset
pivoted_df_final2 = pd.merge(pivoted_df_reset2, distinct_count2, on=['ORIGEN', 'CENTRO', 'PERIODO', 'SEXO', 'GETAREO', 'CODDX'])

pivoted_df_final2_f=pd.merge(pivoted_df_final2, cas, on='CENTRO', how='left')

pivoted_df_final2_f=pivoted_df_final2_f[['COD_IPRESS','COD_UGIPRESS','CENTRO','PERIODO','SEXO','GETAREO','CODDX','ATENME','ATENOME','ATEODO','ATENDIDOS']]


external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css',  # Bootstrap CSS
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.1/css/all.min.css'  # Bootstrap Icons
]

# Crear la aplicación Dash
app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=external_stylesheets)

# Layout del primer reporte (Tabla B1)
tab1_layout = dbc.Container([
    
    html.Div(style={'height': '12px'}),
    dbc.Row([
        # Date Picker for Start Date
        dbc.Col(
    [
        dcc.Dropdown(
            id='cas-trama',
            options=options,
            placeholder='Seleccione un centro asistencial',
            className='px-0 mx-0',
            optionHeight=45,
            style={'height': '45px', 'width': '100%'}
        ),
    ],
    className='px-0 mx-0',
    width=9, md=3, lg=3
),
        dbc.Col(
            dbc.Button(
                html.I(className="fas fa-search"),
                id='submit-val1-trama',
                style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white', 'width': '58%', 'height': '45px', 'fontSize': '20px'},

            ),
            className='mt- 2 mb-2',
            width=3, md=1, lg=1
        ),
        dbc.Col(
            dcc.Loading(
                id="loading-download2",
                type="default",
                children=html.Div([
                    dbc.Button(
                        [html.I(className="fas fa-file-excel"), html.Span(" Descargar datos")],
                        id='download-btn1-trama',
                        n_clicks=0,
                        style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white', 'width': '60%', 'height': '45px'}
                    ),
                    dcc.Download(id="download-dataframe-csv1-trama")
                ]),
                style={'margin-right': '105px'}

            ),
            width=12, md=2, lg=2
        ),
    ], className="mb-4"),

    # Salida de datos con dbc.Spinner
    dbc.Spinner(
        id="loading-output1",
        size="md",
        color="primary",
        type="border",
        fullscreen=False,
        children=html.Div(id='output-data-table-tab1-trama')
    ),
    dbc.Alert(id="error-alert1", is_open=False, dismissable=True, color="danger"),
], fluid=True)

# Layout del segundo reporte (Tabla B2)
tab2_layout = dbc.Container([
    # Título
    html.Div(style={'height': '12px'}),

    dbc.Row([
        # Date Picker for Start Date
        dbc.Col(
    [
        dcc.Dropdown(
            id='cas2-trama',
            options=options,
            placeholder='Seleccione un centro asistencial',
            optionHeight=45,
            className='px-0 mx-0',
            style={'height': '45px', 'width': '100%'}
        ),
    ],
    className='px-0 mx-0',
    width=9, md=3, lg=3
),
        dbc.Col(
            dbc.Button(
                html.I(className="fas fa-search"),
                id='submit-val2-trama',
                style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white', 'width': '58%', 'height': '45px', 'fontSize': '20px'},

            ),
            className='mt- 2 mb-2',
            width=3, md=1, lg=1
        ),
        dbc.Col(
            dcc.Loading(
                id="loading-download",
                type="default",
                children=html.Div([
                    dbc.Button(
                        [html.I(className="fas fa-file-excel"), html.Span(" Descargar datos")],
                        id='download-btn2-trama',
                        n_clicks=0,
                        style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white', 'width': '60%', 'height': '45px'}
                    ),
                    dcc.Download(id="download-dataframe-csv2-trama")
                ]),
                style={'margin-right': '105px'}

            ),
            width=12, md=4, lg=2
        ),
    ], className="mb-4"),

    # Salida de datos con dbc.Spinner
    dbc.Spinner(
        id="loading-output2",
        size="md",
        color="primary",
        type="border",
        fullscreen=False,
        children=html.Div(id='output-data-table-tab2-trama')
    ),
    dbc.Alert(id="error-alert2", is_open=False, dismissable=True, color="danger"),
], fluid=True)

# Layout principal con las pestañas
layout = dbc.Container([
    html.Div(style={'height': '14px'}),
dbc.Row([
    # Título y logotipo en la misma fila
    dbc.Col([
        html.Div(style={'height': '1px'}),
        html.H1("SUSALUD - TABLAS AGREGADAS B1 Y B2", style={'color': '#0064AF', 'fontSize': '28px', 'textAlign': 'Left', 'font-weight': 'bold', 'fontFamily': 'Calibri'}),
        html.H2("Fuente: ESSI. V.1.0.0. Fecha de actualización 01/10/2024", style={'color': '#0064AF', 'fontSize': '12px'}),
    ], width=10, className="mb-2"),   # Ajusta el ancho según sea necesario
    dbc.Col([
        html.Img(src="/assets/Logotipo sin Slogan_Horizontal_Color Transparente.png", alt="Essalud", width="160"),
    ], width=2, className="d-none d-lg-flex align-items-center justify-content-end"),
]),
    dbc.Tabs([
        dbc.Tab(
            tab1_layout,
            label='TABLA B1',
            tab_style={
                'color': '#0064AF',
                'fontWeight': 'bold',
                'fontFamily': 'Calibri'
            },
            active_tab_style={
                'color': 'white',
                'fontWeight': 'bold',
                'fontFamily': 'Calibri'
            }
        ),
        dbc.Tab(
            tab2_layout,
            label='TABLA B2',
            tab_style={
                'color': '#0064AF',
                'fontWeight': 'bold',
                'fontFamily': 'Calibri'
            },
            active_tab_style={
                'color': 'white',
                'fontWeight': 'bold',
                'fontFamily': 'Calibri'
            }
        )
    ],
    style={'borderBottom': '2px solid #0064AF'}),
], fluid=True)



def register_callbacks(app):
    # Callbacks para el primer reporte (TABLA B1)
    @app.callback(
        Output('output-data-table-tab1-trama', 'children'),
        Input('submit-val1-trama', 'n_clicks'),
        State('cas-trama', 'value')
    )
    def update_output_tab1(n_clicks, cas):
        if n_clicks is None:
            return html.Div("Ingrese el centro asistencial", style={'color': '#0064AF', 'fontSize': '18px'})
        
        if n_clicks > 0:
            try:
                if not cas:
                    return html.Div("Por favor, complete todos los campos.", style={'color': 'red', 'fontSize': '18px'})
                
                df = pivoted_df_final_f[pivoted_df_final_f['CENTRO'] == cas].head(20)
                
                if df.empty:
                    return html.Div("No se encontraron datos para los criterios proporcionados.")
                
                return html.Div([
                    html.H4("Data de muestra:", style={'color': '#0064AF', 'width': '100%', 'height': '45px', 'fontSize': '20px'}),
                    dash_table.DataTable(
                        id='data-table-tab1',
                        columns=[{"name": i, "id": i} for i in ['COD_IPRESS','COD_UGIPRESS','CENTRO','PERIODO','SEXO','GETAREO','ATENME','ATENOME','ATEODON','ATENDIDOS']],
                        data=df.to_dict('records'),
                        style_table={'overflowX': 'auto'},
                        page_size=15,
                        page_current=0, 
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
                            {'if': {'column_id': 'CENTRO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '200px', 'textAlign': 'center'},
                            {'if': {'column_id': 'PERIODO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '250px', 'textAlign': 'center'},
                            {'if': {'column_id': 'SEXO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px', 'textAlign': 'center'},
                            {'if': {'column_id': 'GETAREO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '250px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATENME'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATENOME'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATEODON'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATENDIDOS'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'COD_IPRESS'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'COD_UGIPRESS'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'}
                        ],
                        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(244, 250, 253)'}],
                    )
                ])
            except Exception as e:
                return html.Div(f'Error: {e}')

    @app.callback(
        Output("download-dataframe-csv1-trama", "data"),
        Input("download-btn1-trama", "n_clicks"),
        State('cas-trama', 'value'),
        prevent_initial_call=True
    )
    def download_csv1(n_clicks, cas):
        if n_clicks > 0:
            try:
                if not cas:
                    return dcc.send_data_frame(pd.DataFrame().to_csv, filename="data_empty.csv")
                # Filtrar el DataFrame por la columna 'CENTRO'
                df_complete = pivoted_df_final_f[pivoted_df_final_f['CENTRO'] == cas]
                # If the difference is valid, continue with the download
                return dcc.send_data_frame(df_complete.to_csv, filename="DATA_B1.csv", index=False)
            
            except Exception as e:
                return dcc.send_data_frame(pd.DataFrame().to_csv, filename="data_empty.csv", index=False)

    @app.callback(
        Output('output-data-table-tab2-trama', 'children'),
        Input('submit-val2-trama', 'n_clicks'),
        State('cas2-trama', 'value')
    )
    def update_output_tab2(n_clicks, cas):
        if n_clicks is None:
            return html.Div("Ingrese el centro asistencial", style={'color': '#0064AF', 'fontSize': '18px'})
        
        if n_clicks > 0:
            try:
                if not cas:
                    return html.Div("Por favor, complete todos los campos.", style={'color': 'red', 'fontSize': '18px'})
                
                df = pivoted_df_final2_f[pivoted_df_final2_f['CENTRO'] == cas].head(20)

                if df.empty:
                    return html.Div("No se encontraron datos para los criterios proporcionados.")
                
                return html.Div([
                    html.H4("Data de muestra:", style={'color': '#0064AF', 'width': '100%', 'height': '45px', 'fontSize': '20px'}),
                    dash_table.DataTable(
                        id='data-table-tab2',
                        columns=[{"name": i, "id": i} for i in ['COD_IPRESS','COD_UGIPRESS','CENTRO','PERIODO','SEXO','GETAREO','CODDX','ATENME','ATENOME','ATEODO','ATENDIDOS']],
                        data=df.to_dict('records'),
                        style_table={'overflowX': 'auto'},
                        page_size=15,
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
                            {'if': {'column_id': 'PERIODO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '250px', 'textAlign': 'center'},
                            {'if': {'column_id': 'CENTRO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '250px', 'textAlign': 'center'},
                            {'if': {'column_id': 'SEXO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '150px', 'textAlign': 'center'},
                            {'if': {'column_id': 'GETAREO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '250px', 'textAlign': 'center'},
                            {'if': {'column_id': 'CODDX'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATENME'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATENOME'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATEODO'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'ATENDIDOS'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'COD_IPRESS'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'},
                            {'if': {'column_id': 'COD_UGIPRESS'}, 'minWidth': '100px', 'width': '100px', 'maxWidth': '100px', 'textAlign': 'center'}
                        ],
                        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(244, 250, 253)'}],
                    )
                ])
            except Exception as e:
                return html.Div(f'Error: {e}')

    @app.callback(
        Output("download-dataframe-csv2-trama", "data"),
        Input("download-btn2-trama", "n_clicks"),
        State('cas2-trama', 'value'),
        prevent_initial_call=True
    )
    def download_csv2(n_clicks, cas):
        if n_clicks > 0:
            try:
                if not cas:
                    return dcc.send_data_frame(pd.DataFrame().to_csv, filename="data_empty.csv")
                
                # If the difference is valid, continue with the download
                df_complete = pivoted_df_final2_f[pivoted_df_final2_f['CENTRO'] == cas]
                return dcc.send_data_frame(df_complete.to_csv, filename="DATA_B2.csv", index=False)
            
            except Exception as e:
                return dcc.send_data_frame(pd.DataFrame().to_csv, filename="data_empty.csv")