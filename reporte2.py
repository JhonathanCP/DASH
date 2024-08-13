import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import dash_bootstrap_components as dbc
from dash import dash_table

# Function to create a database connection
def create_connection():
    return create_engine('postgresql://postgres:AKindOfMagic@10.0.1.228:5432/dw_essalud')

# Fetch data functions
def fetch_data():
    engine = create_connection()
    with engine.connect() as conn:
        query = """
            SELECT
                red.des_red,
                cas.des_cas,
                area.des_are,
                tipdoc.des_tdo,
                p.pac_dni,
                p.pac_apat,
                p.pac_amat,
                p.pac_npri,
                p.pac_nseg,
                p.pac_edad,
                p.def_fec,
                p.id_cie_p,
                p.id_cie_s1,
                p.id_cie_s2,
                p.id_cie_s3,
                p.id_cie_s4,
                p.id_cie_s5,
                p.id_cie_s6,
                p.id_cie_s7,
                p.id_cie_s8,
                p.id_cie_s9,
                p.id_cie_s10,
                p.validado,
                p.pac_sexo,
                p.dengue
            FROM dat_fac_essi p
            LEFT JOIN dim_red red ON p.id_red = red.id_red
            LEFT JOIN dim_cas cas ON p.id_cas = cas.id_cas
            LEFT JOIN dim_areas area ON p.id_area = area.id_area
            LEFT JOIN dim_tipdoc tipdoc ON p.id_pac_tipdoc = tipdoc.id_tipdoc
        """
        result = pd.read_sql(query, conn)
    return result

def fetch_data2():
    engine = create_connection()
    with engine.connect() as conn:
        query = "SELECT * FROM dim_cie10"
        result = pd.read_sql(query, conn)
    return result

def fetch_data3():
    engine = create_connection()
    with engine.connect() as conn:
        query = "SELECT * FROM etl_act"
        result = pd.read_sql(query, conn)
    return result

# Fetch initial data
dim_cie10 = fetch_data2()
dim_cie10_s = fetch_data2()
etl_act = fetch_data3()
df = fetch_data()

etl_act = etl_act.reset_index(drop=True)
etl_act['fec_act'] = pd.to_datetime(etl_act['fec_act'])
valor = etl_act.loc[etl_act['id_mod'] == 23, 'fec_act'].iloc[0]
fecha_simple = valor.strftime('%d/%m/%Y')

df['dengue'] = pd.to_numeric(df['dengue'], errors='coerce').fillna(0).astype(int)
valores_permitidos = {452, 453, 454, 455}

def obtener_cie_sec(row):
    for i in range(1, 11):
        if row[f'id_cie_s{i}'] in valores_permitidos:
            return row[f'id_cie_s{i}']
    return np.nan

df['CIE_SEC'] = df.apply(obtener_cie_sec, axis=1)
df = df.rename(columns={'id_cie_p': 'id_cie', 'CIE_SEC': 'id_cie_s'})
dim_cie10_s = dim_cie10_s.rename(columns={'id_cie': 'id_cie_s', 'cod_cie': 'cod_cie_s', 'des_cie': 'des_cie_s'})
df_filtrado = df[df['dengue'] == 1]

merged_df = pd.merge(df_filtrado, dim_cie10, on='id_cie', how='left')
merged_df2 = pd.merge(merged_df, dim_cie10_s, on='id_cie_s', how='left')

merged_df2['validado'] = pd.to_numeric(merged_df2['validado'], errors='coerce').fillna(0).astype(int)
merged_df2['dengue'] = pd.to_numeric(merged_df2['dengue'], errors='coerce').fillna(0).astype(int)
merged_df2['validado2'] = merged_df2.apply(lambda row: 'Válido' if row['validado'] >= 1 else 'No cumple', axis=1)
merged_df2['def_fec'] = pd.to_datetime(merged_df2['def_fec'])
merged_df2['Año'] = merged_df2['def_fec'].dt.year

# Function to get unique options for dropdowns
def get_unique_options(df, column_name):
    return [{'label': value, 'value': value} for value in df[column_name].unique()]

# Get unique options for filters
red_options = get_unique_options(merged_df2, 'des_red')
cas_options = get_unique_options(merged_df2, 'des_cas')
are_options = get_unique_options(merged_df2, 'des_are')
val_options = get_unique_options(merged_df2, 'validado2')
id_options = get_unique_options(merged_df2, 'pac_dni')
año_options = get_unique_options(merged_df2, 'Año')

merged_df2 = merged_df2.rename(columns={
    'des_cie': 'Principal motivo de consulta',
    'des_are': 'Área',
    'pac_dni': 'ID',
    'cod_cie': 'CIE10',
    'des_cie_s': 'Otros motivos de consulta',
    'def_fec': 'Fecha de defunción',
    'des_cas': 'IPRESS',
    'des_red': 'Red',
    'validado2': 'Estado'
})

layout = dbc.Container([
    html.Div(style={'height': '12px'}),
   
    html.H1("Reporte de defunciones validadas de Dengue", style={'color': '#0064AF', 'fontSize': '28px'}),
    html.H2(f"Fuente: ESSI. Actualizado al {fecha_simple}. Actualización diaria. Validado por la Oficina de Inteligencia e Información Sanitaria - GCPS. V.1.0.0", style={'color': '#0064AF', 'fontSize': '12px'}),
    html.Hr(style={'border': '1px solid #0064AF'}),
    html.Div(
        children=[
            html.Div(
                "GCPS - OIIS",
                style={
                    'position': 'absolute',
                    'top': '15px',
                    'right': '25px',
                    'color': '#0064AF',
                    'fontSize': '14px',
                    'textAlign': 'center'
                }
            ),
            html.Div(
                "GCTIC - GSIT",
                style={
                    'position': 'absolute',
                    'top': '35px',  # Ajusta este valor para que se posicione justo debajo del primer texto
                    'right': '20px',
                    'color': '#0064AF',
                    'fontSize': '14px',
                    'textAlign': 'center'
                }
            )
        ]
    ),
    dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Red", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Dropdown(
                        id='filter-des_red',
                        options=red_options,
                        placeholder="Selecciona una red",
                        style={'font-size': '14px', 'height': '40px'},
                        maxHeight=150,
                        optionHeight=45
                    ),
                ], width=2),

                dbc.Col([
                    html.Label("Centro asistencial", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Dropdown(
                        id='filter-des_cas',
                        options=cas_options,
                        placeholder="Selecciona un Cas",
                        style={'font-size': '12px', 'height': '40px'},
                        maxHeight=150,
                        optionHeight=45
                    ),
                ], width=2),

                dbc.Col([
                    html.Label("Área", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Dropdown(
                        id='filter-des_are',
                        options=are_options,
                        placeholder="Selecciona un área",
                        style={'font-size': '12px', 'height': '40px'},
                        maxHeight=150
                    ),
                ], width=2),

                dbc.Col([
                    html.Label("Estado", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Dropdown(
                        id='filter-validado',
                        options=val_options,
                        placeholder="Selecciona un estado",
                        style={'font-size': '12px', 'height': '40px'},
                        maxHeight=150
                    ),
                ], width=1),

                dbc.Col([
                    html.Label("ID", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Dropdown(
                        id='filter-id',
                        options=id_options,
                        placeholder="ID",
                        style={'font-size': '12px', 'height': '40px'},
                        maxHeight=150
                    ),
                ], width=1),

                dbc.Col([
                    html.H6("Año de defunciones", style={'font-size': '16px', 'color': '#0064AF', 'fontWeight': 'normal'}),
                    dcc.Dropdown(
                        id='filter-año',
                        options=año_options,
                        placeholder="Año",
                        style={'font-size': '12px', 'height': '33px'},
                        maxHeight=150
                    )
                ], width=2),

                dbc.Col([
                    dbc.Card(
                        dbc.CardBody([
                            html.H4("# Defunciones", className="card-title", style={'color': '#0064AF', 'fontSize': '12px', 'text-align': 'center','margin-top': '-1px','margin-bottom': '2px'}),
                            html.Div(id='total-def', className="card-text", style={'color': '#0064AF', 'fontSize': '20px', 'text-align': 'center'})
                        ]),
                        style={'background-color': '#F4FAFD', 'border-color': '#35A2C1','height':'70px'}
                    ),
                ], width=1),

                dbc.Col([
                    html.Div(
                        children=[
                            html.Button("Descargar datos", id="btn_csv", n_clicks=0, className="btn", style={
                                'color': '#FFFFFF', 'background-color': '#0F71F2',
                                'border': 'none', 'padding': '10px 20px',
                                'text-align': 'center', 'font-size': '16px',
                                'border-radius': '5px'
                            }),
                            dcc.Download(id="download-dataframe-csv-dengue"),
                        ],
                        style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'height': '100%'}
                    )
                ], width=1),
            ]),

            html.Br(),

            dbc.Row([
                dbc.Col([
                    dash_table.DataTable(
                        id='table-dengue',
                        columns=[{"name": i, "id": i} for i in [
                            'ID', 'Área', 'CIE10', 'Principal motivo de consulta','Otros motivos de consulta', 'Fecha de defunción', 'IPRESS', 'Red', 'Estado'
                        ]],
                        style_table={
                            'overflowX': 'auto',
                            'border': 'thin white solid',
                            'fontFamily': 'Calibri',
                            'font-size': '12px',
                            'width': '100%',
                            'height': '100%'
                        },
                        style_cell={
                            'fontFamily': 'Calibri',
                            'height': 'auto',
                            'minWidth': '80px',
                            'width': '80px',
                            'maxWidth': '120px',
                            'whiteSpace': 'normal',
                            'color': '#606060',
                            'font-size': '14px',
                        },
                        style_header={
                            'backgroundColor': '#0064AF',
                            'color': 'white',
                            'fontWeight': 'bold',
                            'textAlign': 'center'
                        },
                        style_data_conditional=[

                        {
                            'if': {'column_id': 'ID'},
                            'textAlign': 'left'
                        },
                        {
                            'if': {'column_id': 'Área'},
                            'textAlign': 'left'
                        },
                        {
                            'if': {'column_id': 'CIE10'},
                            'textAlign': 'left'
                        },
                          {
                            'if': {'column_id': 'Principal motivo de consulta'},
                            'textAlign': 'left'
                        },
                        {
                            'if': {'column_id': 'Otros motivos de consulta'},
                            'textAlign': 'left'
                        },
                          {
                            'if': {'column_id': 'Fecha de defunción'},
                            'textAlign': 'left'
                        },
                        {
                            'if': {'column_id': 'IPRESS'},
                            'textAlign': 'left'
                        },
                        {
                            'if': {'column_id': 'Red'},
                            'textAlign': 'left'
                        },
                        {
                            'if': {'column_id': 'Estado'},
                            'textAlign': 'center'
                        },
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': 'rgb(244, 250, 253)',
                        }
                    ],fixed_rows={'headers': True},
                    sort_action='native',  # Habilitar clasificación
                    )
                ], style={'padding-bottom': '0px', 'margin-bottom': '0px'})
            ], style={'padding-bottom': '0px', 'margin-bottom': '0px'})
        ]),
        style={'border': '1px solid #95D3E9', 'padding': '0px', 'border-radius': '5px','height':'800px','margin-bottom':'45px'}
    ),
    
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # 5 minutos en milisegundos
        n_intervals=0
    )
], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output('table-dengue', 'data'),
        [
            Input('filter-des_red', 'value'),
            Input('filter-des_cas', 'value'),
            Input('filter-des_are', 'value'),
            Input('filter-validado', 'value'),
            Input('filter-id', 'value'),
            Input('filter-año', 'value'),
            Input('interval-component', 'n_intervals')
        ]
    )
    def update_data2(selected_red, selected_cas, selected_are, selected_validado, selected_id, selected_año, n_intervals):
        filtered_df2 = merged_df2

        if selected_red:
            filtered_df2 = filtered_df2[filtered_df2['Red'] == selected_red]
        if selected_cas:
            filtered_df2 = filtered_df2[filtered_df2['IPRESS'] == selected_cas]
        if selected_are:
            filtered_df2 = filtered_df2[filtered_df2['Área'] == selected_are]
        if selected_validado:
            filtered_df2 = filtered_df2[filtered_df2['Estado'] == selected_validado]
        if selected_id:
            filtered_df2 = filtered_df2[filtered_df2['ID'] == selected_id]
        if selected_año:
            filtered_df2 = filtered_df2[filtered_df2['Año'] == selected_año]

        filtered_df2 = filtered_df2.sort_values(by='Fecha de defunción', ascending=False)
        return filtered_df2.to_dict('records')

    @app.callback(
        Output('total-def', 'children'),
        [
            Input('filter-des_red', 'value'),
            Input('filter-des_cas', 'value'),
            Input('filter-des_are', 'value'),
            Input('filter-validado', 'value'),
            Input('filter-id', 'value'),
            Input('filter-año', 'value'),
            Input('interval-component', 'n_intervals')
        ]
    )
    def update_total_def(selected_red, selected_cas, selected_are, selected_validado, selected_id, selected_año, n_intervals):
        filtered_df2 = merged_df2

        if selected_red:
            filtered_df2 = filtered_df2[filtered_df2['Red'] == selected_red]
        if selected_cas:
            filtered_df2 = filtered_df2[filtered_df2['IPRESS'] == selected_cas]
        if selected_are:
            filtered_df2 = filtered_df2[filtered_df2['Área'] == selected_are]
        if selected_validado:
            filtered_df2 = filtered_df2[filtered_df2['Estado'] == selected_validado]
        if selected_id:
            filtered_df2 = filtered_df2[filtered_df2['ID'] == selected_id]
        if selected_año:
            filtered_df2 = filtered_df2[filtered_df2['Año'] == selected_año]

        total_cit_num = filtered_df2['dengue'].sum()
        return f"{total_cit_num}"
    
    @app.callback(
        Output("download-dataframe-csv-dengue", "data"),
        Input("btn_csv", "n_clicks"),
        prevent_initial_call=True,
    )
    def download_as_csv(n_clicks):
        columns_to_include = [
            'ID', 'Área', 'CIE10', 'Principal motivo de consulta', 
            'Otros motivos de consulta', 'Fecha de defunción', 
            'IPRESS', 'Red', 'Estado'
        ]
        
        filtered_df2 = merged_df2[columns_to_include]
        
        return dcc.send_data_frame(filtered_df2.to_csv, "defunciones_dengue.csv", sep=';', index=False)
