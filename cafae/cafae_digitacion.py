import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
from sqlalchemy import create_engine
import dash_bootstrap_components as dbc
from dash import dash_table

def create_connection():
    return create_engine('postgresql://postgres:SDIBD2023$@10.0.1.230:5433/dbcafae')

def fetch_data():
    engine = create_connection()
    with engine.connect() as conn:
        query = "SELECT * FROM cab_acta"
        result = pd.read_sql(query, conn)
    engine.dispose()  # Cierra la conexión
    return result

def fetch_data2():
    engine = create_connection()
    with engine.connect() as conn:
        query = "SELECT * FROM eleccion"
        result = pd.read_sql(query, conn)
    engine.dispose()  # Cierra la conexión
    return result

def fetch_data3():
    engine = create_connection()
    with engine.connect() as conn:
        query = "SELECT * FROM ubigeo"
        result = pd.read_sql(query, conn)
    engine.dispose()  # Cierra la conexión
    return result

def fetch_data4():
    engine = create_connection()
    with engine.connect() as conn:
        query = "SELECT * FROM ubieleccion"
        result = pd.read_sql(query, conn)
    engine.dispose()  # Cierra la conexión
    return result

def fetch_and_process_data():
    cab_acta = fetch_data()
    eleccion = fetch_data2()
    ubigeo = fetch_data3()
    ubieleccion = fetch_data4()

    eleccion = eleccion.rename(columns={'n_cod_pk': 'n_eleccion_fk'})
    ubigeo = ubigeo.rename(columns={'n_cod_pk': 'n_ubigeo_fk'})
    merge1 = pd.merge(ubieleccion, eleccion[['n_eleccion_fk', 'c_descripcion']], on='n_eleccion_fk', how='left')
    dim = pd.merge(merge1, ubigeo[['n_ubigeo_fk', 'c_desc_ubigeo']], on='n_ubigeo_fk', how='left')
    dim = dim.rename(columns={'n_cod_pk': 'n_ubigeo_fk', 'n_ubigeo_fk': 'ubigeo'})
    tab_cab_acta = pd.merge(cab_acta, dim[['n_ubigeo_fk', 'c_descripcion', 'c_desc_ubigeo']], how='left', on='n_ubigeo_fk')
    tab_cab_acta['c_estado_digtal'] = tab_cab_acta['c_estado_digtal'].astype(int)

    tab_cab_acta['DIGITALIZACION'] = tab_cab_acta.apply(aplicar_condiciones, axis=1)
    tab_cab_acta['DIGITACION'] = tab_cab_acta.apply(aplicar_condiciones2, axis=1)
    tab_cab_acta['VERIFICACION'] = tab_cab_acta.apply(aplicar_condiciones3, axis=1)

    df_pivot_table = tab_cab_acta.pivot_table(index=['c_desc_ubigeo', 'c_numero', 'c_descripcion'], columns='DIGITACION', aggfunc='size', fill_value=0)
    df_pivot_table_reset = df_pivot_table.reset_index()

    df_pivot_table_reset = df_pivot_table_reset.rename(columns={'c_desc_ubigeo': 'RED', 'c_descripcion': 'TIPO DE ELECCION', 'c_numero': 'NÚMERO DE ACTA'})
    
    return df_pivot_table_reset

def aplicar_condiciones(row):
    if row['c_estado_digtal'] == 0:
        return 'NO TIENE IMAGEN'
    elif row['c_estado_digtal'] == 1:
        return 'IMAGEN REGISTRADA'
    elif row['c_estado_digtal'] == 2:
        return 'IMAGEN OBSERVADA'
    elif row['c_estado_digtal'] == 3:
        return 'IMAGEN REGISTRADA'
    elif row['c_estado_digtal'] == 4:
        return 'IMAGEN REGISTRADA'
    elif row['c_estado_digtal'] == 5:
        return 'IMAGEN REGISTRADA'
    
def aplicar_condiciones2(row):
    if row['c_estado_digtal'] == 0:
        return 'NO LO VE'
    elif row['c_estado_digtal'] == 1:
        return 'PENDIENTE DE DIGITACION'
    elif row['c_estado_digtal'] == 2:
        return 'IMAGEN OBSERVADA'
    elif row['c_estado_digtal'] == 3:
        return 'COMPLETADA'
    elif row['c_estado_digtal'] == 4:
        return 'OBSERVADA'
    elif row['c_estado_digtal'] == 5:
        return 'COMPLETADA'

def aplicar_condiciones3(row):
    if row['c_estado_digtal'] == 3:
        return 'COMPLETADA'
    elif row['c_estado_digtal'] == 4:
        return 'OBSERVADA'
    elif row['c_estado_digtal'] == 5:
        return 'COMPLETADA'
    else:
        return ""

# Layout de la aplicación Dash
layout = dbc.Container([
    html.Div(style={'height': '12px'}),

    html.H1("Elecciones CAFAE 2024: Digitación de actas", style={'color': '#0064AF', 'fontSize': '28px'}),
    # html.H2("Fuente: CAFAE. Actualizado al 09/08/2024. Actualización diaria. Pendiente de validar. V.1.0.0", style={'color': '#0064AF', 'fontSize': '12px'}),
    html.Hr(style={'border': '1px solid #0064AF'}),

    dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("RED", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Dropdown(
                        id='filter-red-digitacion',
                        options=[],  # Las opciones se llenarán en el callback
                        placeholder="Selecciona una RED",
                        style={'font-size': '14px', 'height': '40px'},
                        maxHeight=150,
                        optionHeight=45
                    ),
                ], width=4),

                dbc.Col([
                    html.Label("Tipo de elección", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Dropdown(
                        id='filter-tipo-eleccion-digitacion',
                        options=[],  # Las opciones se llenarán en el callback
                        placeholder="Selecciona un tipo de elección",
                        style={'font-size': '14px', 'height': '40px'},
                        maxHeight=150,
                        optionHeight=45
                    ),
                ], width=4),

                dbc.Col([
                    html.Label("Número de acta", style={'font-size': '16px', 'color': '#0064AF'}),
                    dcc.Input(
                        id='filter-numero_acta',
                        type='text',
                        placeholder="Buscar número de acta",
                        style={'font-size': '14px', 'height': '40px', 'width': '100%'}
                    ),
                ], width=4),
            ]),

            html.Br(),

            dbc.Row([
                dbc.Col([
                    dash_table.DataTable(
                        id='table-digitacion',
                        columns=[{"name": i, "id": i} for i in [
                            'RED', 'NÚMERO DE ACTA','PENDIENTE DE DIGITACION', 'IMAGEN OBSERVADA','COMPLETADA'
                        ]],
                        style_table={
                            'overflowX': 'auto',
                            'border': 'thin lightgrey solid',
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
                            {'if': {'column_id': 'RED'}, 'textAlign': 'left'},
                            {'if': {'column_id': 'NÚMERO DE ACTA'}, 'textAlign': 'left'},
                            {'if': {'column_id': 'PENDIENTE DE DIGITACION'}, 'textAlign': 'center'},
                            {'if': {'column_id': 'IMAGEN OBSERVADA'}, 'textAlign': 'center'},
                            {'if': {'column_id': 'COMPLETADA'}, 'textAlign': 'center'},
                        ],
                        fixed_rows={'headers': True},
                        sort_action='native',
                    )
                ], style={'padding-bottom': '0px', 'margin-bottom': '0px'})
            ], style={'padding-bottom': '0px', 'margin-bottom': '0px'})
        ]),
        style={'border': '1px solid #95D3E9', 'padding': '0px', 'border-radius': '5px', 'height': '700px', 'margin-bottom': '45px'}
    ),

    # Botón para descargar CSV
    html.Button("Descargar CSV", id="btn_csv", n_clicks=0, className="btn btn-primary"),
    dcc.Download(id="download-dataframe-csv-digitacion"),

], fluid=True)


def register_callbacks(app):
    @app.callback(
        Output('filter-red-digitacion', 'options'),
        Output('filter-tipo-eleccion-digitacion', 'options'),
        Output('table-digitacion', 'data'),
        [
            Input('filter-red-digitacion', 'value'),
            Input('filter-tipo-eleccion-digitacion', 'value'),
            Input('filter-numero_acta', 'value')
        ]
    )
    def update_data(selected_red, selected_tipo_eleccion, search_numero_acta):
        df_pivot_table_reset = fetch_and_process_data()
        
        RED_DIGITACION = [{'label': value, 'value': value} for value in df_pivot_table_reset['RED'].unique()]
        Tipo_eleccion_digitacion = [{'label': value, 'value': value} for value in df_pivot_table_reset['TIPO DE ELECCION'].unique()]

        filtered_df_digitacion = df_pivot_table_reset
        
        if selected_red:
            filtered_df_digitacion = filtered_df_digitacion[filtered_df_digitacion['RED'] == selected_red]
        if selected_tipo_eleccion:
            filtered_df_digitacion = filtered_df_digitacion[filtered_df_digitacion['TIPO DE ELECCION'] == selected_tipo_eleccion]
        if search_numero_acta:
            filtered_df_digitacion = filtered_df_digitacion[filtered_df_digitacion['NÚMERO DE ACTA'].astype(str).str.contains(search_numero_acta)]

        return RED_DIGITACION, Tipo_eleccion_digitacion, filtered_df_digitacion.to_dict('records')

    @app.callback(
        Output("download-dataframe-csv-digitacion", "data"),
        Input("btn_csv", "n_clicks"),
        State('filter-red-digitacion', 'value'),
        State('filter-tipo-eleccion-digitacion', 'value'),
        State('filter-numero_acta', 'value'),
        prevent_initial_call=True,
    )
    def download_as_csv(n_clicks, selected_red, selected_tipo_eleccion, search_numero_acta):
        df_pivot_table_reset = fetch_and_process_data()
        
        filtered_df = df_pivot_table_reset
        
        if selected_red:
            filtered_df = filtered_df[filtered_df['RED'] == selected_red]
        if selected_tipo_eleccion:
            filtered_df = filtered_df[filtered_df['TIPO DE ELECCION'] == selected_tipo_eleccion]
        if search_numero_acta:
            filtered_df = filtered_df[filtered_df['NÚMERO DE ACTA'].astype(str).str.contains(search_numero_acta)]
        
        return dcc.send_data_frame(filtered_df.to_csv, "actas_report.csv", sep=';', index=False)

