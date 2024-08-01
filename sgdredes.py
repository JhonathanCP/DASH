from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine
import dash_bootstrap_components as dbc
from dash import dash_table

def create_connection():
    try:
        engine = create_engine('postgresql://postgres:SdDd3v@10.0.0.131:5433/sgd')
        with engine.connect() as conn:
            print("Connection to the database was successful!")
        return engine
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        return None

def fetch_data(table_name, limit=None):
    engine = create_connection()
    if engine is not None:
        try:
            with engine.connect() as conn:
                query = f"SELECT * FROM {table_name}"
                if limit:
                    query += f" LIMIT {limit}"
                result = pd.read_sql(query, conn)
                print(f"Data fetched successfully from {table_name}!")
                return result
        except Exception as e:
            print(f"Failed to fetch data from {table_name}: {e}")
            return None
    else:
        return None

# Fetch all necessary data
rhtm_dependencia_or = fetch_data("idosgd.rhtm_dependencia")
rhtm_dependencia_dest = fetch_data("idosgd.rhtm_dependencia")
tdtv_destinos = fetch_data("idosgd.tdtv_destinos")
tdtv_remitos = fetch_data("idosgd.tdtv_remitos")
tdtc_expediente = fetch_data("idosgd.tdtc_expediente")
tdtr_otro_origen = fetch_data("idosgd.tdtr_otro_origen")
lg_pro_proveedor = fetch_data("idosgd.lg_pro_proveedor")
si_mae_tipo_doc = fetch_data("idosgd.si_mae_tipo_doc")
tdtr_grupo_documento = fetch_data("idosgd.tdtr_grupo_documento")
si_redes = fetch_data("idosgd.si_redes")
tdtr_motivo = fetch_data("idosgd.tdtr_motivo")
tdtx_ani_simil_limit = fetch_data("idosgd.tdtx_ani_simil", limit=1000000)

# Merge and clean data
merged_df1 = pd.merge(rhtm_dependencia_dest, si_redes[['co_red', 'des_red']], on='co_red', how='left')
merged_df1 = merged_df1.rename(columns={'co_dependencia':'co_dep_des'})
merged_df2 = pd.merge(tdtv_destinos, merged_df1[['co_dep_des','de_dependencia','des_red']], on='co_dep_des', how='left')
destinosf = pd.merge(merged_df2, tdtr_motivo[['co_mot','de_mot']], on='co_mot', how='left')
tdtv_remitos['ID_EXPEDIENTE'] = tdtv_remitos['nu_ann_exp'] + tdtv_remitos['nu_sec_exp']
tdtc_expediente['ID_EXPEDIENTE'] = tdtc_expediente['nu_ann_exp'] + tdtc_expediente['nu_sec_exp']
tdtc_expediente = tdtc_expediente.rename(columns={'co_gru':'co_gru2'})
merged_df3 = pd.merge(tdtv_remitos, tdtc_expediente[['ID_EXPEDIENTE', 'nu_expediente','co_gru2']], on='ID_EXPEDIENTE', how='left')
tdtx_ani_simil_limit = tdtx_ani_simil_limit.rename(columns={'nulem':'nu_dni_emi'})
merged_df4 = pd.merge(merged_df3, tdtx_ani_simil_limit[['nu_dni_emi','denom','deapp']], on='nu_dni_emi', how='left')
tdtr_otro_origen = tdtr_otro_origen.rename(columns={'co_otr_ori':'co_otr_ori_emi'})
merged_df5 = pd.merge(merged_df4, tdtr_otro_origen[['co_otr_ori_emi','de_raz_soc_otr','de_nom_otr','de_ape_pat_otr']], on='co_otr_ori_emi', how='left')
lg_pro_proveedor = lg_pro_proveedor.rename(columns={'cpro_ruc':'nu_ruc_emi'})
merged_df6 = pd.merge(merged_df5, lg_pro_proveedor[['nu_ruc_emi', 'cpro_razsoc']], on='nu_ruc_emi', how='left')
si_mae_tipo_doc = si_mae_tipo_doc.rename(columns={'cdoc_tipdoc':'co_tip_doc_adm'})
merged_df7 = pd.merge(merged_df6, si_mae_tipo_doc[['co_tip_doc_adm','cdoc_desdoc']], on='co_tip_doc_adm', how='left')
merged_df8 = pd.merge(merged_df7, tdtr_grupo_documento, on='co_gru', how='left')
rhtm_dependencia_or = rhtm_dependencia_or.rename(columns={'co_dependencia':'co_dep_emi'})
remitosf = pd.merge(merged_df8, rhtm_dependencia_or[['co_dep_emi', 'de_dependencia']].rename(columns={'de_dependencia': 'de_dependencia_or'}), on='co_dep_emi', how='left')

destinosf['ID_REMITOS'] = destinosf['nu_ann'] + destinosf['nu_emi']
remitosf['ID_REMITOS'] = remitosf['nu_ann'].astype(str) + remitosf['nu_emi'].astype(str)

remitosf['Razón Social'] = (
    remitosf['cpro_razsoc'].fillna('') + " " +
    remitosf['denom'].fillna('') + " " +
    remitosf['deapp'].fillna('') + " " +
    remitosf['de_nom_otr'].fillna('') + " " +
    remitosf['de_ape_pat_otr'].fillna('')
)

TablaFin = pd.merge(destinosf, remitosf, on='ID_REMITOS', how='left')

max_fecha = TablaFin.groupby('ID_REMITOS')['fe_emi'].transform('max')
TablaFin = TablaFin[TablaFin['fe_emi'] == max_fecha]
TablaFin_filtrada = TablaFin[TablaFin['co_gru2'] == '3']
TablaFin = TablaFin_filtrada

def get_unique_options(TablaFin, column_name):
    return [{'label': value, 'value': value} for value in TablaFin[column_name].unique()]

expedientes_options = get_unique_options(TablaFin, 'nu_expediente')

TablaFin = TablaFin.rename(columns={
    'nu_expediente': 'N° Expediente',
    'cdoc_desdoc': 'Clase de documento',
    'de_asu': 'Asunto',
    'fe_emi': 'Fecha de envío',
    'de_dependencia_or': 'Origen',
    'fe_rec_doc': 'Fecha de aceptación',
    'de_dependencia': 'Destino',
    'des_red': 'Red',
})

layout = dbc.Container([
    html.Div([
        html.Div(style={'position': 'relative', 'width': '100%'}),
        html.Img(src='/assets/encabezado.png', style={'width': '100%'}),
        html.Div([
            html.H1("Seguimiento del trámite", style={'color': '#FFFFFF', 'fontSize': '28px', 'textAlign': 'center'}),
        ], style={'position': 'absolute', 'top': '5%', 'left': '50%', 'transform': 'translate(-50%, -50%)', 'width': '100%'}),
        html.Div(style={'height': '12px'}),

        dbc.Row([
            dbc.Col([
                html.H6("# Expediente", style={'font-size': '16px', 'color': '#606060', 'fontWeight': 'normal'}),
                dcc.Input(id='filter-expediente', type='text', placeholder='Buscar...', style={'margin-bottom': '5px', 'width': '100%'}),
                html.P(
                    '(*) Para realizar la consulta ingrese el número de expediente',
                    style={'font-size': '12px', 'color': '#0064AF', 'margin-bottom': '20px'}
                ),
            ], width=12)
        ], style={'width': '100%', 'margin': '0'}),

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
        ], style={'width': '100%', 'margin': '0'}),

        dbc.Row([
            dbc.Col([
                dbc.Card(
                    dbc.CardBody([
                        html.P(id="asunto", className="card-text", style={'font-size': '18px', 'color': '#606060'}),
                    ]),
                    style={"margin-top": "0px", "padding": "0px", "border": "none", "text-align":"center",'font-weight': 'bold','background-color': '#F4FAFD'}
                )
            ], width=12)
        ], style={'width': '100%', 'margin': '0'}),

        dbc.Row([
            dbc.Col([
                html.Div(id='table-container')
            ], width=12)
        ], style={'width': '100%', 'margin': '0'})
    ])
], fluid=True)

def register_callbacks(app):
    @app.callback(
        [Output('table-container', 'children'),
         Output('razon-social', 'children'),
         Output('min-fecha', 'children'),
         Output('tipdoc', 'children'),
         Output('asunto', 'children')],
        [Input('filter-expediente', 'value')]
    )
    def update_table_and_card(search_value):
        if not search_value:
            return html.Div(), "", "", "", "Ingrese el N° de expediente"

        filtered_data = TablaFin[TablaFin['N° Expediente'].str.contains(search_value, case=False, na=False)]
        filtered_data_sorted = filtered_data.sort_values(by='Fecha de envío', ascending=True)
        last_5_data = filtered_data_sorted.tail(5)

        if last_5_data.empty:
            return html.Div(), "", "", "", "Ingrese el N° de expediente"

        min_fecha = filtered_data_sorted['Fecha de envío'].min()
        razon_social_min_fecha = filtered_data_sorted[filtered_data_sorted['Fecha de envío'] == min_fecha]['Razón Social'].values[0]
        tipdoc_min_fecha = filtered_data_sorted[filtered_data_sorted['Fecha de envío'] == min_fecha]['Clase de documento'].values[0]
        asunto_min_fecha = filtered_data_sorted[filtered_data_sorted['Fecha de envío'] == min_fecha]['Asunto'].values[0]

        table = dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i} for i in ['N° Expediente', 'Clase de documento', 'Asunto', 'Fecha de envío', 'Origen', 'Fecha de aceptación', 'Destino', 'Red']],
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
                'minWidth': '80px',
                'width': '80px',
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
        )

        return table, f"Razón Social: {razon_social_min_fecha}", f"Fecha de Envío: {min_fecha.strftime('%Y-%m-%d')}", f"Tipo de documento: {tipdoc_min_fecha}", asunto_min_fecha
