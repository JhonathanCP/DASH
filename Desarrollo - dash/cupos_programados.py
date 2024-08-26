import pandas as pd
from sqlalchemy import create_engine
from dash import html, dcc
import plotly.express as px  # Importar Plotly Express para la visualización
from dash import Dash, html, dcc, Input, Output, callback, State

def create_connection():
    return create_engine('postgresql://postgres:AKindOfMagic@10.0.1.228:5432/INDICADORES_ESSALUD')

def fetch_data(red):
    engine = create_connection()
    with engine.connect() as conn:
        query = f"""
        SELECT	
            red.des_red,
            cas.des_cas,
            area.des_are,
            activi.des_act,
            subacti.des_sub,
            serv.des_ser,
            p.fec_cre,
            p.fec_pro,
            p.hor_tur_ini,
            p.hor_tur_fin,
            tdi.des_tdo,
            p.num_doc_med,
            p.hor_tot,
            tpo.des_tipprog,
            thp.des_tiphorprog,
            epc.des_estprogcit,
            ms.des_motsus
        from prog00_essi p
        left outer join dim_red red on p.id_red = red.id_red
        left outer join dim_cas cas on p.id_cas = cas.id_cas
        left outer join dim_areas area on p.id_area = area.id_area
        left outer join dim_activi activi on p.id_acti = activi.id_activi
        left outer join dim_subacti subacti on p.id_subacti = subacti.id_subacti
        left outer join dim_servicios serv on p.id_serv = serv.id_serv
        left outer join dim_tipdoc tdi on p.id_tdi_med = tdi.id_tipdoc
        left outer join dim_tipoprog tpo on p.id_tipoprog = tpo.id_tipoprog
        left outer join dim_tipohorprog thp on p.id_tipohorprog = thp.id_tipohorprog
        left outer join dim_estprogcit epc on p.id_estprogcit = epc.id_estprogcit
        left outer join dim_motsuspro ms on p.id_motsuspro = ms.id_motsuspro
        WHERE p.id_estreg = 2 AND DATE(p.fec_pro) >= CURRENT_DATE AND DATE(p.fec_pro) < CURRENT_DATE + INTERVAL '2.5 months'
        AND p.id_red = {red}
        """
        result = pd.read_sql(query, conn)
    return result


def layout(red=None):
    data = fetch_data(red) if red is not None else pd.DataFrame()
    return html.Div([
        html.H1('Datos de programación asistencial'),
        dcc.Dropdown(
            id='cas-dropdown',
            options=[{'label': i, 'value': i} for i in data['des_cas'].dropna().unique()],
            placeholder='Seleccione una IPRESS',
            multi=True
        ),
        dcc.Dropdown(
            id='are-dropdown',
            options=[{'label': i, 'value': i} for i in data['des_are'].dropna().unique()],
            placeholder='Seleccione un Área',
            multi=True
        ),
        dcc.Dropdown(
            id='act-dropdown',
            options=[{'label': i, 'value': i} for i in data['des_act'].dropna().unique()],
            placeholder='Seleccione una Actividad',
            multi=True
        ),
        dcc.Dropdown(
            id='sub-dropdown',
            options=[{'label': i, 'value': i} for i in data['des_sub'].dropna().unique()],
            placeholder='Seleccione una Subactividad',
            multi=True
        ),
        dcc.Dropdown(
            id='ser-dropdown',
            options=[{'label': i, 'value': i} for i in data['des_ser'].dropna().unique()],
            placeholder='Seleccione un Servicio',
            multi=True
        ),
        dcc.Store(id='stored-data', data=data.to_dict('records')),
        dcc.Graph(id='horas-chart')
    ])

@callback(
    Output('horas-chart', 'figure'),
    [Input('cas-dropdown', 'value'),
    Input('are-dropdown', 'value'),
    Input('act-dropdown', 'value'),
    Input('sub-dropdown', 'value'),
    Input('ser-dropdown', 'value')],
    [State('stored-data', 'data')],
    prevent_initial_call=False
)
def update_graph(selected_cas, selected_are, selected_act, selected_sub, selected_ser, stored_data):
    if stored_data:
        filtered_data = pd.DataFrame(stored_data)
        # Apply each filter if it has been set
        if selected_cas:
            filtered_data = filtered_data[filtered_data['des_cas'].isin(selected_cas)]
        if selected_are:
            filtered_data = filtered_data[filtered_data['des_are'].isin(selected_are)]
        if selected_act:
            filtered_data = filtered_data[filtered_data['des_act'].isin(selected_act)]
        if selected_sub:
            filtered_data = filtered_data[filtered_data['des_sub'].isin(selected_sub)]
        if selected_ser:
            filtered_data = filtered_data[filtered_data['des_ser'].isin(selected_ser)]

        grouped_data = filtered_data.groupby('fec_pro')['hor_tot'].sum().reset_index()
        fig = px.line(grouped_data, x='fec_pro', y='hor_tot',
            labels={'fec_pro': 'Fecha de programación', 'hor_tot': 'Horas Totales'},
            title='Total de Horas por Fecha de programación')
        return fig
    return px.line(title="No data available")  # Return an empty plot if no data is available
