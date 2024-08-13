from dash import Dash, html, dcc, Input, Output, callback, State, dash_table
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine
import dash_bootstrap_components as dbc

def create_connection():
    return create_engine('postgresql://postgres:AKindOfMagic@10.0.1.228:5432/INDICADORES_ESSALUD')

def fetch_data(red):
    engine = create_connection()
    with engine.connect() as conn:
        query = f"""
        SELECT	
            red.des_red, cas.des_cas, area.des_are, activi.des_act, subacti.des_sub, serv.des_ser,
            p.fec_cre, p.fec_pro, p.hor_tur_ini, p.hor_tur_fin, tdi.des_tdo, p.num_doc_med,
            p.hor_tot, tpo.des_tipprog, thp.des_tiphorprog, epc.des_estprogcit, ms.des_motsus
        from prog00_essi p
        left join dim_red red on p.id_red = red.id_red
        left join dim_cas cas on p.id_cas = cas.id_cas
        left join dim_areas area on p.id_area = area.id_area
        left join dim_activi activi on p.id_acti = activi.id_activi
        left join dim_subacti subacti on p.id_subacti = subacti.id_subacti
        left join dim_servicios serv on p.id_serv = serv.id_serv
        left join dim_tipdoc tdi on p.id_tdi_med = tdi.id_tipdoc
        left join dim_tipoprog tpo on p.id_tipoprog = tpo.id_tipoprog
        left join dim_tipohorprog thp on p.id_tipohorprog = thp.id_tipohorprog
        left join dim_estprogcit epc on p.id_estprogcit = epc.id_estprogcit
        left join dim_motsuspro ms on p.id_motsuspro = ms.id_motsuspro
        WHERE p.id_estreg = 2 AND DATE(p.fec_pro) >= CURRENT_DATE AND DATE(p.fec_pro) < CURRENT_DATE + INTERVAL '2.5 months'
        AND p.id_red = {red}
        """
        return pd.read_sql(query, conn)

def layout(red=None):
    data = fetch_data(red) if red is not None else pd.DataFrame()

    df_aggregated = data.groupby(['des_red', 'des_cas']).agg({
        'num_doc_med': pd.Series.nunique,
        'hor_tot': 'sum'
    }).reset_index()

    df_aggregated.rename(columns={'num_doc_med': 'Conteo médicos', 'hor_tot': 'Horas totales'}, inplace=True)
    df_aggregated['Total horas máxima'] = df_aggregated['Conteo médicos'] * 150
    df_aggregated['Ratio horas (%)'] = (df_aggregated['Horas totales'] / df_aggregated['Total horas máxima'] * 100).map("{:.2f} %".format)

    return html.Div([
        html.H1("Visualización de Horas Programadas"),
        dcc.Store(id='stored-data', data=df_aggregated.to_dict('records')),
        dcc.Dropdown(id='filter-des_cas', options=[{'label': x, 'value': x} for x in df_aggregated['des_cas'].unique()], placeholder="Seleccione un Caso"),
        html.Div(id='total-hours-text'),
        dash_table.DataTable(id='table', columns=[{"name": i, "id": i} for i in df_aggregated.columns], data=df_aggregated.to_dict('records'))
    ])

@callback(
    Output('total-hours-text', 'children'),
    Input('filter-des_cas', 'value'),
    State('stored-data', 'data')
)
def update_total_hours_text(selected_cas, stored_data):
    if not stored_data:
        return "No data available."
    df = pd.DataFrame(stored_data)
    if selected_cas:
        df = df[df['des_cas'] == selected_cas]
    total_hours = df['Horas totales'].sum()
    return f"Total Horas Programadas: {total_hours}"

@callback(
    Output('table', 'data'),
    Input('filter-des_cas', 'value'),
    State('stored-data', 'data')
)
def update_table(selected_cas, stored_data):
    if not stored_data:
        return []
    df = pd.DataFrame(stored_data)
    if selected_cas:
        df = df[df['des_cas'] == selected_cas]
    return df.to_dict('records')
