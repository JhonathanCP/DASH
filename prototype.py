# %%
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sys
import plotly.graph_objects as go
import json
import dash_bootstrap_components as dbc
from dash import dash_table

# %%
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

# %%
red=5

# %%
def fetch_data2(red):
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
            p.num_sol,
            p.fec_sol,
            p.pac_sec,
            p.act_med_sol
        from public.cext00_essi p
        left outer join dim_red red on p.id_red = red.id_red
        left outer join dim_cas cas on p.id_cas = cas.id_cas
        left outer join dim_areas area on p.id_area = area.id_area
        left outer join dim_activi activi on p.id_acti = activi.id_activi
        left outer join dim_subacti subacti on p.id_subacti = subacti.id_subacti
        left outer join dim_servicios serv on p.id_serv = serv.id_serv

        WHERE p.id_estreg = 2 AND p.fec_sol >= '2024-06-01'
        AND p.id_red = {red}
        """
        result = pd.read_sql(query, conn)
    return result

# %%
def fetch_data3(red):
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
            p.cit_num,
            p.fec_cit,
            p.pac_sec
        from public.cext01_essi p
        left outer join dim_red red on p.id_red = red.id_red
        left outer join dim_cas cas on p.id_cas = cas.id_cas
        left outer join dim_areas area on p.id_area = area.id_area
        left outer join dim_activi activi on p.id_acti = activi.id_activi
        left outer join dim_subacti subacti on p.id_subacti = subacti.id_subacti
        left outer join dim_servicios serv on p.id_serv = serv.id_serv

        WHERE p.fec_cit >= '2024-06-01'
        AND p.id_red = {red}
        """
        result = pd.read_sql(query, conn)
    return result

# %%
df=fetch_data(5)

# %%
df2=fetch_data2(5)

# %%
df3=fetch_data3(5)

# %%
df2

# %%

# Crear el DataFrame agregado con todas las columnas necesarias para los filtros
df_aggregated = df.groupby(['fec_pro','des_red', 'des_cas', 'des_are', 'des_act', 'des_sub', 'des_ser']).agg({
    'num_doc_med': pd.Series.nunique,
    'hor_tot': 'sum'
}).reset_index()

# Renombrar las columnas para mayor claridad
df_aggregated = df_aggregated.rename(columns={'num_doc_med': 'Conteo médicos', 'hor_tot': 'Horas totales'})

# Agregar una columna con el valor de 'Conteo médicos' multiplicado por 150
df_aggregated['Total horas máxima'] = df_aggregated['Conteo médicos'] * 150

# Agregar una columna que divida 'Horas totales' entre 'Total horas máxima' y mostrar como porcentaje
df_aggregated['Ratio horas (%)'] = (df_aggregated['Horas totales'] / df_aggregated['Total horas máxima']) * 100

# Formatear la columna 'Ratio horas (%)' para que aparezca con el símbolo de porcentaje
df_aggregated['Ratio horas (%)'] = df_aggregated['Ratio horas (%)'].apply(lambda x: f"{x:.2f} %")

# %%
# Crear la figura de la tabla
# Seleccionar las columnas específicas que quieres mostrar
columns_to_show = ['des_cas', 'Conteo médicos', 'Horas totales', 'Total horas máxima', 'Ratio horas (%)']

fig = go.Figure(data=[go.Table(
    header=dict(
        values=columns_to_show,
        fill_color='paleturquoise',
        align='left'
    ),
    cells=dict(
        values=[df_aggregated[col] for col in columns_to_show],
        fill_color='lavender',
        align='left'
    )
)])

# Mostrar la tabla
fig.show()

# %%
external_stylesheets = [dbc.themes.BOOTSTRAP]  # Ajusta la ruta según la ubicación de tu archivo custom.css

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Función para obtener las opciones únicas de cada columna
def get_unique_options(df, column_name):
    return [{'label': value, 'value': value} for value in df[column_name].unique()]

# Obtener opciones únicas para los filtros
cas_options = get_unique_options(df, 'des_cas')
are_options = get_unique_options(df, 'des_are')
act_options = get_unique_options(df, 'des_act')
sub_options = get_unique_options(df, 'des_sub')
ser_options = get_unique_options(df, 'des_ser')

app.layout = dbc.Container([
    html.Div(),
    html.H1("Reporte de horas programadas, solicitudes y número de citas en Consulta Externa", style={'color': '#0064AF', 'fontSize':'28px'}),
    html.H2("Fuente: ESSI. Actualizado al 12/07/2024. Actualización diaria. Validado por GSIT - GCTIC", style={'color': '#0064AF', 'fontSize': '12px'}),
    
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id='filter-des_cas',
                options=cas_options,
                placeholder="Selecciona un Cas",
                style={'font-size': '14px', 'height': '40px'},
                maxHeight=150
            ),
        ], width=3),

        dbc.Col([
            dcc.Dropdown(
                id='filter-des_act',
                options=act_options,
                placeholder="Selecciona una Actividad",
                style={'font-size': '14px', 'height': '40px'},
                optionHeight=43,
                maxHeight=150
            ),
        ], width=3),

        dbc.Col([
            dcc.Dropdown(
                id='filter-des_sub',
                options=sub_options,
                placeholder="Selecciona una Subactividad",
                style={'font-size': '14px', 'height': '40px'},
                optionHeight=43,
                maxHeight=150
            ),
        ], width=3),

        dbc.Col([
            dcc.Dropdown(
                id='filter-des_ser',
                options=ser_options,
                placeholder="Selecciona un Servicio",
                style={'font-size': '14px', 'height': '40px'},
                maxHeight=150
            ),
        ], width=3),
    ]),

    dbc.Row([
        dbc.Col([
            html.H6("Fecha de horas programadas", style={'font-size': '16px', 'color': '#606060'}),
            dcc.DatePickerRange(
                id='date-picker-range',
                start_date=df['fec_pro'].min(),
                end_date=df['fec_pro'].max(),
                display_format='DD-MM-YYYY',
                style={'font-size': '10px', 'height': '20px'}
            )
        ], width=3),

        dbc.Col([
            html.H6("Fecha de solicitud", style={'font-size': '16px', 'color': '#606060'}),
            dcc.DatePickerRange(
                id='date-picker-range2',
                start_date=df2['fec_sol'].min(),
                end_date=df2['fec_sol'].max(),
                display_format='DD-MM-YYYY',
                style={'font-size': '10px', 'height': '20px'}
            )
        ], width=3),
    ]),

    html.Br(),  # Insertar un espacio en blanco entre los filtros y las tarjetas

    dbc.Row([
        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H4("Total Horas Programadas", className="card-title", style={'color': '#0064AF', 'fontSize': '16px', 'text-align': 'center'}),
                    html.Div(id='total-hours-text', className="card-text", style={'color': '#0064AF', 'fontSize': '25px', 'text-align': 'center'})
                ]),
                style={'background-color': '#F4FAFD', 'border-color': '#35A2C1'}
            ),
        ], width=2),

        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H4("Número de Solicitudes", className="card-title", style={'color': '#0064AF', 'fontSize': '16px', 'text-align': 'center'}),
                    html.Div(id='total-requests-text', className="card-text", style={'color': '#0064AF', 'fontSize': '25px', 'text-align': 'center'})
                ]),
                style={'background-color': '#F4FAFD', 'border-color': '#35A2C1'}
            ),
        ], width=2),

        dbc.Col([
            dbc.Card(
                dbc.CardBody([
                    html.H4("Número de Citas prog", className="card-title", style={'color': '#0064AF', 'fontSize': '16px', 'text-align': 'center'}),
                    html.Div(id='total-cit-num-text', className="card-text", style={'color': '#0064AF', 'fontSize': '25px', 'text-align': 'center'})
                ]),
                style={'background-color': '#F4FAFD', 'border-color': '#35A2C1'}
            ),
        ], width=2),
    ], justify = "center"),

    html.Br(),  # Insertar un espacio en blanco entre las tarjetas y la tabla
    dbc.Row([
        dbc.Col([
            html.H4("Número de horas programadas", style={'textAlign': 'left', 'color': '#0064AF','font-size': '16px'}),
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in ['des_cas', 'Conteo médicos', 'Horas totales', 'Total horas máxima', 'Ratio horas (%)']],
                style_table={
                    'overflowX': 'auto',
                    'border': 'thin lightgrey solid',
                    'fontFamily': 'Calibri',
                    'font-size': '14px',  # Ajusta el tamaño de la fuente
                    'width': '100%'       # Ajusta el ancho de la tabla
                },
                style_cell={
                    'height': 'auto',
                    'minWidth': '80px',
                    'width': '80px',
                    'maxWidth': '120px',
                    'whiteSpace': 'normal',
                    'color': '#606060',
                    'font-size': '14px',  # Ajusta el tamaño de la fuente de las celdas
                },
                style_header={
                    'backgroundColor': '#0064AF',
                    'color': 'white',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                },
                style_data_conditional=[
                {
                    'if': {'column_id': 'des_cas'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Conteo médicos'},
                    'textAlign': 'center'
                },
                 {
                    'if': {'column_id': 'Horas totales'},
                    'textAlign': 'center'
                },
                   {
                    'if': {'column_id': 'Total horas máxima'},
                    'textAlign': 'center'
                },
                   {
                    'if': {'column_id': 'Ratio horas (%)'},
                    'textAlign': 'center'
                },
            ]
        )
    ], width=7),

        dbc.Col([
            html.H4("Número de solicitudes", style={'textAlign': 'left', 'color': '#0064AF','font-size': '16px'}),
            dash_table.DataTable(
                id='table2',
                columns=[{"name": i, "id": i} for i in ['des_cas', 'Conteo distintivo num_sol', 'Conteo distintivo pac_sec']],
                style_table={
                    'overflowX': 'auto',
                    'border': 'thin lightgrey solid',
                    'fontFamily': 'Calibri',
                    'font-size': '14px',  # Ajusta el tamaño de la fuente
                    'width': '100%'       # Ajusta el ancho de la tabla
                },
                style_cell={
                    'height': 'auto',
                    'minWidth': '80px',
                    'width': '80px',
                    'maxWidth': '120px',
                    'whiteSpace': 'normal',
                    'color': '#606060',
                    'font-size': '14px',  # Ajusta el tamaño de la fuente de las celdas
                },
                style_header={
                    'backgroundColor': '#0064AF',
                    'color': 'white',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                },
                style_data_conditional=[
                {
                    'if': {'column_id': 'des_cas'},
                    'textAlign': 'left'
                },
                {
                    'if': {'column_id': 'Conteo distintivo num_sol'},
                    'textAlign': 'center'
                },
                {
                    'if': {'column_id': 'Conteo distintivo pac_sec'},
                    'textAlign': 'center'
                }
            ]
            )
        ], width=5),
    ])
], fluid=True)
@app.callback(
    Output('total-hours-text', 'children'),
    Output('table', 'data'),
    [
        Input('filter-des_cas', 'value'),
        Input('filter-des_act', 'value'),
        Input('filter-des_sub', 'value'),
        Input('filter-des_ser', 'value'),
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date')
    ]
)
def update_data(selected_cas, selected_act, selected_sub, selected_ser, start_date, end_date):
    filtered_df = df
    
    if selected_cas:
        filtered_df = filtered_df[filtered_df['des_cas'] == selected_cas]
    if selected_act:
        filtered_df = filtered_df[filtered_df['des_act'] == selected_act]
    if selected_sub:
        filtered_df = filtered_df[filtered_df['des_sub'] == selected_sub]
    if selected_ser:
        filtered_df = filtered_df[filtered_df['des_ser'] == selected_ser]
    if start_date and end_date:
        filtered_df = filtered_df[(filtered_df['fec_pro'] >= start_date) & (filtered_df['fec_pro'] <= end_date)]
    
    # Calcula las horas totales
    total_hours = filtered_df['hor_tot'].sum()
    formatted_total_hours = "{:,}".format(total_hours)
    
    # Calcula el DataFrame agregado para la tabla
    grouped_df = filtered_df.groupby(['des_red', 'des_cas']).agg({
        'num_doc_med': pd.Series.nunique,
        'hor_tot': 'sum'
    }).reset_index()
    
    # Renombra las columnas y calcula valores adicionales
    grouped_df = grouped_df.rename(columns={'num_doc_med': 'Conteo médicos', 'hor_tot': 'Horas totales'})
    grouped_df['Total horas máxima'] = grouped_df['Conteo médicos'] * 150
    grouped_df['Ratio horas (%)'] = (grouped_df['Horas totales'] / grouped_df['Total horas máxima']) * 100
    
    # Ordena el DataFrame por la columna 'Ratio horas (%)'
    grouped_df = grouped_df.sort_values(by='Ratio horas (%)', ascending=True)
    
    # Formatea la columna 'Ratio horas (%)'
    grouped_df['Ratio horas (%)'] = grouped_df['Ratio horas (%)'].apply(lambda x: f"{x:.2f} %")
    
    # Formatea los valores numéricos con separadores de miles
    grouped_df['Conteo médicos'] = grouped_df['Conteo médicos'].apply(lambda x: "{:,}".format(x))
    grouped_df['Horas totales'] = grouped_df['Horas totales'].apply(lambda x: "{:,}".format(x))
    grouped_df['Total horas máxima'] = grouped_df['Total horas máxima'].apply(lambda x: "{:,}".format(x))
    
    table_data = grouped_df.to_dict('records')
    
    return f"{formatted_total_hours}", table_data

@app.callback(
    Output('total-requests-text', 'children'),
    [
        Input('filter-des_cas', 'value'),
        Input('filter-des_act', 'value'),
        Input('filter-des_sub', 'value'),
        Input('filter-des_ser', 'value')
    ]
)
def update_total_requests_text(selected_cas,selected_act, selected_sub, selected_ser):
    filtered_df2 = df2
    
    if selected_cas:
        filtered_df2 = filtered_df2[filtered_df2['des_cas'] == selected_cas]
    if selected_act:
        filtered_df2 = filtered_df2[filtered_df2['des_act'] == selected_act]
    if selected_sub:
        filtered_df2 = filtered_df2[filtered_df2['des_sub'] == selected_sub]
    if selected_ser:
        filtered_df2 = filtered_df2[filtered_df2['des_ser'] == selected_ser]
    
    # Calcula el conteo distintivo de solicitudes
    total_requests = filtered_df2['num_sol'].nunique()
    formatted_total_requests = "{:,}".format(total_requests)
    return f"{formatted_total_requests}"

@app.callback(
    Output('total-cit-num-text', 'children'),
    [
        Input('filter-des_cas', 'value'),
        Input('filter-des_act', 'value'),
        Input('filter-des_sub', 'value'),
        Input('filter-des_ser', 'value')
    ]
)
def update_total_cit_num_text(selected_cas, selected_act, selected_sub, selected_ser):
    filtered_df3 = df3
    
    if selected_cas:
        filtered_df3 = filtered_df3[filtered_df3['des_cas'] == selected_cas]
    if selected_act:
        filtered_df3 = filtered_df3[filtered_df3['des_act'] == selected_act]
    if selected_sub:
        filtered_df3 = filtered_df3[filtered_df3['des_sub'] == selected_sub]
    if selected_ser:
        filtered_df3 = filtered_df3[filtered_df3['des_ser'] == selected_ser]
    
    # Calcula el conteo distintivo de citas
    total_cit_num = filtered_df3['cit_num'].nunique()
    formatted_total_cit_num = "{:,}".format(total_cit_num)
    return f"{formatted_total_cit_num}"
@app.callback(
    Output('table2', 'data'),
    [
        Input('filter-des_cas', 'value'),
        Input('filter-des_act', 'value'),
        Input('filter-des_sub', 'value'),
        Input('filter-des_ser', 'value'),
        Input('date-picker-range2', 'start_date'),
        Input('date-picker-range2', 'end_date')

    ]
)
def update_data2(selected_cas, selected_act, selected_sub, selected_ser, start_date, end_date):
    filtered_df2 = df2
    
    if selected_cas:
        filtered_df2 = filtered_df2[filtered_df2['des_cas'] == selected_cas]
    if selected_act:
        filtered_df2 = filtered_df2[filtered_df2['des_act'] == selected_act]
    if selected_sub:
        filtered_df2 = filtered_df2[filtered_df2['des_sub'] == selected_sub]
    if selected_ser:
        filtered_df2 = filtered_df2[filtered_df2['des_ser'] == selected_ser]
    if start_date and end_date:
        filtered_df2 = filtered_df2[(filtered_df2['fec_sol'] >= start_date) & (filtered_df2['fec_sol'] <= end_date)]
    
    # Agrupa y cuenta valores distintivos
    grouped_df2 = filtered_df2.groupby(['des_cas']).agg({
        'num_sol': pd.Series.nunique,
        'pac_sec': pd.Series.nunique
    }).reset_index()
    
    # Renombra las columnas
    grouped_df2 = grouped_df2.rename(columns={'num_sol': 'Conteo distintivo num_sol', 'pac_sec': 'Conteo distintivo pac_sec'})
    
    # Ordena por el conteo distintivo de num_sol de mayor a menor
    grouped_df2 = grouped_df2.sort_values(by='Conteo distintivo num_sol', ascending=True)
    
    # Formatea los valores numéricos con separadores de miles
    grouped_df2['Conteo distintivo num_sol'] = grouped_df2['Conteo distintivo num_sol'].apply(lambda x: "{:,}".format(x))
    grouped_df2['Conteo distintivo pac_sec'] = grouped_df2['Conteo distintivo pac_sec'].apply(lambda x: "{:,}".format(x))
    
    table_data2 = grouped_df2.to_dict('records')
    
    return table_data2
# Ejecutar la aplicación
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)


