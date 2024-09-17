import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from flask import Flask, render_template, jsonify, request
from urllib.parse import parse_qs

#from sgdredes.data_provider import get_data

# Inicializar el servidor Flask y la aplicación Dash
server = Flask(__name__)
app = dash.Dash(__name__, suppress_callback_exceptions=True,
                server=server, external_stylesheets=[dbc.themes.BOOTSTRAP,
    'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css',  # Bootstrap CSS
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.1/css/all.min.css']
)
app.title = "SGD Redes - Seguimiento trámite"

# Importar la función para crear la ruta de exportación de CSV
from miconsulta import create_csv_export_route

# Registrar la ruta de exportación de CSV
create_csv_export_route(server)

# Establecer el layout principal de la aplicación
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content', style={"margin": "0", "padding": "0"})
])

# Callback para manejar el contenido basado en la ruta
# Callback para manejar el contenido basado en la ruta y parámetros
@app.callback(Output('page-content', 'children'),
            [Input('url', 'pathname'),
            Input('url', 'search')])

def display_page(pathname, search):
    # Parsear los parámetros de la URL
    
    # if pathname == '/dengue/detalle':
    #     from dengue import layout as reporte2_layout, register_callbacks as register_callbacks_reporte2
    #     if not hasattr(app, 'reporte2_callbacks_registered'):
    #         register_callbacks_reporte2(app)
    #         app.reporte2_callbacks_registered = True
    #     return reporte2_layout
    
    if pathname == '/':
        from sgdredes.sgdredesconsulta import layout as sgd_redes_layout, register_callbacks as register_callbacks_sgdredes
        if not hasattr(app, 'sgdredes_callbacks_registered'):
            register_callbacks_sgdredes(app)
            app.sgdredes_callbacks_registered = True

        # Pasar el parámetro `codigo` al layout de sgdredes
        params = parse_qs(search[1:])
        codigo = params.get('codigo', [None])[0]  # Obtiene el valor del parámetro 'codigo'
        return sgd_redes_layout(codigo)  # Pasar el código al layout

# Servidor
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
