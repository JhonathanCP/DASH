import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from flask import Flask

# Inicializar el servidor Flask y la aplicación Dash
server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Proyecto Dash"

# Importar la función para crear la ruta de exportación de CSV
from miconsulta import create_csv_export_route


# Registrar la ruta de exportación de CSV
create_csv_export_route(server)

# Establecer el layout principal de la aplicación
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Callback para manejar el contenido basado en la ruta
@app.callback(Output('page-content', 'children'), 
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/sgdredes':
        from sgdredes import layout as sgdredes_layout, register_callbacks as register_callbacks_sgdredes
        if not hasattr(app, 'sgdredes_callbacks_registered'):
            register_callbacks_sgdredes(app)
            app.sgdredes_callbacks_registered = True
        return sgdredes_layout
    elif pathname == '/reporte2':
        from reporte2 import layout as reporte2_layout, register_callbacks as register_callbacks_reporte2
        if not hasattr(app, 'reporte2_callbacks_registered'):
            register_callbacks_reporte2(app)
            app.reporte2_callbacks_registered = True
        return reporte2_layout
    elif pathname == '/cafae/digitalizacion':
        from cafae.cafae_digitalizacion import layout as elecciones_layout, register_callbacks as register_callbacks_elecciones
        if not hasattr(app, 'elecciones_callbacks_registered'):
            register_callbacks_elecciones(app)
            app.elecciones_callbacks_registered = True
        return elecciones_layout
    elif pathname == '/cafae/digitacion':
        from cafae.cafae_digitacion import layout as digitacion_layout, register_callbacks as register_callbacks_digitacion
        if not hasattr(app, 'digitacion_callbacks_registered'):
            register_callbacks_digitacion(app)
            app.digitacion_callbacks_registered = True
        return digitacion_layout
    elif pathname == '/cafae/verificacion':
        from cafae.cafae_verificacion import layout as verificacion_layout, register_callbacks as register_callbacks_verificacion
        if not hasattr(app, 'verificacion_callbacks_registered'):
            register_callbacks_verificacion(app)
            app.verificacion_callbacks_registered = True
        return verificacion_layout
    else:
        return html.Div([
            html.H1("Bienvenido a la aplicación Dash"),
            html.P("Selecciona un reporte de las rutas disponibles.")
        ])

# Servidor
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
