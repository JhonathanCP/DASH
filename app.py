import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from flask import Flask
from flask_caching import Cache

# Inicializar el servidor Flask y la aplicación Dash
server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP,
    'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css',  # Bootstrap CSS
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.1/css/all.min.css']
)
app.title = "Proyecto Dash"

# Configurar Flask-Caching con Redis
cache = Cache()
cache.init_app(app.server, config={
    'CACHE_TYPE': 'RedisCache',
    'CACHE_REDIS_HOST': 'localhost',  # Cambia esto si Redis está en otro servidor
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 0,
    'CACHE_DEFAULT_TIMEOUT': 3600  # 1 hora
})

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
    # if pathname == '/sgdredes':
    #     from sgdredes import layout as sgdredes_layout, register_callbacks as register_callbacks_sgdredes
    #     if not hasattr(app, 'sgdredes_callbacks_registered'):
    #         register_callbacks_sgdredes(app)
    #         app.sgdredes_callbacks_registered = True
    #     return sgdredes_layout

    if pathname == '/dengue/detalle':
        from dengue import layout as reporte2_layout, register_callbacks as register_callbacks_reporte2
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

    elif pathname == '/cafae/administracion':
        from cafae.cafae_administracion import layout as administracion_layout, register_callbacks as register_callbacks_administracion
        if not hasattr(app, 'administracion_callbacks_registered'):
            register_callbacks_administracion(app)
            app.administracion_callbacks_registered = True
        return administracion_layout

    else:
        return html.Div([
            html.H1("Bienvenido a la aplicación Dash"),
            html.P("Selecciona un reporte de las rutas disponibles.")
        ])

# Servidor
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
