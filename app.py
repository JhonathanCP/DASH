import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

# Inicializar la aplicación Dash
app = dash.Dash(__name__)
app.title = "Proyecto Dash"

external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

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
    else:
        return html.Div([
            html.H1("Bienvenido a la aplicación Dash"),
            html.P("Selecciona un reporte de las rutas disponibles.")
        ])

# Servidor
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
