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
app.title = "Proyecto Dash"

# Importar la función para crear la ruta de exportación de CSV
from miconsulta import create_csv_export_route

# Registrar la ruta de exportación de CSV
create_csv_export_route(server)

# # Registrar los layouts y callbacks de tramas al inicio de la aplicación
# from TramaB1B2.TramaB1B2 import layout as Tramas_layout, register_callbacks as register_callbacks_tramas
# # Pre-cargar el layout y registrar los callbacks
# register_callbacks_tramas(app)
# app.tramas_callbacks_registered = True  # Marcar que los callbacks ya fueron registrados

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
    
    # elif pathname == '/sgdredes/seguimiento':
    #     from sgdredes.sgdredesconsulta import layout as sgd_redes_layout, register_callbacks as register_callbacks_sgdredes
    #     if not hasattr(app, 'sgdredes_callbacks_registered'):
    #         register_callbacks_sgdredes(app)
    #         app.sgdredes_callbacks_registered = True

    #     # Pasar el parámetro `codigo` al layout de sgdredes
    #     params = parse_qs(search[1:])
    #     codigo = params.get('codigo', [None])[0]  # Obtiene el valor del parámetro 'codigo'
    #     return sgd_redes_layout(codigo)  # Pasar el código al layout

    elif pathname == '/sgdcentral/seguimiento':
            from sgdcentral.sgdcentral import layout as sgd_central_layout, register_callbacks as register_callbacks_sgdcentral
            if not hasattr(app, 'sgdcentral_callbacks_registered'):
                register_callbacks_sgdcentral(app)
                app.sgdcentral_callbacks_registered = True
            return sgd_central_layout
    

    # elif pathname == '/reportes/turnos':
    #         from Turnos_asistenciales.Turnos_asistenciales import layout as turnos_layout, register_callbacks as register_callbacks_turnos
    #         if not hasattr(app, 'turnos_callbacks_registered'):
    #             register_callbacks_turnos(app)
    #             app.turnos_callbacks_registered = True
    #         return turnos_layout

    # elif pathname == '/reportes/tramas':
    #         return Tramas_layout


    
    elif pathname == '/':
            from sgdredes.sgdredesconsulta import layout as sgd_redes_layout, register_callbacks as register_callbacks_sgdredes
            if not hasattr(app, 'sgdredes_callbacks_registered'):
                register_callbacks_sgdredes(app)
                app.sgdredes_callbacks_registered = True
            # Pasar el parámetro `codigo` al layout de sgdredes
            params = parse_qs(search[1:])
            codigo = params.get('codigo', [None])[0]  # Obtiene el valor del parámetro 'codigo'
            return sgd_redes_layout(codigo)  # Pasar el código al layout
    
    else:
        return html.Div([
            html.H1("Bienvenido a la aplicación Dash"),
            html.P("Selecciona un reporte de las rutas disponibles.")
        ])

    # Ruta para servir el archivo HTML de SGD Red
    # @server.route('/sgdredes')
# def sgdredes():
#     return render_template('sgdredes.html')

# # Definir la ruta de la API en el servidor Flask
# @server.route('/api/data', methods=['GET'])
# def data():
#     data = request.get_json()  # Esto obtiene los datos del cuerpo del POST
#     co_red = data.get('co_red')
#     nu_expediente = data.get('nu_expediente')

#     # Validar que los parámetros se proporcionen
#     if not co_red or not nu_expediente:
#         return jsonify({'error': 'Faltan parámetros: co_red y nu_expediente son necesarios.'}), 400

#     # Utiliza la función get_data() para obtener los datos
#     data = get_data(co_red, nu_expediente)
#     return jsonify(data)

# Servidor
if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0', port=8050)
