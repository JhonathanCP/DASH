from dash import Dash, html, dcc, Input, Output
import reports.cupos_programados as rp1
import reports.report2 as rp2
import dash_bootstrap_components as dbc

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname'),
               Input('url', 'search')])

def display_page(pathname, search):
    if pathname == '/cupos_programados':
        red = search.split('=')[1] if search and '=' in search else None
        return rp1.layout(red)
    if pathname == '/report2':
        red = search.split('=')[1] if search and '=' in search else None
        return rp2.layout(red)
    else:
        return html.Div([
            html.H1('Bienvenido a la aplicación de reportes'),
            html.P('Seleccione un reporte en la barra de navegación.')
        ])

if __name__ == '__main__':
    app.run_server(debug=False)
