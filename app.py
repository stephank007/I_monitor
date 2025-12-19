import dash_bootstrap_components as dbc
from dash import Dash, html, page_container

app = Dash(
        __name__,
        use_pages=True,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True,
)

app.title = "מערכת ניטור מערכות SAP-WMS"

app.layout = html.Div(
        [
            html.Div([page_container]),
        ],
        style={"minHeight": "100vh"},
)

if __name__ == "__main__":
    app.run(debug=True)
