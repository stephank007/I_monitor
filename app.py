from dash import Dash, html, dcc, page_container
import dash_bootstrap_components as dbc

app = Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)

app.title = "מערכת ניטור מערכות SAP-WMS"

app.layout = html.Div(
    [
        # dcc.Location(id="url", refresh=False),
        html.Div([page_container])
    ],
    style={"minHeight": "100vh"},
)

if __name__ == "__main__":
    app.run(debug=True)
