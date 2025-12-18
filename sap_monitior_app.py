# app.py
# ------------------------------------------------------------
# Dream-City Rollup Dashboard (DBC version - clickable tiles FIXED)
# MongoDB: localhost:27017
# DB: SAP_Monitor
# Collection: rollup_flows
#
# Install:
#   pip install -U dash dash-bootstrap-components dash-ag-grid pymongo
#
# Run:
#   python app.py
# ------------------------------------------------------------

from __future__ import annotations

from typing import Dict, List, Any
import ast

from pymongo import MongoClient

from dash import Dash, html, dcc, Input, Output, callback_context, ALL
import dash_bootstrap_components as dbc
import dash_ag_grid as dag


# ------------------------------------------------------------
# Mongo configuration
# ------------------------------------------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "SAP_Monitor"
COLLECTION_NAME = "rollup_flows"
LIMIT = 10000


def load_rollups() -> List[Dict[str, Any]]:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    col = client[DB_NAME][COLLECTION_NAME]
    return list(col.find({}, {"_id": 0}).limit(LIMIT))


ROLLUPS = load_rollups()
# ------------------------------------------------------------
# Rollup logic
# ------------------------------------------------------------
def worst_overall(r: Dict[str, Any]) -> str:
    tech = r.get("tech", {}).get("health", "GREEN")
    biz = r.get("business", {}).get("health", "GREEN")
    sla = r.get("sla", {}).get("state", "OK")

    if "RED" in (tech, biz) or sla == "BREACH":
        return "RED"
    if "AMBER" in (tech, biz) or sla == "AT_RISK":
        return "AMBER"
    return "GREEN"


def filter_rollups(tile_id: str) -> List[Dict[str, Any]]:
    if tile_id.startswith("overall_"):
        v = tile_id.split("_", 1)[1]
        return [r for r in ROLLUPS if worst_overall(r) == v]

    if tile_id.startswith("tech_"):
        v = tile_id.split("_", 1)[1]
        return [r for r in ROLLUPS if r.get("tech", {}).get("health") == v]

    if tile_id.startswith("business_"):
        v = tile_id.split("_", 1)[1]
        return [r for r in ROLLUPS if r.get("business", {}).get("health") == v]

    if tile_id.startswith("sla_"):
        v = tile_id.split("_", 1)[1]
        return [r for r in ROLLUPS if r.get("sla", {}).get("state") == v]

    return ROLLUPS


def to_tree_rows(flows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for r in flows:
        cid = r.get("correlation_id", "")
        overall = worst_overall(r)

        plant = r.get("sap_idoc", {}).get("plant", "")
        idoc = r.get("sap_idoc", {}).get("number", "")
        order = r.get("order", {}).get("sap_order", "")
        route = r.get("route", "")

        # Parent
        rows.append({
            "path": [cid],
            "correlation_id": cid,
            "overall": overall,
            "node_type": "FLOW",
            "plant": plant,
            "idoc": idoc,
            "sap_order": order,
            "route": route,
            "key": "Summary",
            "value": "",
            "reason": "",
            "checkpoint": "",
            "sla_state": r.get("sla", {}).get("state", ""),
        })

        # TECH child
        t = r.get("tech", {})
        rows.append({
            "path": [cid, "TECH"],
            "correlation_id": cid,
            "overall": overall,
            "node_type": "TECH",
            "plant": plant,
            "idoc": idoc,
            "sap_order": order,
            "route": route,
            "key": "Transport",
            "value": f"{t.get('health','')} / {t.get('last_status','')}",
            "reason": t.get("reason_code") or "",
            "checkpoint": t.get("last_checkpoint") or "",
            "sla_state": r.get("sla", {}).get("state", ""),
        })

        # BUSINESS child
        b = r.get("business", {})
        rows.append({
            "path": [cid, "BUSINESS"],
            "correlation_id": cid,
            "overall": overall,
            "node_type": "BUSINESS",
            "plant": plant,
            "idoc": idoc,
            "sap_order": order,
            "route": route,
            "key": "Business Status",
            "value": f"{b.get('health','')} / {b.get('status','')}",
            "reason": b.get("reason_code") or "",
            "checkpoint": "",
            "sla_state": r.get("sla", {}).get("state", ""),
        })

        # SLA child
        s = r.get("sla", {})
        rows.append({
            "path": [cid, "SLA"],
            "correlation_id": cid,
            "overall": overall,
            "node_type": "SLA",
            "plant": plant,
            "idoc": idoc,
            "sap_order": order,
            "route": route,
            "key": "SLA Timing",
            "value": f"due={s.get('response_due_seconds','')}s, actual={s.get('actual_response_seconds','')}s",
            "reason": "BREACH" if s.get("breach") else "",
            "checkpoint": "",
            "sla_state": s.get("state", ""),
        })

    return rows


def compute_counts() -> Dict[str, int]:
    tids = [
        "overall_GREEN", "overall_AMBER", "overall_RED",
        "tech_GREEN", "tech_RED",
        "business_GREEN", "business_AMBER", "business_RED",
        "sla_OK", "sla_AT_RISK", "sla_BREACH",
    ]
    return {tid: len(filter_rollups(tid)) for tid in tids}


COUNTS = compute_counts()

# ------------------------------------------------------------
# App + DBC layout
# ------------------------------------------------------------
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "מערכת ניטור מערכות SAP-WMS"


def tile(label: str, tile_id: str, color: str, subtitle: str = "") -> dbc.Col:
    """
    Clickable tile:
      - dbc.Button has n_clicks (pattern-match target)
      - Card is visual content inside
    """
    card = dbc.Card(
        dbc.CardBody(
            [
                html.Div(
                    label,
                    className="fw-bold"
                ),
                html.Div(
                    str(COUNTS.get(tile_id, 0)),
                    className="display-6 fw-bold"
                ),
                html.Div(
                    subtitle,
                    className="text-muted small") if subtitle else html.Div(),
            ]
        ),
        color=color,
        outline=True,
        className="h-100 text-center",
    )

    return dbc.Col(
        dbc.Button(
            card,
            id={"type": "tile", "id": tile_id},
            n_clicks=0,
            color="link",
            className="p-0 w-100 text-start",
            style={
                "textDecoration": "none",
                "border"        : "none",
                "boxShadow"     : "none",
            },
        ),
        # md=3,
        className="col-2 mb-3",
    )


tiles_row = dbc.Row(
    [
        tile("ירוק תהליכי", "overall_GREEN", "success", "סך תהליכים מוצלחים"),
        tile("צהוב תהליכי", "overall_AMBER", "warning", "סך תהליכים באזהרה"),
        tile("אדום תהליכי", "overall_RED", "danger", "פעולות תיקון נדרשות"),
        tile("אדום טכני", "tech_RED", "danger", "תעבורה שנכשלה"),
        tile("אדום תפעולי", "business_RED", "danger",  "תקלה עסקית או דחיה בתגובה"),
        tile("SLA הפרת ", "sla_BREACH", "danger", "הפרת הסכם"),
    ],
    className="g-3",
)

grid = dag.AgGrid(
    id="grid",
    rowData=[],
    columnDefs=[
        {"field": "overall", "headerName": "חיווי כללי", "width": 110},
        {"field": "node_type", "headerName": "סיווג", "width": 120},
        {"field": "plant", "headerName": "מחסן", "width": 95},
        {"field": "idoc", "headerName": "iDoc", "width": 160},
        {"field": "sap_order", "headerName": "הזמנה", "width": 130},
        {"field": "key", "headerName": "מפתח סיווג", "width": 150},
        {"field": "value", "headerName": "ערך", "flex": 1, "minWidth": 260},
        {"field": "reason", "headerName": "סיבה", "width": 220},
        {"field": "checkpoint", "headerName": "צ׳ק פוינט", "width": 200},
        {"field": "sla_state", "headerName": "SLA", "width": 110},
    ],
    defaultColDef={"resizable": True, "sortable": True, "filter": True},
    dashGridOptions={
        "treeData": True,
        "groupDisplayType": "singleColumn",  # +/- expand/collapse
        "animateRows": True,
        "groupDefaultExpanded": 0,
        "getDataPath": {"function": "return params.data.path;"},
        "autoGroupColumnDef": {"headerName": "Correlation / Nodes", "minWidth": 340},
    },
    style={"height": "560px", "width": "100%"},
)

app.layout = dbc.Container(
    [
        dcc.Store(id="selected_tile", data="overall_RED"),
        dbc.Row(
            dbc.Col(
                html.H2(
                    "דשבורד ממשקים מתוכלל"),
                    width=12),
                class_name="my-3 text-center",
        ),

        dbc.Alert(
            # f"הועלו {len(ROLLUPS)} מסמכים מתוכללים מתוך מאגר הנתונים ({DB_NAME}.{COLLECTION_NAME}).",
            f"הועלו {len(ROLLUPS)} מסמכים מתוכללים מתוך מאגר הנתונים ",
            color="info",
            className="mb-3 text-center",
        ),

        dbc.Row(
            dbc.Col(
                # html.Div("Click a tile to load the tree table:", className="fw-bold"),
                html.Div(
                    "לסינון הרשימה לחץ על הקוביה",
                    className="fw-bold text-end",
                ),
                width=12,
                class_name="my-3",
            )
        ),
        tiles_row,

        dbc.Row(
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dbc.Col(html.H4("Rollup Tree"), md=8),
                            dbc.Col(html.Div(id="active_filter", className="text-muted small text-end"), md=4),
                        ],
                        className="align-items-center mb-2",
                    ),
                    grid,
                ],
                width=12,
            ),
            className="mt-2",
        )
    ],
    fluid=True,
)

# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@app.callback(
    Output("selected_tile", "data"),
    Input ({"type": "tile", "id": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def select_tile(_clicks):
    trig = callback_context.triggered
    if not trig:
        return "overall_RED"

    # prop_id looks like: {"type":"tile","id":"sla_BREACH"}.n_clicks
    prop_id = trig[0]["prop_id"].split(".")[0]

    # Safe parse dict string
    tile_obj = ast.literal_eval(prop_id)
    return tile_obj.get("id", "overall_RED")


@app.callback(
    Output("grid"         , "rowData" ),
    Output("active_filter", "children"),
    Input ("selected_tile", "data"    ),
)
def update_grid(tile_id: str):
    flows = filter_rollups(tile_id)[:600]
    return to_tree_rows(flows), f"{len(flows)} תוצאה {tile_id}פילטר נבחר "
    #return to_tree_rows(flows), f"Active filter: {tile_id} (showing {len(flows)} flows)"


if __name__ == "__main__":
    app.run(debug=True)
