from __future__ import annotations

from typing import Dict, Any, List, Optional
import ast

import dash
from dash import html, dcc, Input, Output, State, callback_context, ALL, callback, no_update
import dash_bootstrap_components as dbc
import dash_ag_grid as dag

from data_store import (
    ROLLUPS,
    compute_counts,
    filter_rollups,
    to_grouped_rows,
)

# ------------------------------------------------------------
# Page registration
# ------------------------------------------------------------
dash.register_page(
    __name__,
    path="/",
    name="Dashboard"
)

COUNTS = compute_counts()

# ------------------------------------------------------------
# Tiles
# ------------------------------------------------------------
def tile(label: str, tile_id: str, color: str, subtitle: str = "") -> dbc.Col:
    card = dbc.Card(
        dbc.CardBody(
            [
                html.Div(label, className="fw-bold"),
                html.Div(str(COUNTS.get(tile_id, 0)), className="display-6 fw-bold"),
                html.Div(subtitle, className="text-muted small") if subtitle else html.Div(),
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
            className="p-0 w-100",
            style={
                "textDecoration": "none",
                "border": "none",
                "boxShadow": "none",
            },
        ),
        className="col-2 mb-3",
    )


tiles_row = dbc.Row(
    [
        tile("ירוק תהליכי", "overall_GREEN", "success", "סך תהליכים מוצלחים"),
        tile("צהוב תהליכי", "overall_AMBER", "warning", "סך תהליכים באזהרה"),
        tile("אדום תהליכי", "overall_RED", "danger", "פעולות תיקון נדרשות"),
        tile("אדום טכני", "tech_RED", "danger", "תעבורה שנכשלה"),
        tile("אדום תפעולי", "business_RED", "danger", "תקלה תפעולית/ דחיה"),
        tile("SLA הפרה", "sla_BREACH", "danger", "חריגה מהסכם"),
    ],
    className="g-3",
)

# ------------------------------------------------------------
# AG Grid (Grouped Tree)
# ------------------------------------------------------------
grid = dag.AgGrid(
    id="grid",
    rowData=[],
    columnDefs=[
        # hierarchy (hidden)
        {"field": "sap_order", "hide": True},
        {"field": "node_type", "hide": True},

        # visible columns
        # {"field": "overall", "headerName": "חיווי כללי", "width": 110},
        {
            "field": "overall",
            "headerName": "חיווי",
            "width": 90,
            "valueFormatter": {"function": "rowStatusIconText(params)"     },
            "cellStyle"     : {"function": "rowStatusIconCellStyle(params)"},
            # "cellRenderer": {"function": "rowStatusIcon(params)"},
            # "cellStyle": {"function": "overallIconStyle(params)"},

            "sortable": False,
            "filter": False,
        },

        {"field": "row_status", "hide": True},
        {"field": "value", "headerName": "ערך", "minWidth": 360, "flex": 1, "wrapText": True, "autoHeight": True},
        {"field": "reason", "headerName": "סיבה", "minWidth": 280, "wrapText": True, "autoHeight": True},
        {"field": "checkpoint", "headerName": "צ׳ק פוינט", "minWidth": 240, "wrapText": True, "autoHeight": True},

        {"field": "plant", "headerName": "מחסן", "width": 110},
        {"field": "idoc", "headerName": "iDoc", "minWidth": 180},
        {"field": "key", "headerName": "רכיב", "minWidth": 140},
        {"field": "sla_state", "headerName": "SLA", "width": 130},
        {"field": "order_overall", "headerName": "סטטוס הזמנה", "minWidth": 140},
    ],
    defaultColDef={
        "resizable": True,
        "sortable": True,
        "filter": True,
        "wrapText": True,
        "autoHeight": True,
    },
    enableEnterpriseModules=True,
    dangerously_allow_code=True,
    dashGridOptions={
        "treeData": True,
        "animateRows": True,
        "groupDefaultExpanded": 0,
        "getDataPath": {
            "function": "getDataPathSap(params)"
        },
        "icons": {
            "groupExpanded": '<span style="font-weight:700;">−</span>',
            "groupContracted": '<span style="font-weight:700;">+</span>',
        },
        "autoGroupColumnDef": {
            "headerName": "היררכיה (הזמנה → רכיב)",
            "minWidth": 240,
            "cellRendererParams": {"suppressCount": True},
        },
        "rowSelection": "single",
        "getRowStyle": {"function": "rowStyleByRowStatus(params)"},
        # "getRowStyle": {"function": "rowStyleOverall(params)"},
    },
    style={
        "height": "2048px",
        "width": "100%",
    },
)

# ------------------------------------------------------------
# Layout
# ------------------------------------------------------------
layout = dbc.Container(
    [
        dcc.Store(id="selected_tile", data="overall_RED"),

        dbc.Row(
            dbc.Col(html.H2("דשבורד ממשקים מתוכלל"), width=12),
            class_name="my-3 text-center",
        ),

        dbc.Alert(
            f"הועלו {len(ROLLUPS)} מסמכים מתוכללים מתוך מאגר הנתונים",
            color="info",
            className="mb-3 text-center",
        ),

        dbc.Row(
            dbc.Col(
                html.Div("לסינון הרשימה לחץ על הקוביה", className="fw-bold text-end"),
                width=12,
            ),
            class_name="my-3",
        ),

        tiles_row,

        dbc.Row(
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dbc.Col(html.H4("טבלה מתכללת"), md=8),
                            dbc.Col(
                                html.Div(
                                    id="active_filter",
                                    className="text-muted small text-end"
                                ),
                                md=4,
                            ),
                        ],
                        className="align-items-center mb-2",
                    ),
                    grid,
                    html.Div(
                        "פתיחת מסך פרטים: לחץ על שורת FLOW תחת ההזמנה",
                        className="text-muted small text-end mt-2",
                    ),
                ],
                width=12,
            ),
            className="mt-2",
        ),
    ],
    fluid=True,
    style={"direction": "rtl"},
)

# ------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------
@callback(
    Output("selected_tile", "data"),
    Input({"type": "tile", "id": ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def select_tile(_clicks):
    trig = callback_context.triggered
    if not trig:
        return "overall_RED"

    prop_id = trig[0]["prop_id"].split(".")[0]
    tile_obj = ast.literal_eval(prop_id)
    return tile_obj.get("id", "overall_RED")


@callback(
    Output("grid", "rowData"),
    Output("active_filter", "children"),
    Input("selected_tile", "data"),
)
def update_grid(tile_id: str):
    flows = filter_rollups(tile_id)[:600]
    rows = to_grouped_rows(flows)
    return rows, f"{len(flows)} תוצאות | פילטר נבחר: {tile_id}"


@callback(
    Output("_pages_location", "pathname"),
    Input("grid", "cellClicked"),
    State("grid", "rowData"),
    State("_pages_location", "pathname"),
    prevent_initial_call=True,
)
def go_detail(
        cell_clicked: Optional[Dict[str, Any]],
        row_data: Optional[List[Dict[str, Any]]],
        current_path: str,
):
    if not cell_clicked or not row_data:
        return no_update

    row_index = cell_clicked.get("rowIndex")
    if row_index is None or row_index >= len(row_data):
        return no_update

    row = row_data[row_index]
    if row.get("node_type") != "FLOW":
        return no_update

    cid = row.get("correlation_id")
    if not cid:
        return no_update

    new_path = f"/detail/{cid}"
    return new_path if new_path != current_path else no_update
