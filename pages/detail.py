from __future__ import annotations

import dash
from dash import html
import dash_bootstrap_components as dbc
from data_store import ROLLUP_BY_CID, worst_overall

dash.register_page(__name__, path_template="/detail/<correlation_id>", name="פרטי תהליך")


def badge_for(overall: str) -> dbc.Badge:
    m = {
        "GREEN": "success",
        "AMBER": "warning",
        "RED"  : "danger",
    }
    return dbc.Badge(overall, color=m.get(overall, "secondary"), className="ms-2")


def layout(correlation_id: str = ""):
    r = ROLLUP_BY_CID.get(correlation_id)
    
    if not r:
        return dbc.Container(
            [
                dbc.Row(
                    dbc.Col(
                        dbc.Button(
                            "← חזרה לדשבורד",
                            href="/",
                            color="secondary",
                            outline=True,
                        ),
                        width=12,
                    ),
                    class_name="mt-3",
                ),
                dbc.Alert(f"מספר תהליך לא נמצא: {correlation_id}", color="danger", className="mt-3"),
            ],
            fluid=True,
            style={"direction": "rtl"},
        )
    
    overall = worst_overall(r)
    idoc = r.get("sap_idoc", {}).get("number", "לא ידוע")
    sap_order = r.get("order", {}).get("sap_order", "לא ידוע")
    
    tech = r.get("tech", {})
    biz = r.get("business", {})
    sla = r.get("sla", {})
    
    failed_phase = (
        tech.get("last_checkpoint")
        if tech.get("health") == "RED" else "בדיקה עסקית"
        if biz.get("health") in ("AMBER", "RED") else "חריגת SLA"
        if sla.get("state") == "BREACH"
        else "תקין"
    )
    
    short_desc = (
        "התהליך נכשל ונדרש טיפול בהתאם לסיבה המפורטת."
        if overall != "GREEN"
        else "לא זוהתה תקלה בתהליך."
    )
    
    failure_reason_title = (
        f"תקלה טכנית: {tech.get('reason_code')}"
        if tech.get("reason_code")
        else f"תקלה עסקית: {biz.get('reason_code')}"
        if biz.get("reason_code") and biz.get("reason_code") != "FULL_CONFIRM"
        else "הפרת SLA – אין תגובה בזמן"
        if sla.get("state") == "BREACH"
        else "לא זוהתה סיבת כשל"
    )
    
    return dbc.Container(
        [
            dbc.Row(
                dbc.Col(
                    [
                        dbc.Button("← חזרה לדשבורד", href="/", color="secondary", outline=True, className="me-2"),
                    ],
                    width=12,
                ),
                class_name="mt-3",
            ),
            dbc.Row(
                dbc.Col(
                    html.Span("פרטי תהליך — תהליך כושל", style={"fontSize": "28px", "fontWeight": "700"}),
                ),
            ),
            
            html.Div(
                [
                    html.Span(f"מספר iDoc: {idoc}  ·  "),
                    html.Span(f"הזמנת SAP: {sap_order}  ·  "),
                    html.Span("סטטוס: "),
                    badge_for(overall),
                ],
                className="text-muted mt-2",
            ),
            
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H5("סיכום התקלה", className="mb-3"),
                        html.Div([html.Span("שלב כשל: ", className="fw-bold"), failed_phase]),
                        html.Div([html.Span("תיאור קצר: ", className="fw-bold"), short_desc], className="mt-2"),
                    ],
                ),
                className="mt-3",
            ),
            
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5("ציר זמן של עיבוד", className="mb-3"),
                                    html.Ul(
                                        [
                                            html.Li("1. יצירת iDoc ב-SAP (סטטוס 03)"),
                                            html.Li("2. המרה ל-JSON במתווך (PO)"),
                                            html.Li("3. ולידציה עסקית / ניתוב"),
                                            html.Li("4. מעבר Firewall יוצא → נכנס"),
                                            html.Li("5. עיבוד במערכת WMS"),
                                        ],
                                        className="text-muted",
                                    ),
                                    html.Div("העיבוד נעצר בשלב הכשל במידה וקיים.", className="text-muted small"),
                                ],
                            ),
                        ),
                        md=7,
                        class_name="mt-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5("סיבת התקלה", className="mb-3"),
                                    html.Div(
                                        failure_reason_title,
                                        className="fw-bold text-danger" if overall == "RED" else "fw-bold",
                                    ),
                                    html.Hr(),
                                    html.Div(
                                        [
                                            html.Div(
                                                f"• טכני: {tech.get('health')} / {tech.get('last_status')} / {tech.get('reason_code')}",
                                                className="text-muted",
                                            ),
                                            html.Div(
                                                f"• עסקי: {biz.get('health')} / {biz.get('status')} / {biz.get('reason_code')}",
                                                className="text-muted",
                                            ),
                                            html.Div(
                                                f"• SLA: {sla.get('state')} (יעד={sla.get('response_due_seconds')} שניות, בפועל={sla.get('actual_response_seconds')} שניות)",
                                                className="text-muted",
                                            ),
                                        ],
                                    ),
                                    html.Div(
                                        "המלצה: בדוק נתוני מאסטר, מלאי, או תעבורה (PO / Firewall / WMS).",
                                        className="text-muted small mt-2",
                                    ),
                                ],
                            ),
                        ),
                        md=5,
                        class_name="mt-3",
                    ),
                ],
                className="g-3",
            ),
            
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5("המלצות לתיקון", className="mb-3"),
                                    html.Ol(
                                        [
                                            html.Li(f"פתח את ההזמנה ב-SAP ({sap_order}) במסכים VA03 / VL03N."),
                                            html.Li("בדוק שדות מחסן / מיקום מלאי."),
                                            html.Li("תקן נתוני מאסטר או מיפויים שגויים."),
                                            html.Li("בצע אימות מחדש / שליחה מחדש לאחר התיקון."),
                                        ],
                                        className="text-muted",
                                    ),
                                ],
                            ),
                        ),
                        md=7,
                        class_name="mt-3",
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5("מידע מתקדם / לוגים", className="mb-3"),
                                    html.Div(
                                        "אזור להצגת לוגים, שגיאות סכימה, SPLUNK ו-Diagnostics.", className="text-muted",
                                    ),
                                    html.Pre(
                                        f"correlation_id: {correlation_id}\n"
                                        f"checkpoint אחרון: {tech.get('last_checkpoint')}\n"
                                        f"סיבת כשל טכנית: {tech.get('reason_code')}\n"
                                        f"סיבת כשל עסקית: {biz.get('reason_code')}\n"
                                        f"סטטוס SLA: {sla.get('state')}\n",
                                        className="mt-2 text-muted",
                                        style={"whiteSpace": "pre-wrap", "fontSize": "12px"},
                                    ),
                                ],
                            ),
                        ),
                        md=5,
                        class_name="mt-3",
                    ),
                ],
                className="g-3",
            ),
            
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H5("פעולות", className="mb-3"),
                        dbc.Row(
                            [
                                dbc.Col(dbc.Button("שליחה מחדש", color="primary", className="w-100"), md=2),
                                dbc.Col(dbc.Button("תיקון", color="info", outline=True, className="w-100"), md=2),
                                dbc.Col(dbc.Button("מחיקה", color="danger", outline=True, className="w-100"), md=2),
                                dbc.Col(
                                    html.Div(
                                        [
                                            html.Div("סטטוס פעולה: ", className="fw-bold d-inline"),
                                            html.Span("לא בוצע", className="text-muted"),
                                            html.Div(
                                                "(יעודכן ל: ״בשליחה״, ״תוקן״, ״נמחק״ וכו׳)",
                                                className="text-muted small",
                                            ),
                                        ],
                                    ),
                                    md=6,
                                ),
                            ],
                            className="g-2",
                        ),
                    ],
                ),
                className="mt-3 mb-4",
            ),
        ],
        fluid=True,
        style={"direction": "rtl"},
    )
