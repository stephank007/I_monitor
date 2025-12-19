from __future__ import annotations

from typing import Dict, List, Any

from pymongo import MongoClient

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
# Status helpers
# ------------------------------------------------------------
def sla_state_to_status(sla_state: str) -> str:
    """
    SLA state -> unified status used for coloring/icons.
    OK -> GREEN
    AT_RISK -> AMBER
    BREACH -> RED
    """
    s = (sla_state or "").upper()
    if s == "BREACH":
        return "RED"
    if s == "AT_RISK":
        return "AMBER"
    return "GREEN"


def worst_status(statuses: List[str]) -> str:
    """
    Given statuses in {"GREEN","AMBER","RED"}, return worst.
    """
    st = [((x or "").upper()) for x in statuses]
    if "RED" in st:
        return "RED"
    if "AMBER" in st:
        return "AMBER"
    return "GREEN"


def worst_overall(r: Dict[str, Any]) -> str:
    tech = (r.get("tech", {}).get("health") or "GREEN").upper()
    biz = (r.get("business", {}).get("health") or "GREEN").upper()
    sla_state = (r.get("sla", {}).get("state") or "OK").upper()
    sla_status = sla_state_to_status(sla_state)
    return worst_status([tech, biz, sla_status])


# ------------------------------------------------------------
# Filtering / counts (tiles)
# ------------------------------------------------------------
def filter_rollups(tile_id: str) -> List[Dict[str, Any]]:
    if tile_id.startswith("overall_"):
        v = tile_id.split("_", 1)[1].upper()
        return [r for r in ROLLUPS if worst_overall(r) == v]
    
    if tile_id.startswith("tech_"):
        v = tile_id.split("_", 1)[1].upper()
        return [r for r in ROLLUPS if (r.get("tech", {}).get("health") or "GREEN").upper() == v]
    
    if tile_id.startswith("business_"):
        v = tile_id.split("_", 1)[1].upper()
        return [r for r in ROLLUPS if (r.get("business", {}).get("health") or "GREEN").upper() == v]
    
    if tile_id.startswith("sla_"):
        v = tile_id.split("_", 1)[1].upper()  # OK / AT_RISK / BREACH
        return [r for r in ROLLUPS if (r.get("sla", {}).get("state") or "OK").upper() == v]
    
    return ROLLUPS


def compute_counts() -> Dict[str, int]:
    tids = [
        "overall_GREEN", "overall_AMBER", "overall_RED",
        "tech_GREEN", "tech_AMBER", "tech_RED",
        "business_GREEN", "business_AMBER", "business_RED",
        "sla_OK", "sla_AT_RISK", "sla_BREACH",
    ]
    return {tid: len(filter_rollups(tid)) for tid in tids}


# ------------------------------------------------------------
# Grouped tree rows (sap_order -> node_type)
# Each leaf row gets its own row_status (NOT the aggregate)
# FLOW row uses aggregate overall as row_status
# order_overall is computed per sap_order for display (and group row coloring via children)
# ------------------------------------------------------------
def to_grouped_rows(flows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    
    for r in flows:
        sap_order = r.get("order", {}).get("sap_order", "UNKNOWN")
        cid = r.get("correlation_id", "")
        plant = r.get("sap_idoc", {}).get("plant", "")
        idoc = r.get("sap_idoc", {}).get("number", "")
        
        # Per-section statuses
        tech = r.get("tech", {})
        biz = r.get("business", {})
        sla = r.get("sla", {})
        
        tech_status = (tech.get("health") or "GREEN").upper()
        biz_status = (biz.get("health") or "GREEN").upper()
        sla_state = (sla.get("state") or "OK").upper()
        sla_status = sla_state_to_status(sla_state)
        
        # Aggregate for FLOW / order
        overall = worst_status([tech_status, biz_status, sla_status])
        
        # FLOW (aggregate)
        rows.append(
            {
                "sap_order"     : sap_order,
                "node_type"     : "FLOW",
                "overall"       : overall,
                "row_status"    : overall,  # <-- important
                "order_overall" : overall,  # will be overwritten later (but ok)
                "plant"         : plant,
                "idoc"          : idoc,
                "key"           : "Summary",
                "value"         : "",
                "reason"        : "",
                "checkpoint"    : "",
                "sla_state"     : sla_state,
                "correlation_id": cid,
            },
        )
        
        # TECH (own status)
        rows.append(
            {
                "sap_order"     : sap_order,
                "node_type"     : "TECH",
                "overall"       : overall,
                "row_status"    : tech_status,  # <-- important
                "order_overall" : overall,
                "plant"         : plant,
                "idoc"          : idoc,
                "key"           : "Transport",
                "value"         : f"{tech_status} / {tech.get('last_status', '')}",
                "reason"        : tech.get("reason_code", "") or "",
                "checkpoint"    : tech.get("last_checkpoint", "") or "",
                "sla_state"     : sla_state,
                "correlation_id": cid,
            },
        )
        
        # BUSINESS (own status)
        rows.append(
            {
                "sap_order"     : sap_order,
                "node_type"     : "BUSINESS",
                "overall"       : overall,
                "row_status"    : biz_status,  # <-- important
                "order_overall" : overall,
                "plant"         : plant,
                "idoc"          : idoc,
                "key"           : "Business",
                "value"         : f"{biz_status} / {biz.get('status', '')}",
                "reason"        : biz.get("reason_code", "") or "",
                "checkpoint"    : "",
                "sla_state"     : sla_state,
                "correlation_id": cid,
            },
        )
        
        # SLA (own status derived from SLA state)
        rows.append(
            {
                "sap_order"     : sap_order,
                "node_type"     : "SLA",
                "overall"       : overall,
                "row_status"    : sla_status,  # <-- important (AMBER when AT_RISK)
                "order_overall" : overall,
                "plant"         : plant,
                "idoc"          : idoc,
                "key"           : "SLA",
                "value"         : f"יעד: {sla.get('response_due_seconds', '')}s / בפועל: {sla.get('actual_response_seconds', '')}s",
                "reason"        : sla_state,
                "checkpoint"    : "",
                "sla_state"     : sla_state,
                "correlation_id": cid,
            },
        )
    
    # Ensure order_overall is consistent per sap_order (in case of multiple flows per order)
    per_order: Dict[str, str] = {}
    for row in rows:
        so = row.get("sap_order", "UNKNOWN")
        per_order[so] = worst_status([per_order.get(so, "GREEN"), row.get("row_status", "GREEN")])
    
    for row in rows:
        row["order_overall"] = per_order.get(row.get("sap_order", "UNKNOWN"), "GREEN")
    
    return rows


# Fast lookup for detail page
ROLLUP_BY_CID = {r.get("correlation_id"): r for r in ROLLUPS if r.get("correlation_id")}

"""
def ROLLUP_BY_CID(correlation_id: str) -> Dict[str, Any] | None:

for r in ROLLUPS:
    if r.get("correlation_id") == correlation_id:
        return r
return None
"""
