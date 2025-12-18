import json, random, hashlib, os, math
from datetime import datetime, timedelta, timezone

random.seed(42)

# Parameters
n_flows = 10000
start_utc = datetime(2025, 12, 9, 0, 0, 0, tzinfo=timezone.utc)  # 7-day window ending near Dec 16, 2025
end_utc = datetime(2025, 12, 16, 23, 59, 59, tzinfo=timezone.utc)

# Distributions (rough)
p_transport_fail = 0.003  # 0.3%
p_schema_fail = 0.002  # 0.2%
p_po_mapping_fail = 0.0015  # 0.15%
p_fw_fail = 0.0008  # 0.08%
p_tls_fail = 0.0007  # 0.07%
p_other_transport_fail = 0.001  # 0.1% (http 500/401/413/dns)
# Remaining are transport ok.

# Business distributions (conditional on transport OK)
p_partial = 0.006  # 0.6%
p_reject = 0.002  # 0.2%
p_late = 0.004  # 0.4%
p_no_response = 0.001  # 0.1% (but could be transport ok + missing business response)
p_confirm_gt_req = 0.0005
p_uom_mismatch = 0.0005

# Reference lists
skus = [
    "PANTS-BLK-32", "PANTS-BLU-34", "PANTS-GRN-36", "PANTS-YLW-28", "PANTS-RED-30",
    "SHIRT-WHT-M", "SHIRT-BLK-L", "JACKET-NVY-50", "SOCKS-GRY-10", "BELT-BRN-40"
]
plants = ["DC01", "DC02", "DC03"]
schemas = ["ORDERS_v7", "ORDERS_v6", "ORDERS_v8"]
idoc_type = "ORDERS05"

reason_transport = [
    ("SCHEMA_INVALID_FIELD", "SAP_SCHEMA_VALIDATION"),
    ("PO_MAPPING_ERROR", "PO_MAPPING_OK"),
    ("FIREWALL_DROP", "FW_EGRESS_ALLOWED"),
    ("TLS_CERT_EXPIRED", "PO_SENT_HTTP"),
    ("DNS_FAILURE", "PO_SENT_HTTP"),
    ("HTTP_504", "SCXCONNECT_HTTP_ACK"),
    ("HTTP_500", "SCXCONNECT_HTTP_ACK"),
    ("HTTP_401", "SCXCONNECT_HTTP_ACK"),
    ("HTTP_413", "SCXCONNECT_HTTP_ACK"),
    ("CONNECTION_RESET", "PO_SENT_HTTP"),
    ("PO_QUEUE_BACKLOG", "PO_SENT_HTTP")
]


def rand_dt(start, end):
    delta = end - start
    sec = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=sec)


def make_hash(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def corr_id(i, dt):
    return f"DC-{dt.strftime('%Y%m%d')}-{i:06d}"


def idoc_number(i):
    return f"{9000000000 + i:016d}"[-16:]


def sap_order(i):
    return f"{4500100000 + i}"


def pick_failure():
    r = random.random()
    # allocate by cumulative probabilities
    cum = 0
    for p, code, checkpoint in [
        (p_schema_fail, "SCHEMA_INVALID_FIELD", "SAP_SCHEMA_VALIDATION"),
        (p_po_mapping_fail, "PO_MAPPING_ERROR", "PO_MAPPING_OK"),
        (p_fw_fail, "FIREWALL_DROP", "FW_EGRESS_ALLOWED"),
        (p_tls_fail, "TLS_CERT_EXPIRED", "PO_SENT_HTTP"),
        (p_other_transport_fail,
         random.choice(["DNS_FAILURE", "HTTP_500", "HTTP_401", "HTTP_413", "CONNECTION_RESET", "PO_QUEUE_BACKLOG"]),
         None),
        (p_transport_fail, "HTTP_504", "SCXCONNECT_HTTP_ACK"),
    ]:
        cum += p
        if r < cum:
            if checkpoint is None:
                checkpoint = "PO_SENT_HTTP" if code in ["DNS_FAILURE", "CONNECTION_RESET", "PO_QUEUE_BACKLOG",
                                                        "TLS_CERT_EXPIRED"] else "SCXCONNECT_HTTP_ACK"
            return True, code, checkpoint
    return False, None, None


def make_tech_events(flow):
    """
    Create checkpoint events for a flow, including failures.
    """
    base = flow["order_sent_utc"]
    cid = flow["correlation_id"]
    idoc = flow["sap_idoc_number"]
    schema = flow["schema"]
    plant = flow["plant"]
    payload_hash = flow["payload_hash"]
    route = "SAP->PO->FW->SCXConnect->WMS"

    events = []
    # SAP_IDOC_CREATED
    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "SAP_IDOC_CREATED",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "CREATED",
        "timestamps": {"event_utc": (base + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SAP", "host": "sap-prd-01"}
    })
    # Schema validation
    schema_time = base + timedelta(seconds=2)
    if flow["transport_failure"] and flow["transport_reason"] == "SCHEMA_INVALID_FIELD":
        events.append({
            "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "SAP_SCHEMA_VALIDATION",
            "correlation_id": cid,
            "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
            "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
            "status": "FAIL", "reason_code": "SCHEMA_INVALID_FIELD",
            "detail": random.choice([
                "Element 'ShipToParty' missing",
                "Invalid value for 'RequestedDeliveryDate'",
                "Unexpected element 'BatchNumber'",
                "Length exceeded for 'CustomerPO'"
            ]),
            "timestamps": {"event_utc": schema_time.isoformat().replace("+00:00", "Z")},
            "endpoint": {"system": "SAP", "host": "sap-prd-01"}
        })
        return events

    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "SAP_SCHEMA_VALIDATION",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "SCHEMA_OK",
        "timestamps": {"event_utc": schema_time.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SAP", "host": "sap-prd-01"}
    })
    # PO received
    po_recv = base + timedelta(seconds=5)
    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "SAP_PO_RECEIVED",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "RECEIVED_BY_PO",
        "timestamps": {"event_utc": po_recv.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SAP/PO", "host": "po-prd-01"}
    })
    # Mapping
    map_time = base + timedelta(seconds=8)
    if flow["transport_failure"] and flow["transport_reason"] == "PO_MAPPING_ERROR":
        events.append({
            "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "PO_MAPPING_OK",
            "correlation_id": cid,
            "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
            "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
            "status": "FAIL", "reason_code": "PO_MAPPING_ERROR",
            "detail": f"ValueMapping not found for plant={plant}",
            "timestamps": {"event_utc": map_time.isoformat().replace("+00:00", "Z")},
            "endpoint": {"system": "SAP/PO", "host": "po-prd-01"}
        })
        return events

    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "PO_MAPPING_OK",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "MAPPING_OK",
        "timestamps": {"event_utc": map_time.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SAP/PO", "host": "po-prd-01"}
    })
    # Send HTTP
    send_time = base + timedelta(seconds=12)
    if flow["transport_failure"] and flow["transport_checkpoint"] == "PO_SENT_HTTP":
        code = flow["transport_reason"]
        evt = {
            "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "PO_SENT_HTTP",
            "correlation_id": cid,
            "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
            "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
            "status": "FAIL", "reason_code": code,
            "timestamps": {"event_utc": send_time.isoformat().replace("+00:00", "Z")},
            "endpoint": {"system": "SAP/PO", "host": "po-prd-01"},
        }
        if code == "TLS_CERT_EXPIRED":
            evt["detail"] = "remote cert expired"
        elif code == "DNS_FAILURE":
            evt["detail"] = "wms.api.dreamcity.local not resolved"
        elif code == "CONNECTION_RESET":
            evt["detail"] = "connection reset by peer"
        elif code == "PO_QUEUE_BACKLOG":
            evt["detail"] = f"adapter queue depth={random.randint(5000, 20000)}"
        events.append(evt)
        return events

    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "PO_SENT_HTTP",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "HTTP_SENT",
        "timestamps": {"event_utc": send_time.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SAP/PO", "host": "po-prd-01"}
    })
    # Firewall
    fw_time = base + timedelta(seconds=13)
    if flow["transport_failure"] and flow["transport_reason"] == "FIREWALL_DROP":
        events.append({
            "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "FW_EGRESS_ALLOWED",
            "correlation_id": cid,
            "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
            "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
            "status": "FAIL", "reason_code": "FIREWALL_DROP",
            "detail": f"Denied by rule FW-OUT-{random.randint(200, 299)}",
            "timestamps": {"event_utc": fw_time.isoformat().replace("+00:00", "Z")},
            "endpoint": {"system": "FIREWALL", "host": "fw-edge-01"}
        })
        return events
    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "FW_EGRESS_ALLOWED",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "ALLOWED",
        "timestamps": {"event_utc": fw_time.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "FIREWALL", "host": "fw-edge-01"}
    })
    # SCX received
    scx_recv = base + timedelta(seconds=15)
    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "SCXCONNECT_RECEIVED",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "RECEIVED",
        "timestamps": {"event_utc": scx_recv.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SCExpert/Connect", "host": "scxconnect-01"}
    })
    # HTTP ACK
    ack_time = base + timedelta(seconds=15 + flow["transport_latency_sec"])
    if flow["transport_failure"] and flow["transport_checkpoint"] == "SCXCONNECT_HTTP_ACK":
        code = flow["transport_reason"]
        http_status = 504 if code == "HTTP_504" else 500 if code == "HTTP_500" else 401 if code == "HTTP_401" else 413
        events.append({
            "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "SCXCONNECT_HTTP_ACK",
            "correlation_id": cid,
            "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
            "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
            "status": "FAIL", "reason_code": code,
            "http": {"status_code": http_status, "latency_ms": flow["transport_latency_sec"] * 1000},
            "timestamps": {"event_utc": ack_time.isoformat().replace("+00:00", "Z")},
            "endpoint": {"system": "SCExpert/Connect", "host": "scxconnect-01"}
        })
        return events

    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "SCXCONNECT_HTTP_ACK",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": "OK", "reason_code": "HTTP_204",
        "http": {"status_code": 204, "latency_ms": flow["transport_latency_sec"] * 1000},
        "timestamps": {"event_utc": ack_time.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SCExpert/Connect", "host": "scxconnect-01"}
    })
    # WMS ingested (can be false-success later)
    ingest_time = ack_time + timedelta(seconds=random.randint(1, 5))
    ingest_status = "OK"
    ingest_reason = "INGESTED"
    if flow.get("false_success_ingest_fail"):
        ingest_status = "FAIL"
        ingest_reason = "WMS_INGEST_FAILED"
    events.append({
        "city": "Dream-City", "layer": "TECH", "route": route, "checkpoint": "WMS_INGESTED",
        "correlation_id": cid,
        "sap_idoc": {"idoc_type": idoc_type, "number": idoc, "plant": plant},
        "message": {"direction": "OUTBOUND", "payload_hash": payload_hash, "schema": schema},
        "status": ingest_status, "reason_code": ingest_reason,
        "timestamps": {"event_utc": ingest_time.isoformat().replace("+00:00", "Z")},
        "endpoint": {"system": "SCExpert/WMS", "host": "wms-01"}
    })
    return events


def make_business_event(flow):
    cid = flow["correlation_id"]
    sent = flow["order_sent_utc"]
    if not flow["transport_ok"]:
        # business event may still exist as "not sent" depending on where it failed
        return {
            "city": "Dream-City", "layer": "BUSINESS", "process": "ORDER_TO_WMS", "correlation_id": cid,
            "order": {"sap_order": flow["sap_order"], "items": flow["items"]},
            "wms_response": {"status": "NONE"},
            "sla": {"response_due_seconds": flow["sla_due_sec"], "actual_response_seconds": None, "breach": True},
            "status": "FAIL",
            "reason_code": "NOT_SENT_DUE_TECH_FAILURE",
            "timestamps": {"order_sent_utc": sent.isoformat().replace("+00:00", "Z")}
        }
    # Transport ok but maybe ingest fail or no response
    if flow.get("false_success_ingest_fail"):
        return {
            "city": "Dream-City", "layer": "BUSINESS", "process": "ORDER_TO_WMS", "correlation_id": cid,
            "order": {"sap_order": flow["sap_order"], "items": flow["items"]},
            "wms_response": {"status": "NONE"},
            "sla": {"response_due_seconds": flow["sla_due_sec"],
                    "actual_response_seconds": flow["sla_due_sec"] + random.randint(60, 600), "breach": True},
            "status": "FAIL",
            "reason_code": "WMS_INGEST_FAILED_AFTER_HTTP_204",
            "timestamps": {"order_sent_utc": sent.isoformat().replace("+00:00", "Z")}
        }
    if flow["business_outcome"] == "NO_RESPONSE":
        return {
            "city": "Dream-City", "layer": "BUSINESS", "process": "ORDER_TO_WMS", "correlation_id": cid,
            "order": {"sap_order": flow["sap_order"], "items": flow["items"]},
            "wms_response": {"status": "NONE"},
            "sla": {"response_due_seconds": flow["sla_due_sec"],
                    "actual_response_seconds": flow["sla_due_sec"] + random.randint(1, 3600), "breach": True},
            "status": "FAIL", "reason_code": "NO_WMS_RESPONSE",
            "timestamps": {"order_sent_utc": sent.isoformat().replace("+00:00", "Z")}
        }
    # Otherwise WMS responded
    responded = sent + timedelta(seconds=flow["business_resp_sec"])
    breach = flow["business_resp_sec"] > flow["sla_due_sec"]
    base = {
        "city": "Dream-City", "layer": "BUSINESS", "process": "ORDER_TO_WMS", "correlation_id": cid,
        "order": {"sap_order": flow["sap_order"], "items": flow["items"]},
        "sla": {"response_due_seconds": flow["sla_due_sec"], "actual_response_seconds": flow["business_resp_sec"],
                "breach": breach},
        "timestamps": {"order_sent_utc": sent.isoformat().replace("+00:00", "Z"),
                       "wms_responded_utc": responded.isoformat().replace("+00:00", "Z")}
    }
    if flow["business_outcome"] == "OK":
        base["wms_response"] = {"status": "CONFIRMED", "items": flow["confirmed_items"]}
        base["status"] = "OK" if not breach else "FAIL"
        base["reason_code"] = "FULL_CONFIRM" if not breach else "SLA_BREACH_LATE_RESPONSE"
        return base
    if flow["business_outcome"] == "PARTIAL":
        base["wms_response"] = {"status": "PARTIAL", "items": flow["confirmed_items"]}
        base["status"] = "DEGRADED" if not breach else "FAIL"
        base["reason_code"] = "QTY_MISMATCH_PARTIAL" if not breach else "SLA_BREACH_PARTIAL_LATE"
        return base
    if flow["business_outcome"] == "REJECT":
        base["wms_response"] = {"status": "REJECTED", "detail": flow["reject_detail"], "items": flow["confirmed_items"]}
        base["status"] = "FAIL"
        base["reason_code"] = flow["reject_code"] if not breach else f"SLA_BREACH_{flow['reject_code']}"
        return base
    if flow["business_outcome"] == "CONFIRMED_GT":
        base["wms_response"] = {"status": "CONFIRMED", "items": flow["confirmed_items"]}
        base["status"] = "FAIL"
        base["reason_code"] = "CONFIRMED_GT_REQUESTED"
        return base
    if flow["business_outcome"] == "UOM_MISMATCH":
        base["wms_response"] = {"status": "REJECTED", "detail": "UoM mismatch EA vs PCS",
                                "items": flow["confirmed_items"]}
        base["status"] = "FAIL"
        base["reason_code"] = "UOM_MISMATCH"
        return base
    return base


def make_rollup(flow, tech_last, biz_evt):
    # Determine health
    tech_health = "GREEN"
    if tech_last["status"] != "OK":
        tech_health = "RED"
    elif any(e.get("status") == "FAIL" and e.get("checkpoint") == "WMS_INGESTED" for e in flow["tech_events"]):
        tech_health = "RED"
    biz_health = "GREEN"
    if biz_evt["status"] in ["DEGRADED"]:
        biz_health = "AMBER"
    if biz_evt["status"] == "FAIL":
        biz_health = "RED"
    overall = "GREEN"
    if "RED" in (tech_health, biz_health):
        overall = "RED"
    elif "AMBER" in (tech_health, biz_health):
        overall = "AMBER"
    sla_state = "OK"
    if biz_evt["sla"]["breach"]:
        sla_state = "BREACH"
    elif biz_evt["sla"]["actual_response_seconds"] is not None and biz_evt["sla"]["actual_response_seconds"] > \
            biz_evt["sla"]["response_due_seconds"] * 0.8:
        sla_state = "AT_RISK"
    return {
        "city": "Dream-City",
        "correlation_id": flow["correlation_id"],
        "route": "SAP->PO->FW->SCXConnect->WMS",
        "sap_idoc": {"idoc_type": idoc_type, "number": flow["sap_idoc_number"], "plant": flow["plant"]},
        "order": {"sap_order": flow["sap_order"], "items": flow["items"]},
        "tech": {"health": tech_health, "last_checkpoint": tech_last["checkpoint"], "last_status": tech_last["status"],
                 "reason_code": tech_last.get("reason_code")},
        "business": {"health": biz_health, "status": biz_evt["status"], "reason_code": biz_evt.get("reason_code")},
        "sla": {"state": sla_state, **biz_evt["sla"]},
        "timestamps": {"order_sent_utc": flow["order_sent_utc"].isoformat().replace("+00:00", "Z")}
    }


# Generate flows
flows = []
tech_events = []
business_events = []
rollups = []

for i in range(1, n_flows + 1):
    sent = rand_dt(start_utc, end_utc - timedelta(minutes=10))
    cid = corr_id(i, sent)
    idoc = idoc_number(i)
    order = sap_order(i)
    plant = random.choice(plants)
    schema = random.choices(schemas, weights=[0.75, 0.15, 0.10])[0]
    # items: 1-3 lines
    n_items = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
    items = []
    for _ in range(n_items):
        sku = random.choice(skus)
        qty = random.randint(1, 12)
        item = {"sku": sku, "qty_requested": qty}
        # sometimes include uom
        if random.random() < 0.05:
            item["uom"] = random.choice(["EA", "PCS"])
        items.append(item)
    payload_hash = make_hash(cid + json.dumps(items, sort_keys=True))
    sla_due = random.choice([60, 120, 180])  # seconds

    transport_failure, t_reason, t_checkpoint = pick_failure()
    # add some HTTP-level failures when transport_failure but checkpoint is SCXCONNECT_HTTP_ACK
    transport_ok = not transport_failure

    transport_latency = random.randint(80, 450) / 100.0  # 0.8 to 4.5 sec

    # chance of false-success: http 204 but WMS ingest fails (rare)
    false_success = (transport_ok and random.random() < 0.0008)

    flow = {
        "correlation_id": cid,
        "sap_idoc_number": idoc,
        "sap_order": order,
        "plant": plant,
        "schema": schema,
        "items": items,
        "payload_hash": payload_hash[:16],
        "sla_due_sec": sla_due,
        "order_sent_utc": sent,
        "transport_failure": transport_failure,
        "transport_reason": t_reason,
        "transport_checkpoint": t_checkpoint,
        "transport_latency_sec": int(transport_latency * 1000) // 1000,  # int-ish seconds for logs
        "transport_ok": transport_ok,
        "false_success_ingest_fail": false_success
    }
    # Determine business outcome
    if not transport_ok:
        flow["business_outcome"] = "NOT_SENT"
    else:
        r = random.random()
        outcome = "OK"
        if r < p_no_response:
            outcome = "NO_RESPONSE"
        elif r < p_no_response + p_uom_mismatch:
            outcome = "UOM_MISMATCH"
        elif r < p_no_response + p_uom_mismatch + p_confirm_gt_req:
            outcome = "CONFIRMED_GT"
        elif r < p_no_response + p_uom_mismatch + p_confirm_gt_req + p_reject:
            outcome = "REJECT"
        elif r < p_no_response + p_uom_mismatch + p_confirm_gt_req + p_reject + p_partial:
            outcome = "PARTIAL"
        else:
            outcome = "OK"
        # response time
        if outcome == "NO_RESPONSE":
            flow["business_resp_sec"] = None
        else:
            # base response within SLA often, but allow late outcomes too
            if random.random() < p_late:
                flow["business_resp_sec"] = sla_due + random.randint(10, 240)
            else:
                flow["business_resp_sec"] = random.randint(5, max(10, sla_due - 5))
        flow["business_outcome"] = outcome

    # Confirmed items / reject details
    confirmed_items = []
    if flow["transport_ok"] and not flow.get("false_success_ingest_fail") and flow["business_outcome"] not in [
        "NO_RESPONSE"]:
        for it in items:
            req = it["qty_requested"]
            if flow["business_outcome"] == "PARTIAL":
                conf = max(0, req - random.randint(1, req))  # ensure less or equal
                if conf == req: conf = max(0, req - 1)
            elif flow["business_outcome"] == "CONFIRMED_GT":
                conf = req + random.randint(1, 3)
            elif flow["business_outcome"] == "REJECT":
                conf = 0
            elif flow["business_outcome"] == "UOM_MISMATCH":
                conf = 0
            else:
                conf = req
            ci = {"sku": it["sku"], "qty_confirmed": conf}
            if "uom" in it:
                ci["uom"] = it["uom"]
            confirmed_items.append(ci)
    flow["confirmed_items"] = confirmed_items

    if flow["business_outcome"] == "REJECT":
        flow["reject_code"], flow["reject_detail"] = random.choice([
            ("SKU_UNKNOWN", "Unknown SKU"),
            ("BLOCKED_SHIP_TO", "Invalid ship-to / blocked customer"),
            ("VALIDATION_ERROR", "Mandatory field missing in order"),
        ])
    elif flow["business_outcome"] == "UOM_MISMATCH":
        flow["reject_code"], flow["reject_detail"] = ("UOM_MISMATCH", "UoM mismatch EA vs PCS")

    # Generate events
    tevents = make_tech_events(flow)
    flow["tech_events"] = tevents
    tech_events.extend(tevents)

    bevt = make_business_event(flow)
    business_events.append(bevt)

    tech_last = tevents[-1]
    rollups.append(make_rollup(flow, tech_last, bevt))
    flows.append(flow)

# Output files for mongoimport
out_dir = "/mnt/data"
tech_path = os.path.join(out_dir, "dream_city_tech_events.jsonl")
biz_path = os.path.join(out_dir, "dream_city_business_events.jsonl")
rollup_path = os.path.join(out_dir, "dream_city_rollup_flows.jsonl")


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            # Ensure Mongo friendly: ISO strings already, no datetime objects
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


write_jsonl(tech_path, tech_events)
write_jsonl(biz_path, business_events)
write_jsonl(rollup_path, rollups)

# Also provide JSON array variants
tech_array_path = os.path.join(out_dir, "dream_city_tech_events.array.json")
biz_array_path = os.path.join(out_dir, "dream_city_business_events.array.json")
rollup_array_path = os.path.join(out_dir, "dream_city_rollup_flows.array.json")

with open(tech_array_path, "w", encoding="utf-8") as f:
    json.dump(tech_events, f, ensure_ascii=False)
with open(biz_array_path, "w", encoding="utf-8") as f:
    json.dump(business_events, f, ensure_ascii=False)
with open(rollup_array_path, "w", encoding="utf-8") as f:
    json.dump(rollups, f, ensure_ascii=False)

(len(flows), len(tech_events), len(business_events), len(rollups), tech_path, biz_path, rollup_path)