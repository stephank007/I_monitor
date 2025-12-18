var dagfuncs = window.dashAgGridFunctions = window.dashAgGridFunctions || {};

// Tree path: YYYY-MM -> Category -> Account -> Payee (leaf)
dagfuncs.getDataPathSap = function (data) {
    if (!data) return [];
    return [data.sap_order, data.node_type];
};

dagfuncs.rowStyleOverall = function (params) {
    // Leaf rows (FLOW/TECH/BUSINESS/SLA) have params.data
    if (params && params.data && params.data.overall) {
        const s = params.data.overall;
        if (s === "RED")   return { backgroundColor: "#fdecea" };
        if (s === "AMBER") return { backgroundColor: "#fff4e5" };
        if (s === "GREEN") return { backgroundColor: "#edf7ed" };
        return {};
    }

    // Group rows (sap_order) often have no data, so compute from children
    if (params && params.node && params.node.group) {
        const kids = params.node.allLeafChildren || [];
        let worst = "GREEN";

        for (let i = 0; i < kids.length; i++) {
            const d = kids[i] && kids[i].data ? kids[i].data : null;
            const s = d ? d.overall : null;

            if (s === "RED") { worst = "RED"; break; }
            if (s === "AMBER") worst = "AMBER";
        }

        if (worst === "RED")   return { backgroundColor: "#fdecea" };
        if (worst === "AMBER") return { backgroundColor: "#fff4e5" };
        if (worst === "GREEN") return { backgroundColor: "#edf7ed" };
    }

    return {};
};

// Text icon only (no HTML)
dagfuncs.rowStatusIconText = function (params) {
    const s = params && params.data ? params.data.row_status : null;
    if (s === "RED") return "✖";
    if (s === "AMBER") return "⚠";
    if (s === "GREEN") return "✔";
    return "";
};

// Style the icon (this is what adds the color!)
dagfuncs.rowStatusIconCellStyle = function (params) {
    const s = params && params.data ? params.data.row_status : null;

    const base = {
        textAlign: "center",
        fontSize: "18px",
        fontWeight: "800",
        lineHeight: "20px",
    };

    if (s === "RED")   return { ...base, color: "#d32f2f" };
    if (s === "AMBER") return { ...base, color: "#f9a825" };
    if (s === "GREEN") return { ...base, color: "#2e7d32" };

    return base;
};


// Overall icon formatter: RED/AMBER/GREEN -> ✖ / ⚠ / ✔
dagfuncs.overallIcon = function (params) {
    const v = params && params.value ? params.value : "";
    if (v === "RED") return "✖";
    if (v === "AMBER") return "⚠";
    if (v === "GREEN") return "✔";
    return v;
};

// Make the icon bigger + centered

dagfuncs.overallIconStyle = function () {
    return {
        textAlign : "center",
        fontSize  : "18px",
        lineHeight: "20px",
    };
};

dagfuncs.rowStatusIcon = function (params) {
    const s = params && params.data ? params.data.row_status : null;

    if (s === "RED") {
        return { html: '<span style="color:#d32f2f; font-weight:700;">✖</span>' };
    }
    if (s === "AMBER") {
        return { html: '<span style="color:#f9a825; font-weight:700;">⚠</span>' };
    }
    if (s === "GREEN") {
        return { html: '<span style="color:#2e7d32; font-weight:700;">✔</span>' };
    }
    return { html: "" };
};


/*
dagfuncs.rowStatusIcon = function (params) {
    const s = params && params.data ? params.data.row_status : null;
    if (s === "RED") return "✖";
    if (s === "AMBER") return "⚠";
    if (s === "GREEN") return "✔";
    return "";
};
*/

dagfuncs.rowStyleByRowStatus = function (params) {
    // Leaf rows: color by row_status (TECH/BUSINESS/SLA are independent)
    if (params && params.data && params.data.row_status) {
        const s = params.data.row_status;
        if (s === "RED")   return { backgroundColor: "#fdecea" };
        if (s === "AMBER") return { backgroundColor: "#fff4e5" };
        if (s === "GREEN") return { backgroundColor: "#edf7ed" };
        return {};
    }

    // Group rows: compute worst from children.row_status
    if (params && params.node && params.node.group) {
        const kids = params.node.allLeafChildren || [];
        let worst = "GREEN";
        for (let i = 0; i < kids.length; i++) {
            const d = kids[i] && kids[i].data ? kids[i].data : null;
            const s = d ? d.row_status : null;
            if (s === "RED") { worst = "RED"; break; }
            if (s === "AMBER") worst = "AMBER";
        }
        if (worst === "RED")   return { backgroundColor: "#fdecea" };
        if (worst === "AMBER") return { backgroundColor: "#fff4e5" };
        if (worst === "GREEN") return { backgroundColor: "#edf7ed" };
    }

    return {};
};
