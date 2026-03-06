// ============================================================================
// PATCH index.html — substituir les funcions analizarPenal i renderizarPanelDB
// Aplicar dins el bloc <script> de index.html
// ============================================================================

// ============================================================================
// PANEL BD — mostra múltiples candidats PARCIAL per persona
//            fitxa: nom, DNI, adreça, data naix, aparicions, % similitud
// ============================================================================
function renderizarPanelDB(entidades) {
    const panel = document.getElementById("panelDB");
    panel.innerHTML = "";

    const matches = [
        ...(entidades.personas    || []),
        ...(entidades.vehiculos   || []),
        ...(entidades.ubicaciones || []),
    ].filter(m => m.match_type !== "SIN_COINCIDENCIA");

    if (!matches.length) {
        panel.innerHTML = "<p style='color:#666;padding:10px;'>✔ No consten coincidències a la base de dades.</p>";
        return;
    }

    // Agrupar PARCIAL de persones per 'texto' (nom original extret)
    // Els vehicles/ubicacions EXACTO els mostrem directament
    const grups = {};
    matches.forEach(m => {
        const clau = `${m.match_type}__${m.texto}`;
        if (!grups[clau]) grups[clau] = { match_type: m.match_type, texto: m.texto, items: [] };
        grups[clau].items.push(m);
    });

    Object.values(grups).forEach(grup => {
        if (grup.match_type === "PARCIAL" && grup.items.length > 1) {
            // Bloc multi-candidat per a persones
            renderCardMultiCandidats(panel, grup.texto, grup.items);
        } else {
            // Bloc single (EXACTO vehicle/ubicació o PARCIAL únic)
            grup.items.forEach(m => renderCardSingle(panel, m));
        }
    });
}

// Targeta per a múltiples candidats PARCIAL d'una mateixa persona extreta
function renderCardMultiCandidats(panel, nomOriginal, items) {
    const wrapper = document.createElement("div");
    wrapper.style.cssText = "background:#fff8e1;border:2px solid #ffc107;border-radius:10px;padding:16px;margin-bottom:16px;";
    wrapper.innerHTML = `
        <div style="margin-bottom:10px;">
            <strong style="color:#856404;font-size:15px;">⚠ "${nomOriginal}" — ${items.length} candidats trobats</strong>
            <div style="font-size:12px;color:#666;margin-top:3px;">Selecciona el candidat correcte o descarta'ls tots</div>
        </div>`;

    items.forEach((m, idx) => {
        const rec   = m.db_record || {};
        const conf  = Math.round((m.confidence || 0) * 100);
        const apar  = (m.enrichment || {}).apariciones_previas || 0;
        const risc  = apar >= 5 ? "ALTO" : apar >= 2 ? "MEDIO" : "BAJO";
        const rcol  = {ALTO:"#dc3545", MEDIO:"#ffc107", BAJO:"#198754"}[risc];

        const nom   = rec.nombre    ? `${rec.nombre} ${rec.apellidos||""}`.trim() : "—";
        const dni   = rec.dni       || "—";
        const dir   = rec.direccion || "—";
        const naix  = rec.fecha_nacimiento || "—";
        const tel   = rec.telefono  || "—";

        const card = document.createElement("div");
        card.style.cssText = "background:white;border:1px solid #dee2e6;border-radius:8px;padding:12px;margin-bottom:8px;";
        card.innerHTML = `
            <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px;">
                <div>
                    <strong style="font-size:14px;color:#1b263b;">${nom}</strong>
                    <span style="font-size:12px;color:#778da9;margin-left:8px;">Similitud: ${conf}%</span>
                </div>
                <div style="background:${rcol};color:white;padding:4px 10px;border-radius:5px;font-size:11px;font-weight:700;">${risc}</div>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:12px;color:#495057;margin-bottom:8px;">
                <div><strong>DNI:</strong> ${dni}</div>
                <div><strong>Data naix:</strong> ${naix}</div>
                <div><strong>Adreça:</strong> ${dir}</div>
                <div><strong>Tel:</strong> ${tel}</div>
                ${apar > 0 ? `<div style="grid-column:1/-1;color:#6c757d;">📋 ${apar} aparicions prèvies</div>` : ""}
            </div>
            <button onclick="this.closest('[data-grup]').querySelectorAll('.candidat-card').forEach(c=>c.style.border='1px solid #dee2e6');this.closest('.candidat-card').style.border='3px solid #198754';showStatus('Candidat seleccionat: ${nom.replace(/'/g,"\\'")}','success');"
                style="background:#e9ecef;border:none;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;font-weight:600;">
                ✓ Seleccionar aquest candidat
            </button>`;
        card.classList.add("candidat-card");
        wrapper.appendChild(card);
    });
    wrapper.setAttribute("data-grup", nomOriginal);
    panel.appendChild(wrapper);
}

// Targeta single (vehicle, ubicació, o persona amb un sol candidat)
function renderCardSingle(panel, m) {
    const rec   = m.db_record || {};
    const conf  = Math.round((m.confidence || 0) * 100);
    const apar  = (m.enrichment || {}).apariciones_previas || 0;
    const risc  = apar >= 5 ? "ALTO" : apar >= 2 ? "MEDIO" : "BAJO";
    const rcol  = {ALTO:"#dc3545", MEDIO:"#ffc107", BAJO:"#198754"}[risc];

    let titol = "";
    if (rec.nombre)         titol = `${rec.nombre} ${rec.apellidos||""}`.trim();
    else if (rec.plate)     titol = rec.plate;
    else if (rec.canonical_name) titol = rec.canonical_name;
    else                    titol = m.texto || "—";

    const dades = Object.entries(rec)
        .filter(([k,v]) => v && !["nombre","apellidos"].includes(k))
        .map(([k,v]) => `<div><strong>${k}:</strong> ${v}</div>`)
        .join("");

    const rols  = ((m.enrichment||{}).roles_previos||[]).join(", ");
    const vehs  = ((m.enrichment||{}).vehiculos_relacionados||[]).map(v=>v.plate).join(", ");
    const badge = m.match_type === "EXACTO"
        ? `<span style="background:#0066cc;color:white;padding:2px 8px;border-radius:4px;font-size:11px;">EXACTO</span>`
        : `<span style="background:#e6a817;color:white;padding:2px 8px;border-radius:4px;font-size:11px;">PARCIAL</span>`;

    const card = document.createElement("div");
    card.style.cssText = "background:white;border:2px solid #dee2e6;border-radius:10px;padding:16px;margin-bottom:12px;";
    card.innerHTML = `
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
            <div>
                <strong style="font-size:15px;color:#1b263b;">${titol}</strong>
                <div style="font-size:12px;color:#778da9;margin-top:3px;">${badge} · Confiança ${conf}%</div>
            </div>
            <div style="background:${rcol};color:white;padding:6px 12px;border-radius:6px;font-size:12px;font-weight:700;">${risc}</div>
        </div>
        <div style="background:#f8f9fa;padding:10px;border-radius:6px;font-size:13px;margin-bottom:10px;">${dades || "<em style='color:#aaa'>Sense dades addicionals</em>"}</div>
        ${apar > 0 ? `
        <div style="font-size:13px;color:#495057;border-top:1px solid #dee2e6;padding-top:10px;">
            📋 <strong>${apar}</strong> aparicions prèvies
            ${rols  ? `· Rols: ${rols}`         : ""}
            ${vehs  ? `· Vehicles: ${vehs}`      : ""}
        </div>` : ""}`;
    panel.appendChild(card);
}


// ============================================================================
// PANEL PENAL — FIX: variable textoOriginal → originalText
// ============================================================================
async function analizarPenal(textoOriginal) {   // ← paràmetre renombrat correctament
    const panelPenal = document.getElementById("panelPenal");
    if (!panelPenal) return;

    panelPenal.innerHTML = "<p style='color:#888;padding:10px;'>⏳ Analitzant tipificació penal...</p>";

    try {
        const response = await fetch(
            "https://penal-backend.onrender.com/api/v1/penal/analyze",
            {
                method:  "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ texto_coloquial: textoOriginal }),   // ← variable correcta
            }
        );

        if (!response.ok) {
            panelPenal.innerHTML = "<p style='color:#dc3545;padding:10px;'>⚠ Error en el servei penal.</p>";
            return;
        }

        const penal = await response.json();
        renderizarPanelPenal(penal);

    } catch (e) {
        console.error("Error penal:", e);
        panelPenal.innerHTML = "<p style='color:#dc3545;padding:10px;'>⚠ No s'ha pogut connectar amb el tipificador penal.</p>";
    }
}
