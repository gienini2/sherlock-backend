/**
 * FRONTEND PATCH — translateText() + renderitzat de marcadors
 * ============================================================
 *
 * Substitueix la funció translateText() i les funcions de renderitzat
 * a index_dev_viejo.html
 *
 * CANVIS RESPECTE LA VERSIÓ ANTERIOR:
 *   - Una sola crida a /api/translate (ja no crida Sherlock directament)
 *   - La resposta ja conté texto_drag + anotaciones + entidades
 *   - El text mostrat al editor és text_drag NET (sense marcadors [[...]])
 *   - Les anotaciones contenen start/end sobre el text net
 *   - Pestanya BD usa entidades (ja no cal cridar /explain)
 *
 * COLORS:
 *   EXACTO  → blau    (#cce5ff)  dada verificada 100%
 *   PARCIAL → taronja (#fff3cd)  coincidència parcial, revisar
 */

// ============================================================================
// FUNCIÓ PRINCIPAL — substitueix l'anterior translateText()
// ============================================================================

async function translateText() {
  const mode        = document.getElementById("modeSelect").value;
  const originalText = document.getElementById("textOriginal").value.trim();

  if (!originalText) {
    showStatus("Escriu o dicta alguna cosa primer", "error");
    return;
  }

  document.getElementById("loader").classList.add("show");
  document.getElementById("translateBtn").disabled = true;
  showStatus("Processant...", "info");

  try {
    // UNA SOLA CRIDA — Bitàcola fa de proxy cap a Sherlock
    const response = await fetch(
      "https://bitacola-backend.onrender.com/api/translate",
      {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          text:    originalText,
          mode:    mode,
          user_id: localStorage.getItem("user_id"),
        }),
      }
    );

    logUsage("translate");

    if (!response.ok) {
      const err = await response.json();
      showStatus(`Error: ${err.error}`, "error");
      return;
    }

    const data = await response.json();
    // data = { texto_coloquial, texto_drag, anotaciones, entidades, ms }

    const hora = new Date().toLocaleTimeString("ca-ES", {
      hour: "2-digit", minute: "2-digit"
    });

    // Substituir [HORA] si Claude l'ha deixat al text
    const textoDRAG = data.texto_drag.replace(/\[HORA\]/g, hora);

    // ----------------------------------------------------------------
    // Pintar el text amb les anotacions al textarea visible
    // ----------------------------------------------------------------
    const textoHTML = pintarTextoAnotado(textoDRAG, data.anotaciones);

    // Mostrar al div de previsualització (readonly)
    const preview = document.getElementById("textTechnicalPreview");
    if (preview) {
      preview.innerHTML = textoHTML;
      preview.style.display = "block";
    }

    // Al textarea editable posem el text net (sense HTML)
    document.getElementById("textTechnical").value = textoDRAG;
    document.getElementById("textTechnical").style.display = "block";

    // ----------------------------------------------------------------
    // Pestanya BD
    // ----------------------------------------------------------------
    renderizarPanelDB_v2(data.entidades);

    // ----------------------------------------------------------------
    // Tipificació penal (async, no bloqueja la UI)
    // ----------------------------------------------------------------
    analizarPenal(textoDRAG);

    // ----------------------------------------------------------------
    // Mostrar seccions
    // ----------------------------------------------------------------
    document.getElementById("technicalGroup").style.display = "block";
    document.getElementById("addBtnGroup").style.display    = "grid";
    document.getElementById("finalBtnGroup").style.display  = "grid";

    showStatus(`Anàlisi completada en ${data.ms}ms`, "success");

  } catch (e) {
    console.error(e);
    showStatus("Error en l'anàlisi: " + e.message, "error");
  } finally {
    document.getElementById("loader").classList.remove("show");
    document.getElementById("translateBtn").disabled = false;
  }
}


// ============================================================================
// PINTAR ANOTACIONS AL TEXT
// ============================================================================

function pintarTextoAnotado(texto, anotaciones) {
  if (!anotaciones || anotaciones.length === 0) return texto;

  // Ordenar DESC per posició per no trencar índexs
  const sorted = [...anotaciones].sort((a, b) => b.start - a.start);

  let html = texto;

  for (const ann of sorted) {
    const antes   = html.substring(0, ann.start);
    const entitat = html.substring(ann.start, ann.end);
    const despres = html.substring(ann.end);

    // Color i tooltip segons match
    const config = {
      "EXACTO":  { cls: "entity-exact",   icon: "✓", color: "#cce5ff", border: "#0066cc" },
      "PARCIAL": { cls: "entity-partial", icon: "⚠", color: "#fff3cd", border: "#e6a817" },
    }[ann.match] || { cls: "entity-none", icon: "", color: "transparent", border: "transparent" };

    if (!config.cls || ann.match === "SIN_COINCIDENCIA") {
      // Sense match → no pintar
      continue;
    }

    const tooltip = ann.texto_enriquecido !== ann.texto_original
      ? `${config.icon} ${ann.texto_enriquecido} | Historial: ${ann.historial_count} actuacions`
      : `${config.icon} ${ann.type} | Historial: ${ann.historial_count} actuacions`;

    html = `${antes}<span
      class="${config.cls}"
      data-id="${ann.id}"
      data-type="${ann.type}"
      data-match="${ann.match}"
      data-enriquecido="${ann.texto_enriquecido}"
      title="${tooltip}"
      style="background:${config.color}; border-bottom:2px solid ${config.border}; padding:1px 3px; border-radius:2px; cursor:pointer;"
    >${entitat}</span>${despres}`;
  }

  return html;
}


// ============================================================================
// PESTANYA BD — substitueix renderizarPanelDB()
// ============================================================================

function renderizarPanelDB_v2(entidades) {
  const panel = document.getElementById("panelDB");
  panel.innerHTML = "";

  // Recollir tots els matches amb db_record
  const matches = [
    ...( entidades.personas    || []),
    ...( entidades.vehiculos   || []),
    ...( entidades.ubicaciones || []),
  ].filter(m => m.db_record && m.match_type !== "SIN_COINCIDENCIA");

  if (matches.length === 0) {
    panel.innerHTML = "<p style='color:#666; padding:10px;'>✔ No consten coincidències a la base de dades.</p>";
    return;
  }

  matches.forEach(m => {
    const db         = m.db_record;
    const enrich     = m.enrichment || {};
    const confidence = Math.round((m.confidence || 0) * 100);
    const aparicions = enrich.apariciones_previas || 0;

    // Calcular nivell de risc simple
    const risc      = aparicions >= 5 ? "ALTO" : aparicions >= 2 ? "MEDIO" : "BAJO";
    const riscColor = { ALTO: "#dc3545", MEDIO: "#ffc107", BAJO: "#198754" }[risc];

    // Nom/identificador principal
    let titol = "";
    if (db.nombre)    titol = `${db.nombre} ${db.apellidos || ""}`.trim();
    else if (db.plate) titol = db.plate;
    else if (db.canonical_name) titol = db.canonical_name;

    // Dades secundàries
    const dadesHTML = Object.entries(db)
      .filter(([k, v]) => v && !["nombre","apellidos"].includes(k))
      .map(([k, v]) => `<div><strong>${k}:</strong> ${v}</div>`)
      .join("");

    // Historial (si hi ha roles o vehicles relacionats)
    const rols    = (enrich.roles_previos || []).join(", ");
    const vehicles = (enrich.vehiculos_relacionados || [])
      .map(v => v.plate).join(", ");

    const card = document.createElement("div");
    card.style.cssText = `
      background:white; border:2px solid #dee2e6; border-radius:10px;
      padding:16px; margin-bottom:12px;
    `;
    card.innerHTML = `
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
        <div>
          <strong style="font-size:15px; color:#1b263b;">${titol}</strong>
          <div style="font-size:12px; color:#778da9;">
            ${m.match_type} · Confiança ${confidence}%
          </div>
        </div>
        <div style="background:${riscColor}; color:white; padding:6px 12px;
             border-radius:6px; font-size:12px; font-weight:700;">
          ${risc}
        </div>
      </div>
      <div style="background:#f8f9fa; padding:10px; border-radius:6px; font-size:13px; margin-bottom:10px;">
        ${dadesHTML}
      </div>
      ${aparicions > 0 ? `
      <div style="font-size:13px; color:#495057; border-top:1px solid #dee2e6; padding-top:10px;">
        📋 <strong>${aparicions}</strong> aparicions prèvies
        ${rols    ? `· Rols: ${rols}`         : ""}
        ${vehicles ? `· Vehicles: ${vehicles}` : ""}
      </div>` : ""}
    `;
    panel.appendChild(card);
  });
}


// ============================================================================
// TIPIFICACIÓ PENAL — crida al penal_backend i renderitza el panel PENAL
// ============================================================================

async function analizarPenal(textoDRAG) {
  const panelPenal = document.getElementById("panelPenal");
  if (!panelPenal) return; // si no existeix el panel, skip

  panelPenal.innerHTML = "<p style='color:#888; padding:10px;'>⏳ Analitzant tipificació penal...</p>";

  try {
    const response = await fetch(
      "https://penal-backend.onrender.com/api/v1/penal/analyze",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ texto: textoDRAG }),
      }
    );

    if (!response.ok) {
      panelPenal.innerHTML = "<p style='color:#dc3545; padding:10px;'>⚠ Error en el servei penal.</p>";
      return;
    }

    const penal = await response.json();
    renderizarPanelPenal(penal);

  } catch (e) {
    console.error("Error penal:", e);
    panelPenal.innerHTML = "<p style='color:#dc3545; padding:10px;'>⚠ No s'ha pogut connectar amb el tipificador penal.</p>";
  }
}


function renderizarPanelPenal(penal) {
  const panel = document.getElementById("panelPenal");
  if (!panel) return;

  // Cas: no és penal
  if (!penal.is_penal) {
    panel.innerHTML = `
      <div style="background:#d4edda; border:1px solid #c3e6cb; border-radius:8px; padding:16px; color:#155724;">
        <strong>✔ Sense indicis penals</strong>
        <div style="font-size:12px; margin-top:4px; color:#555;">
          Score gate: ${(penal.score_gate || 0).toFixed(4)}
        </div>
      </div>`;
    return;
  }

  // Cas: és penal
  const resultats = penal.resultats || [];
  const modul     = penal.modulo    || "desconegut";
  const scoreGate = (penal.score_gate || 0).toFixed(4);
  const confRouter = ((penal.confidence_router || 0) * 100).toFixed(0);

  const COLORS = {
    0.9:  "#dc3545",  // molt alt
    0.75: "#e67e22",  // alt
    0.6:  "#f39c12",  // mitjà-alt
    0.5:  "#f1c40f",  // mitjà
  };

  function getColor(score) {
    if (score >= 0.9)  return COLORS[0.9];
    if (score >= 0.75) return COLORS[0.75];
    if (score >= 0.6)  return COLORS[0.6];
    return COLORS[0.5];
  }

  // Nom llegible del mòdul
  const MODUL_NOMS = {
    "patrimoni":           "Delictes contra el Patrimoni",
    "patrimonio":          "Delictes contra el Patrimoni",
    "vida_integridad":     "Delictes contra la Vida i Integritat",
    "libertad":            "Delictes contra la Llibertat",
    "orden_publico":       "Ordre Públic",
    "seguridad_colectiva": "Seguretat Col·lectiva",
    "Indemnidad_sexual":   "Indemnitat Sexual",
    "delitos_funcionarios":"Delictes de Funcionaris",
    "medio_ambiente":      "Medi Ambient",
  };
  const modulNom = MODUL_NOMS[modul] || modul;

  let html = `
    <div style="margin-bottom:12px; padding:10px; background:#fff3cd; border-radius:8px; border:1px solid #ffc107;">
      <strong style="color:#856404;">⚖ Possible infracció penal detectada</strong>
      <div style="font-size:12px; color:#666; margin-top:4px;">
        Mòdul: <strong>${modulNom}</strong> · Confiança router: ${confRouter}% · Score gate: ${scoreGate}
      </div>
    </div>`;

  if (resultats.length === 0) {
    html += `<p style="color:#888; font-size:13px;">Cap tipificació concreta superada el llindar.</p>`;
  } else {
    resultats.forEach(r => {
      const color = getColor(r.score);
      const pct   = Math.round(r.score * 100);
      // Nom llegible del delicte (eliminar prefix del mòdul)
      const delicteNom = r.delito
        .replace(/^[^-]+-/, "")   // eliminar prefix "patrimonio-" etc.
        .replace(/_/g, " ")
        .replace(/\b\w/g, c => c.toUpperCase());

      html += `
        <div style="background:white; border:2px solid ${color}; border-radius:8px; padding:12px; margin-bottom:8px; display:flex; justify-content:space-between; align-items:center;">
          <div>
            <strong style="color:#1b263b; font-size:14px;">${delicteNom}</strong>
            <div style="font-size:11px; color:#778da9; margin-top:2px;">${r.delito}</div>
          </div>
          <div style="background:${color}; color:white; padding:6px 14px; border-radius:20px; font-size:13px; font-weight:700; min-width:54px; text-align:center;">
            ${pct}%
          </div>
        </div>`;
    });
  }

  // Mòduls addicionals avaluats
  if (penal.modulos_evaluados && penal.modulos_evaluados.length > 1) {
    const altres = penal.modulos_evaluados.slice(1);
    html += `<div style="font-size:11px; color:#888; margin-top:8px;">
      Altres mòduls avaluats: ${altres.map(m => `${MODUL_NOMS[m.modulo] || m.modulo} (${(m.confidence*100).toFixed(0)}%)`).join(" · ")}
    </div>`;
  }

  panel.innerHTML = html;
}
