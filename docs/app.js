// app.js — Periscopio Radar Compras Públicas (Licitaciones + Compra Ágil)

function fmtCLP(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  try {
    return new Intl.NumberFormat("es-CL", {
      style: "currency",
      currency: "CLP",
      maximumFractionDigits: 0,
    }).format(x);
  } catch {
    return String(x);
  }
}

function fmtDT(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return new Intl.DateTimeFormat("es-CL", {
    timeZone: "America/Santiago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(d);
}

function esc(s) {
  return (s ?? "")
    .toString()
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function issueLink(repo, id, title) {
  const base = `https://github.com/${repo}/issues/new`;
  const ititle = `Reviewed: ${id}`;
  const body =
    `Marcada como revisada desde el dashboard.\n\n` +
    `- ID: ${id}\n` +
    `- Título: ${title}\n`;
  const q = new URLSearchParams({
    labels: "reviewed",
    title: ititle,
    body: body,
  });
  return `${base}?${q.toString()}`;
}

async function loadJSON(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(`Fetch failed ${path}: ${r.status}`);
  return await r.json();
}

// -----------------------------
// Estado UI
// -----------------------------
let DATA = [];
let META = null;

let VIEW = []; // datos filtrados por search
let SORT = { key: "score", dir: "desc" }; // asc/desc

// -----------------------------
// Helpers de sort robustos
// -----------------------------
function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function toStr(x) {
  return (x ?? "").toString().toLowerCase();
}

function toBoolNum(x) {
  return x ? 1 : 0;
}

function toDateNum(iso) {
  if (!iso) return 0;
  const d = new Date(iso);
  const t = d.getTime();
  return Number.isFinite(t) ? t : 0;
}

function getSourceLabel(src) {
  return src === "compra_agil" ? "Compra Ágil" : "Licitación";
}

// -----------------------------
// Render
// -----------------------------
function renderTable(rows) {
  const tbody = document.getElementById("tbody");
  tbody.innerHTML = "";

  for (const o of rows) {
    const reviewed = !!o.reviewed;
    const reviewedPill = reviewed
      ? `<span class="pill reviewed">Sí</span>`
      : `<span class="pill">No</span>`;

    const link = o.url
      ? `<a href="${esc(o.url)}" target="_blank" rel="noopener">Abrir</a>`
      : "—";

    const mark =
      META && META.repo && o.id
        ? `<a href="${esc(issueLink(META.repo, o.id, o.title || ""))}" target="_blank" rel="noopener">Marcar</a>`
        : "—";

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><span class="pill">${esc(o.score)}</span></td>
      <td>${reviewedPill}</td>
      <td>${esc(getSourceLabel(o.source))}</td>
      <td>
        <div><strong>${esc(o.title || "")}</strong></div>
        <div class="muted">${esc(o.id || "")}</div>
        <div>${link}</div>
      </td>
      <td>${esc(o.buyer || "—")}</td>
      <td>${fmtCLP(o.amount_clp)}</td>
      <td>${esc(fmtDT(o.published_at))}</td>
      <td>${esc(fmtDT(o.questions_end_at))}</td>
      <td>${esc(fmtDT(o.close_at))}</td>
      <td>${mark}</td>
    `;
    tbody.appendChild(tr);
  }
}

// -----------------------------
// Conteos (header)
// -----------------------------
function computeCounts(allRows, viewRows, metaCounts) {
  const all = allRows || [];
  const view = viewRows || [];

  const allLic = all.filter((o) => o.source !== "compra_agil");
  const allCA = all.filter((o) => o.source === "compra_agil");

  const viewLic = view.filter((o) => o.source !== "compra_agil");
  const viewCA = view.filter((o) => o.source === "compra_agil");

  const reviewedLicAll = allLic.filter((o) => o.reviewed === true).length;
  const reviewedCAAll = allCA.filter((o) => o.reviewed === true).length;

  // Si el ETL entrega totales por fuente, los usamos; si no, fallback a DATA
  const licTotal = metaCounts?.licitaciones_total ?? allLic.length;
  const caTotal = metaCounts?.compra_agil_total ?? allCA.length;

  const shownLic = viewLic.length;
  const shownCA = viewCA.length;

  // revisadas “desde issues” viene agregado (no por fuente); preferimos calcular por DATA para separar
  const licReviewed = reviewedLicAll;
  const caReviewed = reviewedCAAll;

  return {
    totals: { lic: licTotal, ca: caTotal },
    shown: { lic: shownLic, ca: shownCA },
    reviewed: { lic: licReviewed, ca: caReviewed },
    reviewed_total: licReviewed + caReviewed,
    shown_total: shownLic + shownCA,
    total_current: metaCounts?.total_current ?? (licTotal + caTotal),
  };
}

function renderHeader() {
  // Fecha de actualización
  const last = META?.last_update_iso;
  document.getElementById("lastUpdate").textContent =
    `Última actualización: ${fmtDT(last)} (Chile)`;

  const c = META?.counts || {};
  const cc = computeCounts(DATA, VIEW, c);

  // Texto con desglose por fuente
  const parts = [
    `Mostradas: ${cc.shown_total} (Licitaciones: ${cc.shown.lic} · Compra Ágil: ${cc.shown.ca})`,
    `Total activas: ${cc.total_current} (Licitaciones: ${cc.totals.lic} · Compra Ágil: ${cc.totals.ca})`,
    `Revisadas: ${cc.reviewed_total} (Licitaciones: ${cc.reviewed.lic} · Compra Ágil: ${cc.reviewed.ca})`,
  ];

  document.getElementById("counts").textContent = parts.join(" · ");
}

// -----------------------------
// Search + Sort
// -----------------------------
function applySearch() {
  const q = (document.getElementById("search").value || "").trim().toLowerCase();
  if (!q) {
    VIEW = [...DATA];
  } else {
    VIEW = DATA.filter((o) => {
      const s = `${o.id || ""} ${o.title || ""} ${o.buyer || ""}`.toLowerCase();
      return s.includes(q);
    });
  }

  // re-aplica sort sobre el VIEW
  applySort(SORT.key, SORT.dir, false);

  renderHeader();
  renderTable(VIEW);
}

function compareBy(key, a, b) {
  switch (key) {
    case "score":
      return toNum(a.score) - toNum(b.score);

    case "reviewed":
      return toBoolNum(a.reviewed) - toBoolNum(b.reviewed);

    case "source":
      return toStr(getSourceLabel(a.source)).localeCompare(toStr(getSourceLabel(b.source)), "es");

    case "title":
      return toStr(a.title).localeCompare(toStr(b.title), "es");

    case "buyer":
      return toStr(a.buyer).localeCompare(toStr(b.buyer), "es");

    case "amount_clp":
      return toNum(a.amount_clp) - toNum(b.amount_clp);

    case "published_at":
      return toDateNum(a.published_at) - toDateNum(b.published_at);

    case "questions_end_at":
      // Compra Ágil puede venir sin este campo
      return toDateNum(a.questions_end_at) - toDateNum(b.questions_end_at);

    case "close_at":
      return toDateNum(a.close_at) - toDateNum(b.close_at);

    default:
      return 0;
  }
}

function applySort(key, dir, rerender = true) {
  SORT = { key, dir };
  const mult = dir === "asc" ? 1 : -1;

  VIEW.sort((a, b) => {
    const d = compareBy(key, a, b);
    // desempate estable: por score desc, luego por fecha publicación desc, luego id
    if (d !== 0) return mult * d;

    const d2 = (toNum(a.score) - toNum(b.score));
    if (d2 !== 0) return -d2;

    const d3 = (toDateNum(a.published_at) - toDateNum(b.published_at));
    if (d3 !== 0) return -d3;

    return toStr(a.id).localeCompare(toStr(b.id), "es");
  });

  if (rerender) {
    renderHeader();
    renderTable(VIEW);
  }
}

function setupSorting() {
  // Mapea columnas del thead a keys (en el mismo orden del HTML)
  const keys = [
    "score",
    "reviewed",
    "source",
    "title",
    "buyer",
    "amount_clp",
    "published_at",
    "questions_end_at",
    "close_at",
    null, // Acción no ordena
  ];

  const ths = document.querySelectorAll("#tbl thead th");
  ths.forEach((th, idx) => {
    const key = keys[idx];
    if (!key) return;

    th.style.cursor = "pointer";
    th.title = "Ordenar";

    th.addEventListener("click", () => {
      const isSame = SORT.key === key;
      const nextDir = isSame && SORT.dir === "desc" ? "asc" : "desc";
      applySort(key, nextDir, true);
    });
  });
}

// -----------------------------
// Main
// -----------------------------
async function main() {
  META = await loadJSON("data/meta.json");
  DATA = await loadJSON("data/opportunities.json");

  // VIEW inicial = DATA completa
  VIEW = [...DATA];

  // Sorting habilitado
  setupSorting();

  // Sort inicial (score desc)
  applySort("score", "desc", false);

  // Header + tabla
  renderHeader();
  renderTable(VIEW);

  // Search
  document.getElementById("search").addEventListener("input", applySearch);
}

main().catch((err) => {
  console.error(err);
  const tbody = document.getElementById("tbody");
  tbody.innerHTML = `<tr><td colspan="10">Error cargando datos: ${esc(err.message)}</td></tr>`;
});
