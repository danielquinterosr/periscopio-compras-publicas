function fmtCLP(x) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  try {
    return new Intl.NumberFormat("es-CL", { style: "currency", currency: "CLP", maximumFractionDigits: 0 }).format(x);
  } catch {
    return String(x);
  }
}

function fmtDT(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return new Intl.DateTimeFormat("es-CL", {
    timeZone: "America/Santiago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  }).format(d);
}

function esc(s) {
  return (s ?? "").toString()
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function issueLink(repo, id, title) {
  // Crea issue prellenado con label reviewed
  const base = `https://github.com/${repo}/issues/new`;
  const ititle = `Reviewed: ${id}`;
  const body = `Marcada como revisada desde el dashboard.\n\n- ID: ${id}\n- Título: ${title}\n`;
  const q = new URLSearchParams({
    labels: "reviewed",
    title: ititle,
    body: body
  });
  return `${base}?${q.toString()}`;
}

async function loadJSON(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(`Fetch failed ${path}: ${r.status}`);
  return await r.json();
}

let DATA = [];
let META = null;

function renderTable(rows) {
  const tbody = document.getElementById("tbody");
  tbody.innerHTML = "";

  for (const o of rows) {
    const reviewed = !!o.reviewed;
    const reviewedPill = reviewed
      ? `<span class="pill reviewed">Sí</span>`
      : `<span class="pill">No</span>`;

    const link = o.url ? `<a href="${esc(o.url)}" target="_blank" rel="noopener">Abrir</a>` : "—";

    const mark = (META && META.repo && o.id)
      ? `<a href="${esc(issueLink(META.repo, o.id, o.title))}" target="_blank" rel="noopener">Marcar</a>`
      : "—";

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><span class="pill">${esc(o.score)}</span></td>
      <td>${reviewedPill}</td>
      <td>
        <div><strong>${esc(o.title)}</strong></div>
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

function applySearch() {
  const q = (document.getElementById("search").value || "").trim().toLowerCase();
  if (!q) {
    renderTable(DATA);
    return;
  }
  const filtered = DATA.filter(o => {
    const s = `${o.id || ""} ${o.title || ""} ${o.buyer || ""}`.toLowerCase();
    return s.includes(q);
  });
  renderTable(filtered);
}

async function main() {
  META = await loadJSON("data/meta.json");
  DATA = await loadJSON("data/opportunities.json");

  // Header
  document.getElementById("lastUpdate").textContent =
    `Última actualización: ${fmtDT(META.last_update_iso)} (Chile)`;

  const c = META.counts || {};
  document.getElementById("counts").textContent =
    `Mostradas: ${c.licitaciones_mostradas ?? DATA.length} · Total activas: ${c.licitaciones_total ?? "—"} · Revisadas: ${c.reviewed_ids ?? "—"}`;

  renderTable(DATA);

  // Search
  document.getElementById("search").addEventListener("input", () => applySearch());
}

main().catch(err => {
  console.error(err);
  const tbody = document.getElementById("tbody");
  tbody.innerHTML = `<tr><td colspan="9">Error cargando datos: ${esc(err.message)}</td></tr>`;
});
