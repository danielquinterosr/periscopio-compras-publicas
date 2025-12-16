let DATA = [];

function fmtCLP(x) {
  if (x === null || x === undefined) return "—";
  try { return Math.round(x).toLocaleString("es-CL"); } catch { return String(x); }
}

function render(rows) {
  const tbody = document.querySelector("#tbl tbody");
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td><strong>${r.score}</strong></td>
      <td>${r.title || ""}</td>
      <td>${r.buyer || ""}</td>
      <td>${fmtCLP(r.amount_clp)}</td>
      <td>${r.published_at || ""}</td>
      <td>${r.close_at || ""}</td>
      <td>${r.url ? `<a href="${r.url}" target="_blank" rel="noopener">Abrir</a>` : "—"}</td>
    </tr>
  `).join("");
  document.getElementById("countShown").textContent = rows.length;
}

function applyFilters() {
  const q = (document.getElementById("q").value || "").toLowerCase();
  const minScore = parseInt(document.getElementById("minScore").value || "3", 10);

  const rows = DATA.filter(r => {
    const hay = `${r.title || ""} ${r.buyer || ""}`.toLowerCase();
    return r.score >= minScore && (q === "" || hay.includes(q));
  });

  render(rows);
}

async function init() {
  const meta = await fetch("data/meta.json", { cache: "no-store" }).then(r => r.json());

  // Mostrar en hora Chile
  try {
    document.getElementById("lastUpdate").textContent =
      new Date(meta.last_update_iso).toLocaleString("es-CL", { timeZone: "America/Santiago" });
  } catch {
    document.getElementById("lastUpdate").textContent = meta.last_update_iso || "—";
  }

  DATA = await fetch("data/opportunities.json", { cache: "no-store" }).then(r => r.json());
  render(DATA);

  document.getElementById("q").addEventListener("input", applyFilters);
  document.getElementById("minScore").addEventListener("input", applyFilters);
}

init();
