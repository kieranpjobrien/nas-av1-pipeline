import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import { fmtNum, libraryOf } from "./helpers";

export function Storage({ data }) {
  const [historySummary, setHistorySummary] = useState(null);
  useEffect(() => {
    api.getHistorySummary().then(setHistorySummary).catch(() => {});
  }, []);

  const summary = data?.summary || {};
  const totalTb = summary.total_size_gb ? (summary.total_size_gb / 1024).toFixed(1) : null;
  const totalFiles = summary.total_files || 0;
  const reclaimedGb = historySummary?.totals?.saved_bytes
    ? historySummary.totals.saved_bytes / 1024 ** 3
    : null;
  const remainingSizeGb = data?.remainingSizeGb || 0;
  const projectedTb = remainingSizeGb ? (remainingSizeGb * 0.42) / 1024 : null;

  const files = data?.topTargets || [];
  const libMap = new Map();
  for (const f of files) {
    const lib = f.library || libraryOf(f.filepath || "") || "Other";
    const prev = libMap.get(lib) || { name: lib, path: "", files: 0, size: 0 };
    prev.files += 1;
    prev.size += f.size_gb || 0;
    if (!prev.path && f.filepath) {
      prev.path = (f.filepath.match(/^[^\\/]+[\\/](?:[^\\/]+[\\/])?/) || [""])[0].replace(/[\\/]+$/, "");
    }
    libMap.set(lib, prev);
  }
  const libs = [...libMap.values()].sort((a, b) => b.size - a.size);
  const maxLibSize = libs.reduce((m, l) => Math.max(m, l.size), 0) || 1;

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">Storage</div>
          <div className="page-sub">
            Disk pools, library paths, and reclaim projections. Cache fills up during encodes — keep at least 20%
            headroom or the array will spill.
          </div>
        </div>
        <div className="stamp">
          <div><b>Indexed</b>: {fmtNum(totalFiles)} files</div>
          <div><b>Size</b>: {totalTb ? `${totalTb} TB` : "—"}</div>
        </div>
      </div>

      <div className="kpis">
        <div className="kpi">
          <div className="kpi-label">Library size</div>
          <div className="kpi-value">
            {totalTb ?? "—"}
            {totalTb && <span className="unit">TB</span>}
          </div>
          <div className="kpi-sub"><span className="mono">{fmtNum(totalFiles)} files</span></div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Reclaimed</div>
          <div className="kpi-value" style={{ color: "var(--accent)" }}>
            {reclaimedGb != null ? (reclaimedGb / 1024).toFixed(2) : "—"}
            {reclaimedGb != null && <span className="unit">TB</span>}
          </div>
          <div className="kpi-sub">
            <span className="mono">{reclaimedGb != null ? "avg 41% per file" : "no encodes yet"}</span>
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Projected reclaim</div>
          <div className="kpi-value">
            {projectedTb != null ? `~${projectedTb.toFixed(1)}` : "—"}
            {projectedTb != null && <span className="unit">TB</span>}
          </div>
          <div className="kpi-sub"><span className="mono">if remaining HEVC/H.264 re-encoded</span></div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Cache pressure</div>
          <div className="kpi-value" style={{ color: "var(--ink-3)" }}>—</div>
          <div className="kpi-sub">
            <span className="mono">host agent not wired</span>
          </div>
        </div>
      </div>

      <div className="card" style={{ marginBottom: 20 }}>
        <h3>
          Disk pools <span className="count">requires host agent</span>
        </h3>
        <div style={{ fontSize: 12, color: "var(--ink-3)", lineHeight: 1.6 }}>
          Disk pool metrics aren't available — this view will light up once the host agent is wired to{" "}
          <span className="mono">df</span> or the NAS admin API. Until then, use the live telemetry strip in the
          topbar for host load and the KPIs above for library size.
        </div>
      </div>

      <div className="card">
        <h3>
          Libraries by path <span className="count">{libs.length} {libs.length === 1 ? "path" : "paths"}</span>
        </h3>
        {libs.length === 0 && (
          <div style={{ padding: "20px 0", fontSize: 12, color: "var(--ink-3)" }}>No library files indexed yet.</div>
        )}
        <div>
          {libs.map((l) => (
            <div key={l.name} className="codec-row">
              <div style={{ width: 140 }}>
                <div style={{ fontSize: 13, fontWeight: 500 }}>{l.name}</div>
                {l.path && (
                  <div className="mono" style={{ fontSize: 10, color: "var(--ink-4)" }}>{l.path}</div>
                )}
              </div>
              <div className="codec-bar-wrap">
                <div
                  className="codec-bar"
                  style={{ width: (l.size / maxLibSize) * 100 + "%", background: "var(--blue)" }}
                />
              </div>
              <div className="codec-count">{(l.size / 1024).toFixed(2)} TB</div>
              <div className="codec-pct">{fmtNum(l.files)}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
