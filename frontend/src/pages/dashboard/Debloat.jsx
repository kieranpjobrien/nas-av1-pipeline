import { useEffect, useState } from "react";
import { api } from "../../lib/api";

const fmtSize = (gb) => (gb >= 1024 ? `${(gb / 1024).toFixed(2)} TB` : `${Math.round(gb)} GB`);

function fmtEta(s) {
  if (s == null) return "";
  if (s >= 3600) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  if (s >= 60) return `${Math.floor(s / 60)}m ${s % 60}s`;
  return `${s}s`;
}

const btn = (accent) => ({
  padding: "4px 10px",
  fontSize: 11,
  fontWeight: 700,
  fontFamily: "inherit",
  letterSpacing: "0.04em",
  textTransform: "uppercase",
  cursor: "pointer",
  border: `1px solid ${accent ? "var(--accent)" : "var(--border)"}`,
  background: accent ? "rgba(34,211,120,0.08)" : "transparent",
  color: accent ? "var(--accent)" : "var(--ink-2)",
  borderRadius: 4,
  display: "inline-flex",
  alignItems: "center",
  gap: 6,
});

const deBloatTag = {
  background: "var(--accent)",
  color: "#06281e",
  borderColor: "var(--accent)",
};

/**
 * De-bloat reclaim — mirrors the pipeline's "In flight" card but for the
 * standalone AV1->AV1 reclaim: live progress, start/pause/stop controls,
 * tagged "de-bloat" to distinguish from the convert pipeline. Self-contained
 * (polls /api/reclaim); decoupled from the pipeline state DB.
 */
export function DebloatCard() {
  const [r, setR] = useState(null);
  const [busy, setBusy] = useState(false);
  const refresh = () => api.getReclaim().then(setR).catch(() => {});
  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 3000);
    return () => clearInterval(t);
  }, []);

  if (!r || (r.reclaimed === 0 && !r.running && r.flagged === 0)) return null;
  const ip = r.in_progress;
  const ctl = async (fn) => {
    setBusy(true);
    try {
      await fn();
    } catch {
      /* errors surface via the network tab; keep the card alive */
    }
    setTimeout(refresh, 700);
    setBusy(false);
  };

  return (
    <div className="card" style={{ marginBottom: 16 }}>
      <h3 style={{ display: "flex", alignItems: "center", gap: 8 }}>
        De-bloat reclaim
        <span className="tag" style={deBloatTag}>de-bloat</span>
        <span className="tag">{r.paused ? "paused" : r.running ? "live" : "idle"}</span>
        <span className="count">
          {r.reclaimed} done · {fmtSize(r.saved_gb)} banked · {r.flagged} flagged
        </span>
        <span style={{ marginLeft: "auto", display: "inline-flex", gap: 6 }}>
          {!r.running && (
            <button disabled={busy} style={btn(true)} onClick={() => ctl(() => api.startProcess("reclaim"))}>
              ▶ Start
            </button>
          )}
          {r.running && !r.paused && (
            <button disabled={busy} style={btn(false)} onClick={() => ctl(api.reclaimPause)}>
              ⏸ Pause
            </button>
          )}
          {r.running && r.paused && (
            <button disabled={busy} style={btn(true)} onClick={() => ctl(api.reclaimResume)}>
              ▶ Resume
            </button>
          )}
          {r.running && (
            <button disabled={busy} style={btn(false)} onClick={() => ctl(() => api.killProcess("reclaim"))}>
              ⏹ Stop
            </button>
          )}
        </span>
      </h3>

      {ip ? (
        <div style={{ marginBottom: 6 }}>
          <div
            style={{ display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 13, marginBottom: 5 }}
          >
            <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
              <span className="tag" style={deBloatTag}>de-bloat</span>
              <b>{(ip.name || "").replace(/\.mkv$/i, "")}</b>
            </span>
            <span className="mono" style={{ fontSize: 11, color: "var(--ink-3)" }}>
              {ip.phase}
              {ip.cap ? ` · → ${ip.cap}` : ""}
              {ip.speed ? ` · ${ip.speed}` : ""}
              {ip.eta_s != null ? ` · ETA ${fmtEta(ip.eta_s)}` : ""}
            </span>
          </div>
          <div className="codec-bar-wrap">
            <div
              className="codec-bar"
              style={{
                width: `${ip.progress_pct ?? (r.running ? 4 : 0)}%`,
                background: "var(--accent)",
                opacity: ip.progress_pct == null ? 0.4 : 1,
                transition: "width 0.6s",
              }}
            />
          </div>
          {ip.progress_pct != null && (
            <div className="mono" style={{ fontSize: 10, color: "var(--ink-4)", marginTop: 3 }}>{ip.progress_pct}%</div>
          )}
        </div>
      ) : (
        <div style={{ fontSize: 12, color: "var(--ink-3)", padding: "6px 0" }}>
          {r.paused
            ? "Paused — will stop at the next film boundary (the current encode finishes first)."
            : r.running
              ? "Working…"
              : "Idle. Start to keep banking space — can't run alongside the convert pipeline (one NVENC)."}
        </div>
      )}
    </div>
  );
}
