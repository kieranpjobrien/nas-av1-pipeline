import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import { prettyTitle } from "./helpers";

// Each in-flight file is one "worker slot" — we don't track worker IDs at the backend.
function deriveSlots(pipelineData) {
  const files = pipelineData?.files || {};
  return Object.entries(files)
    .filter(([, info]) =>
      ["encoding", "analyzing", "fetching", "uploading", "processing"].includes(
        (info.status || "").toLowerCase()
      )
    )
    .map(([path, info]) => ({
      path,
      filename: path.split(/[\\/]/).pop(),
      status: (info.status || "").toLowerCase(),
      stage: info.stage || info.reason || null,
      tier: info.tier || null,
      error: info.error || null,
    }));
}

export function Workers({ pipelineData }) {
  const slots = deriveSlots(pipelineData);
  const [gpu, setGpu] = useState(null);
  const [host, setHost] = useState(null);
  const [config, setConfig] = useState(null);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      const [g, h, c] = await Promise.all([
        api.getGpu().catch(() => null),
        api.getHostStats().catch(() => null),
        api.getConfig().catch(() => null),
      ]);
      if (cancelled) return;
      setGpu(g);
      setHost(h);
      setConfig(c);
    };
    tick();
    const id = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const configuredWorkers = config?.effective?.max_concurrent_workers ?? null;

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">Workers</div>
          <div className="page-sub">
            One slot per in-flight file. The pipeline schedules work against the effective config
            (<span className="mono" style={{ color: "var(--ink-2)" }}>max_concurrent_workers</span>
            {configuredWorkers != null ? ` = ${configuredWorkers}` : ""}).
          </div>
        </div>
        <div className="stamp">
          <div><b>Active</b>: {slots.length}{configuredWorkers != null ? `/${configuredWorkers}` : ""}</div>
          {gpu?.available && (
            <div>
              <b>GPU</b>: {gpu.gpu_util}% · enc {gpu.encoder_util}%
            </div>
          )}
          {host?.available && (
            <div>
              <b>CPU</b>: {host.cpu_pct.toFixed(0)}% · {host.cpu_count} threads
            </div>
          )}
        </div>
      </div>

      <div className="active-list">
        {slots.length === 0 && (
          <div className="active" style={{ opacity: 0.6, textAlign: "center", padding: "28px 16px" }}>
            <div style={{ fontSize: 13, color: "var(--ink-2)", marginBottom: 4 }}>No active workers</div>
            <div style={{ fontSize: 11, color: "var(--ink-3)" }}>
              Pipeline is idle — use Run next batch or start the pipeline process.
            </div>
          </div>
        )}
        {slots.map((s) => (
          <div key={s.path} className={`active ${s.error ? "err" : ""}`}>
            <div className="active-top">
              <div className="active-title" title={s.filename}>
                {prettyTitle(s.filename)}
              </div>
              <div className="active-meta">
                {[s.status, s.tier].filter(Boolean).join(" · ")}
              </div>
            </div>
            <div className="active-foot">
              <span>{s.stage || s.status}</span>
              <span className="mono">{s.path}</span>
            </div>
          </div>
        ))}
      </div>

      {gpu?.available && (
        <div className="card" style={{ marginTop: 16 }}>
          <h3>
            GPU <span className="count">{gpu.name}</span>
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 20 }}>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                Utilisation
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {gpu.gpu_util}%
              </div>
            </div>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                Encoder
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {gpu.encoder_util}%
              </div>
            </div>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                Temp
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {gpu.temp_c}°C
              </div>
            </div>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                VRAM
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {(gpu.mem_used_mb / 1024).toFixed(1)}/{(gpu.mem_total_mb / 1024).toFixed(0)} GB
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
