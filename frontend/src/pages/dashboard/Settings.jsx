import { useEffect, useState } from "react";
import { api } from "../../lib/api";

const LOCAL_DEFAULTS = {
  bazarrUrl: "http://192.168.4.42:6767",
};

function SettingsRow({ label, hint, children }) {
  return (
    <div className="s-row">
      <div className="s-label">
        <div className="s-l">{label}</div>
        {hint && <div className="s-h">{hint}</div>}
      </div>
      <div className="s-ctrl">{children}</div>
    </div>
  );
}

function ConfigSummary({ effective }) {
  if (!effective) return <div style={{ fontSize: 12, color: "var(--ink-3)" }}>Loading config…</div>;

  const rows = [
    ["Video codec", effective.video_codec],
    ["Max staging bytes", effective.max_staging_bytes ? `${(effective.max_staging_bytes / 1024 ** 3).toFixed(0)} GB` : "—"],
    ["Max fetch buffer", effective.max_fetch_buffer_bytes ? `${(effective.max_fetch_buffer_bytes / 1024 ** 3).toFixed(0)} GB` : "—"],
    ["Min free space", effective.min_free_space_bytes ? `${(effective.min_free_space_bytes / 1024 ** 3).toFixed(0)} GB` : "—"],
  ];
  if (effective.cq?.movie?.["1080p"] != null) {
    rows.push(["CQ · movie · 1080p", effective.cq.movie["1080p"]]);
    rows.push(["CQ · series · 1080p", effective.cq.series?.["1080p"] ?? "—"]);
    rows.push(["CQ · 4K HDR · movie", effective.cq.movie["4K_HDR"] ?? "—"]);
  }
  if (effective.nvenc_preset?.movie?.["1080p"]) {
    rows.push(["NVENC preset · 1080p movie", effective.nvenc_preset.movie["1080p"]]);
  }

  return (
    <dl className="ins-grid" style={{ fontSize: 12 }}>
      {rows.map(([k, v]) => (
        <div key={k} style={{ display: "contents" }}>
          <dt>{k}</dt>
          <dd>{String(v)}</dd>
        </div>
      ))}
    </dl>
  );
}

export function Settings() {
  const [tab, setTab] = useState("encoder");
  const [config, setConfig] = useState(null);
  const [health, setHealth] = useState(null);
  const [localPrefs, setLocalPrefs] = useState(() => {
    try {
      return { ...LOCAL_DEFAULTS, ...JSON.parse(localStorage.getItem("nc.settings") || "{}") };
    } catch {
      return LOCAL_DEFAULTS;
    }
  });
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.getConfig().then(setConfig).catch(() => setConfig(null));
    api.getHealth().then(setHealth).catch(() => setHealth(null));
  }, []);

  useEffect(() => {
    localStorage.setItem("nc.settings", JSON.stringify(localPrefs));
  }, [localPrefs]);

  const setPref = (k, v) => setLocalPrefs((s) => ({ ...s, [k]: v }));

  const tabs = [
    ["encoder", "Encoder"],
    ["pipeline", "Pipeline"],
    ["integrations", "Integrations"],
    ["about", "About"],
  ];

  const openClassicEditor = () => {
    const url = new URL(window.location.href);
    url.searchParams.set("classic", "1");
    window.location.href = url.toString();
  };

  const overridesCount = config?.overrides ? Object.keys(config.overrides).length : 0;

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">Settings</div>
          <div className="page-sub">
            The full config editor lives in Classic view — this page is a read-only summary of the effective config
            plus a couple of local-only preferences (Bazarr URL). Deep edits land in{" "}
            <span className="mono" style={{ color: "var(--ink-2)" }}>control/config_overrides.json</span>.
          </div>
        </div>
        <div className="stamp">
          <div><b>Effective</b>: {config ? "loaded" : "—"}</div>
          <div><b>Overrides</b>: {overridesCount}</div>
          <div>
            <b>Pipeline</b>:{" "}
            <span
              style={{
                color:
                  health?.pipeline_status === "running" ? "var(--good)" : "var(--ink-2)",
              }}
            >
              {health?.pipeline_status ?? "—"}
            </span>
          </div>
        </div>
      </div>

      <div className="work-tabs" style={{ marginBottom: 20 }}>
        {tabs.map(([k, l]) => (
          <button key={k} className={`work-tab ${tab === k ? "on" : ""}`} onClick={() => setTab(k)}>
            {l}
          </button>
        ))}
      </div>

      <div className="card">
        {tab === "encoder" && (
          <div className="settings-form">
            <SettingsRow
              label="Effective encoder config"
              hint="Live read from pipeline/config.py + control/config_overrides.json"
            >
              <ConfigSummary effective={config?.effective} />
            </SettingsRow>
            <SettingsRow
              label="Full editor"
              hint="CQ, NVENC presets, multipass, and per-tier tuning live in Classic view"
            >
              <button className="ins-btn" onClick={openClassicEditor}>
                Open Classic editor →
              </button>
            </SettingsRow>
          </div>
        )}
        {tab === "pipeline" && (
          <div className="settings-form">
            <SettingsRow label="Effective limits" hint="Staging, fetch buffer, and free-space guards">
              <ConfigSummary effective={config?.effective} />
            </SettingsRow>
            <SettingsRow label="Pipeline process" hint={`pid ${health?.pipeline_pid ?? "—"}`}>
              <div style={{ display: "flex", gap: 8 }}>
                <button
                  className="ins-btn"
                  onClick={async () => {
                    try {
                      const r = await api.startProcess("pipeline");
                      window.notify?.({
                        kind: r?.ok === false ? "warn" : "good",
                        title: r?.ok === false ? "Already running" : "Pipeline started",
                        body: r?.error || `pid ${r?.pid ?? "?"}`,
                      });
                    } catch (e) {
                      window.notify?.({ kind: "bad", title: "Start failed", body: String(e.message || e) });
                    }
                  }}
                  disabled={health?.pipeline_status === "running"}
                >
                  Start
                </button>
                <button
                  className="ins-btn"
                  onClick={async () => {
                    try {
                      const r = await api.stopProcess("pipeline");
                      window.notify?.({
                        kind: r?.ok === false ? "warn" : "good",
                        title: r?.ok === false ? "Not running" : "Pipeline stopped",
                        body: r?.method || r?.error || "",
                      });
                    } catch (e) {
                      window.notify?.({ kind: "bad", title: "Stop failed", body: String(e.message || e) });
                    }
                  }}
                  disabled={health?.pipeline_status !== "running"}
                >
                  Stop
                </button>
              </div>
            </SettingsRow>
          </div>
        )}
        {tab === "integrations" && (
          <div className="settings-form">
            <div
              style={{
                padding: "12px 14px",
                marginBottom: 4,
                background: "rgba(122,184,224,0.05)",
                border: "1px solid rgba(122,184,224,0.2)",
                borderRadius: 8,
                fontSize: 11,
                color: "var(--ink-2)",
                lineHeight: 1.5,
              }}
            >
              Subtitle management is handled by <b style={{ color: "var(--blue)" }}>Bazarr</b>. This daemon only reads
              sidecar .srt/.ass files from disk — fetching, scoring and sync live in Bazarr.
            </div>
            <SettingsRow label="Bazarr URL" hint="Used to deep-link from Library → Fetch subs">
              <input
                type="text"
                value={localPrefs.bazarrUrl}
                onChange={(e) => setPref("bazarrUrl", e.target.value)}
                className="nc-input"
                placeholder="http://192.168.4.42:6767"
                style={{ fontFamily: "JetBrains Mono,monospace", fontSize: 12 }}
              />
            </SettingsRow>
            <SettingsRow label="Open Bazarr" hint="Launches the Bazarr web UI in a new tab">
              <a
                href={localPrefs.bazarrUrl || "#"}
                target="_blank"
                rel="noreferrer noopener"
                className="ins-btn"
                style={{ color: "var(--accent)", textDecoration: "none" }}
              >
                Open {localPrefs.bazarrUrl || "Bazarr"} →
              </a>
            </SettingsRow>
          </div>
        )}
        {tab === "about" && (
          <div className="settings-form">
            <SettingsRow label="ffmpeg">
              <span className="mono">{health?.ffmpeg_version ?? "—"}</span>
            </SettingsRow>
            <SettingsRow label="Python">
              <span className="mono">{health?.python_version ?? "—"}</span>
            </SettingsRow>
            <SettingsRow label="GPU">
              <span className="mono">
                {health?.gpu_available ? `${health.gpu_name} · ${health.gpu_temp_c ?? "?"}°C` : "not detected"}
              </span>
            </SettingsRow>
            <SettingsRow label="NAS Movies">
              <span className="mono" style={{ color: health?.nas_movies_reachable ? "var(--good)" : "var(--bad)" }}>
                {health?.nas_movies_reachable ? "reachable" : "unreachable"}
              </span>
            </SettingsRow>
            <SettingsRow label="NAS Series">
              <span className="mono" style={{ color: health?.nas_series_reachable ? "var(--good)" : "var(--bad)" }}>
                {health?.nas_series_reachable ? "reachable" : "unreachable"}
              </span>
            </SettingsRow>
            <SettingsRow
              label="Remote SSH (track strip / mkvmerge)"
              hint="Remote mkvmerge via SSH + Docker. Falls back to slow local ops if unset."
            >
              <span className="mono" style={{ display: "flex", flexDirection: "column", gap: 2, alignItems: "flex-end" }}>
                <span style={{ color: health?.nas_ssh_configured ? "var(--good)" : "var(--warn)" }}>
                  NAS · {health?.nas_ssh_configured ? health.nas_ssh_host : "unset"}
                </span>
                <span style={{ color: health?.server_ssh_configured ? "var(--good)" : "var(--warn)" }}>
                  SRV · {health?.server_ssh_configured ? health.server_ssh_host : "unset"}
                </span>
              </span>
            </SettingsRow>
            <SettingsRow label="Staging free">
              <span className="mono">
                {health?.staging_free_gb != null
                  ? `${health.staging_free_gb} GB / ${health.staging_total_gb} GB`
                  : "—"}
              </span>
            </SettingsRow>
            <SettingsRow label="Pipeline">
              <span
                className="mono"
                style={{ color: health?.pipeline_status === "running" ? "var(--good)" : "var(--ink-2)" }}
              >
                {health?.pipeline_status ?? "—"}
                {health?.pipeline_pid ? ` · pid ${health.pipeline_pid}` : ""}
              </span>
            </SettingsRow>
          </div>
        )}
      </div>

      <div style={{ display: "flex", gap: 10, marginTop: 16, justifyContent: "flex-end" }}>
        <button
          className="top-btn"
          onClick={() => {
            api
              .getConfig()
              .then((r) => {
                setConfig(r);
                window.notify?.({ kind: "info", title: "Config reloaded", body: `${Object.keys(r.overrides || {}).length} overrides active` });
              })
              .catch((e) =>
                window.notify?.({ kind: "bad", title: "Reload failed", body: String(e.message || e) })
              );
          }}
        >
          Reload config
        </button>
        <button
          className="top-btn primary"
          disabled={saving}
          onClick={async () => {
            setSaving(true);
            try {
              await api.setConfig(config?.overrides || {});
              window.notify?.({
                kind: "good",
                title: "Overrides written",
                body: "control/config_overrides.json saved — pipeline picks them up on next file.",
              });
            } catch (e) {
              window.notify?.({ kind: "bad", title: "Save failed", body: String(e.message || e) });
            } finally {
              setSaving(false);
            }
          }}
        >
          {saving ? "Saving…" : "Save overrides"}
        </button>
      </div>
    </div>
  );
}
