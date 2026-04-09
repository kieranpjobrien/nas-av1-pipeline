import { useState, useEffect } from "react";
import { PALETTE } from "../theme";
import { usePolling } from "../lib/usePolling";
import { api } from "../lib/api";
import { SectionTitle } from "../components/SectionTitle";
import { StatCard } from "../components/StatCard";
import { ProcessRow } from "../components/ProcessRow";
import { PauseButton, BTN_BASE } from "../components/PauseButton";
import { PathEditor } from "../components/PathEditor";
import { GentleEditor } from "../components/GentleEditor";
import { MediaSearch } from "../components/MediaSearch";
import { SettingsEditor } from "../components/SettingsEditor";

// CQ values: lower = better quality, higher = more compression
// Base CQ table from config.py
const CQ_TABLE = {
  movie:  { "4K HDR": 22, "4K SDR": 27, "1080p": 28, "720p": 30, "480p": 30 },
  series: { "4K HDR": 24, "4K SDR": 30, "1080p": 30, "720p": 32, "480p": 32 },
};
const PROFILE_OFFSETS = { protected: -3, baseline: 0, lossy: 6 };

const CQ_MARKERS = [
  { cq: 19, label: "Nolan 4K HDR", desc: "Protected profile — Interstellar, Oppenheimer", colour: PALETTE.green, side: "top" },
  { cq: 22, label: "Movies 4K HDR", desc: "Baseline — high quality reference", colour: PALETTE.accent, side: "bottom" },
  { cq: 25, label: "Movies 1080p (protected)", desc: "Visually important films at 1080p", colour: PALETTE.green, side: "top" },
  { cq: 27, label: "Movies 4K SDR", desc: "Baseline 4K without HDR", colour: PALETTE.accent, side: "bottom" },
  { cq: 28, label: "Movies 1080p", desc: "Standard movie encode", colour: PALETTE.accent, side: "top" },
  { cq: 30, label: "Series 1080p", desc: "Standard series — The Wire, Succession", colour: "#8b8bef", side: "bottom" },
  { cq: 34, label: "Sitcoms 1080p", desc: "Lossy profile — Seinfeld, Friends", colour: PALETTE.accentWarm, side: "top" },
  { cq: 36, label: "Series 720p (lossy)", desc: "Lossy 720p — older shows, reality TV", colour: PALETTE.accentWarm, side: "bottom" },
];

function CQGuide() {
  const minCQ = 16, maxCQ = 42;
  const range = maxCQ - minCQ;
  const pct = (cq) => ((cq - minCQ) / range) * 100;
  const mono = { fontFamily: "'JetBrains Mono', monospace" };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 4 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 11, color: PALETTE.textMuted }}>
        <span>← Higher quality (larger files)</span>
        <span>More compression (smaller files) →</span>
      </div>

      {/* Scale */}
      <div style={{ position: "relative", height: 140, marginBottom: 12 }}>
        {/* Track */}
        <div style={{
          position: "absolute", left: 0, right: 0, top: 60, height: 8,
          borderRadius: 4, overflow: "hidden",
          background: `linear-gradient(90deg, ${PALETTE.green} 0%, ${PALETTE.accent} 40%, ${PALETTE.accentWarm} 80%, ${PALETTE.red} 100%)`,
          opacity: 0.3,
        }} />

        {/* CQ tick marks */}
        {Array.from({ length: (maxCQ - minCQ) / 2 + 1 }, (_, i) => minCQ + i * 2).map((cq) => (
          <div key={cq} style={{
            position: "absolute", left: `${pct(cq)}%`, top: 56, width: 1, height: cq % 10 === 0 ? 16 : 8,
            background: PALETTE.textMuted, opacity: 0.3,
          }}>
            {cq % 10 === 0 && (
              <div style={{ position: "absolute", top: 20, left: -8, fontSize: 9, color: PALETTE.textMuted, ...mono }}>{cq}</div>
            )}
          </div>
        ))}

        {/* Markers */}
        {CQ_MARKERS.map((m, i) => {
          const isTop = m.side === "top";
          return (
            <div key={i} style={{ position: "absolute", left: `${pct(m.cq)}%`, top: isTop ? 0 : 76, transform: "translateX(-50%)" }}>
              {/* Connector line */}
              <div style={{
                position: "absolute",
                left: "50%", width: 1,
                background: m.colour, opacity: 0.5,
                ...(isTop ? { bottom: 0, height: 20 } : { top: 0, height: 16 }),
              }} />
              {/* Label */}
              <div style={{
                whiteSpace: "nowrap", fontSize: 10,
                ...(isTop ? { marginBottom: 20 } : { marginTop: 16 }),
              }}>
                <span style={{ ...mono, color: m.colour, fontSize: 11, fontWeight: 700 }}>CQ {m.cq}</span>
                <span style={{ color: PALETTE.text, marginLeft: 4, fontSize: 10 }}>{m.label}</span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Profile legend */}
      <div style={{ display: "flex", gap: 16, fontSize: 11, color: PALETTE.textMuted, borderTop: `1px solid ${PALETTE.border}`, paddingTop: 12 }}>
        <span><span style={{ color: PALETTE.green, fontWeight: 700 }}>●</span> Protected (CQ −3) — reference films, epics</span>
        <span><span style={{ color: PALETTE.accent, fontWeight: 700 }}>●</span> Baseline (CQ ±0) — standard encode</span>
        <span><span style={{ color: PALETTE.accentWarm, fontWeight: 700 }}>●</span> Lossy (CQ +6) — sitcoms, reality TV</span>
      </div>
    </div>
  );
}

export function ControlPage({ wsControl }) {
  // Use WebSocket control data if available, fall back to polling
  const { data: polledStatus, refresh } = usePolling(api.getControlStatus, 3000, { enabled: !wsControl });
  const status = wsControl ? { pause_state: wsControl.pause_state } : polledStatus;
  const [skip, setSkip] = useState(null);
  const [priority, setPriority] = useState(null);
  const [gentle, setGentle] = useState(null);
  const [reencode, setReencode] = useState(null);
  const [flash, setFlash] = useState(null);

  useEffect(() => {
    api.getSkip().then(setSkip);
    api.getGentle().then(setGentle);
    api.getReencode().then(setReencode);
    // Load priority and auto-clean completed items
    Promise.all([api.getPriority(), api.getPipeline()]).then(([prio, pipeline]) => {
      const paths = prio?.paths || [];
      if (paths.length === 0 || !pipeline?.files) {
        setPriority(prio);
        return;
      }
      const doneStatuses = ["completed", "replaced", "done", "verified", "skipped"];
      const remaining = paths.filter((p) => {
        const info = pipeline.files[p];
        return !info || !doneStatuses.includes((info.status || "").toLowerCase());
      });
      if (remaining.length < paths.length) {
        api.setPriority(remaining).then(() => {
          setPriority({ paths: remaining });
          const n = paths.length - remaining.length;
          setFlash(`Cleared ${n} completed item${n !== 1 ? "s" : ""} from priority`);
          setTimeout(() => setFlash(null), 3000);
        });
      } else {
        setPriority(prio);
      }
    });
  }, []);

  const pauseState = status?.pause_state || "running";
  const isRunning = pauseState === "running";

  const showFlash = (msg) => {
    setFlash(msg);
    setTimeout(() => setFlash(null), 3000);
  };

  const handlePause = async (type) => {
    const stateForType = { all: "paused_all", fetch: "paused_fetch", encode: "paused_encode" };
    if (pauseState === stateForType[type]) {
      await api.resume();
      showFlash("Resumed");
    } else {
      await api.pause(type);
      showFlash(`Paused ${type}`);
    }
    refresh();
  };

  const handleResume = async () => {
    await api.resume();
    showFlash("Resumed -- pipeline will continue");
    refresh();
  };

  const handleSearchAdd = async (list, paths, extra) => {
    if (list === "skip") {
      const current = skip?.paths || [];
      const merged = [...new Set([...current, ...paths])];
      await api.setSkip(merged);
      setSkip({ paths: merged });
      showFlash(`Added ${paths.length} to skip list`);
    } else if (list === "priority") {
      const current = priority?.paths || [];
      const merged = [...new Set([...current, ...paths])];
      await api.setPriority(merged);
      setPriority({ paths: merged });
      showFlash(`Added ${paths.length} to priority list`);
    } else if (list === "gentle") {
      const current = gentle || { paths: {}, patterns: {}, default_offset: 0 };
      const updated = { ...current };
      updated.paths = { ...updated.paths };
      for (const p of paths) {
        if (!updated.paths[p]) {
          updated.paths[p] = { cq_offset: 2 };
        }
      }
      await api.setGentle(updated);
      setGentle(updated);
      showFlash(`Added ${paths.length} to gentle overrides`);
    } else if (list === "reencode") {
      const cq = extra?.cq || 30;
      const current = reencode?.files || {};
      const updated = { ...current };
      for (const p of paths) {
        updated[p] = { cq };
      }
      const patterns = reencode?.patterns || {};
      await api.setReencode(updated, patterns);
      setReencode({ files: updated, patterns });
      showFlash(`Added ${paths.length} to re-encode list (CQ ${cq})`);
    }
  };

  const STATE_LABELS = {
    running: { text: "Running", colour: PALETTE.green },
    paused_all: { text: "Paused", colour: PALETTE.red },
    paused_fetch: { text: "Fetch Paused", colour: PALETTE.accentWarm },
    paused_encode: { text: "Encode Paused", colour: PALETTE.accentWarm },
  };
  const stateInfo = STATE_LABELS[pauseState] || { text: "Unknown", colour: PALETTE.textMuted };

  return (
    <div>
      {/* Process Management */}
      <SectionTitle>Process Management</SectionTitle>

      {/* Core */}
      <ProcessRow name="scanner" label="Media Scanner" startLabel="Rescan Library" onFlash={showFlash} />
      <ProcessRow name="pipeline" label="AV1 Pipeline (Full Gamut)" startLabel="Start Pipeline" onFlash={showFlash} />
      <ProcessRow name="gap_filler" label="Gap Filler (Cleanup)" startLabel="Run Gap Filler" onFlash={showFlash} />

      {/* Backfill — run these before gap filler for best results */}
      <div style={{ marginTop: 16, marginBottom: 4, color: PALETTE.textMuted, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 1 }}>Backfill</div>
      <ProcessRow name="tmdb_enrich" label="TMDb Metadata (report + files)" startLabel="Enrich" onFlash={showFlash} />
      <ProcessRow name="detect_languages" label="Language Detection + Apply (Text + OCR)" startLabel="Detect + Apply" onFlash={showFlash} />
      <ProcessRow name="detect_languages_whisper" label="Language Detection + Apply (Whisper)" startLabel="Detect + Apply (GPU)" onFlash={showFlash} badge="uses GPU" />

      {/* Diagnostics */}
      <div style={{ marginTop: 16, marginBottom: 4, color: PALETTE.textMuted, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 1 }}>Diagnostics</div>
      <ProcessRow name="integrity" label="Integrity Check" startLabel="Run Check" onFlash={showFlash} />

      {/* Plex */}
      <div style={{ marginTop: 16, marginBottom: 4, color: PALETTE.textMuted, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 1 }}>Plex</div>
      <ProcessRow name="plex_sync" label="Plex Sync (Scan + Audit + Apply)" startLabel="Sync Plex" onFlash={showFlash} />

      {/* Collections */}
      <div style={{ marginTop: 16, marginBottom: 4, color: PALETTE.textMuted, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 1 }}>Collections</div>
      <ProcessRow name="rewatchables" label="The Rewatchables" startLabel="Sync Collection" onFlash={showFlash} />

      <div style={{ marginBottom: 32 }} />

      {/* Pipeline Control */}
      <SectionTitle>Pipeline Control</SectionTitle>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 24 }}>
        <div style={{ width: 16, height: 16, borderRadius: "50%", background: stateInfo.colour }} />
        <span style={{ color: stateInfo.colour, fontSize: 20, fontWeight: 700 }}>{stateInfo.text}</span>
      </div>

      {flash && (
        <div style={{ background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`, borderRadius: 8, padding: "10px 16px", marginBottom: 16, color: PALETTE.text, fontSize: 13 }}>
          {flash}
        </div>
      )}

      <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 32 }}>
        <PauseButton label="PAUSE ALL" active={pauseState === "paused_all"} colour={PALETTE.accentWarm} onClick={() => handlePause("all")} />
        <PauseButton label="PAUSE FETCHING" active={pauseState === "paused_fetch"} colour={PALETTE.accent} onClick={() => handlePause("fetch")} />
        <PauseButton label="PAUSE ENCODING" active={pauseState === "paused_encode"} colour={PALETTE.accent} onClick={() => handlePause("encode")} />
        <button
          onClick={handleResume}
          disabled={isRunning}
          style={{
            ...BTN_BASE,
            background: isRunning ? PALETTE.surfaceLight : PALETTE.green,
            color: isRunning ? PALETTE.textMuted : "#000",
            cursor: isRunning ? "default" : "pointer",
          }}
        >
          RESUME
        </button>
      </div>

      {/* Media Search */}
      <SectionTitle>Search & Add to Lists</SectionTitle>
      <MediaSearch onAdd={handleSearchAdd} />

      {/* Skip list */}
      <SectionTitle>Skip List</SectionTitle>
      {skip && (
        <PathEditor
          title="Files to skip"
          paths={skip.paths || []}
          onSave={async (paths) => { await api.setSkip(paths); setSkip({ paths }); }}
        />
      )}

      {/* Priority list */}
      <SectionTitle>Priority List</SectionTitle>
      {priority && (
        <PathEditor
          title="Priority files (process first)"
          paths={priority.paths || []}
          onSave={async (paths) => { await api.setPriority(paths); setPriority({ paths }); }}
          reorderable
        />
      )}

      {/* CQ Heuristic Guide */}
      <SectionTitle>CQ Quality Guide</SectionTitle>
      <CQGuide />

      {/* Gentle overrides */}
      <SectionTitle>Gentle Overrides</SectionTitle>
      {gentle && (
        <GentleEditor
          gentle={gentle}
          onSave={async (data) => { await api.setGentle(data); setGentle(data); }}
        />
      )}

      {/* Re-encode list */}
      <SectionTitle>Re-encode List</SectionTitle>
      {reencode && (
        <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16 }}>
          {Object.keys(reencode.files || {}).length === 0 && Object.keys(reencode.patterns || {}).length === 0 ? (
            <div style={{ color: PALETTE.textMuted, fontSize: 13 }}>
              No files or patterns flagged for re-encoding. Use Search above to add files, or add a glob pattern below.
            </div>
          ) : null}

          {/* Patterns sub-section */}
          {Object.keys(reencode.patterns || {}).length > 0 && (
            <div style={{ marginBottom: Object.keys(reencode.files || {}).length > 0 ? 12 : 0 }}>
              <div style={{ color: PALETTE.textMuted, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Patterns</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {Object.entries(reencode.patterns).map(([pattern, opts]) => (
                  <div key={pattern} style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 12px", borderRadius: 8,
                    background: PALETTE.surfaceLight,
                  }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{
                        color: PALETTE.accent, fontSize: 12,
                        fontFamily: "'JetBrains Mono', monospace",
                      }}>
                        {pattern}
                      </div>
                      <div style={{ color: PALETTE.textMuted, fontSize: 10, marginTop: 2 }}>
                        CQ {opts.cq}
                      </div>
                    </div>
                    <button
                      onClick={async () => {
                        const updated = { ...reencode.patterns };
                        delete updated[pattern];
                        const files = reencode.files || {};
                        await api.setReencode(files, updated);
                        setReencode({ files, patterns: updated });
                        showFlash("Removed pattern from re-encode list");
                      }}
                      style={{
                        background: "transparent", border: `1px solid ${PALETTE.red}`,
                        color: PALETTE.red, borderRadius: 6, padding: "4px 10px",
                        fontSize: 11, cursor: "pointer", flexShrink: 0,
                      }}
                    >
                      Remove
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Exact files sub-section */}
          {Object.keys(reencode.files || {}).length > 0 && (
            <div>
              {Object.keys(reencode.patterns || {}).length > 0 && (
                <div style={{ color: PALETTE.textMuted, fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Files</div>
              )}
              <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                {Object.entries(reencode.files).map(([path, opts]) => (
                  <div key={path} style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 12px", borderRadius: 8,
                    background: PALETTE.surfaceLight,
                  }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{
                        color: PALETTE.text, fontSize: 12,
                        whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
                        fontFamily: "'JetBrains Mono', monospace",
                      }}>
                        {path.split("\\").pop() || path}
                      </div>
                      <div style={{ color: PALETTE.textMuted, fontSize: 10, marginTop: 2 }}>
                        CQ {opts.cq}
                      </div>
                    </div>
                    <button
                      onClick={async () => {
                        const updated = { ...reencode.files };
                        delete updated[path];
                        const patterns = reencode.patterns || {};
                        await api.setReencode(updated, patterns);
                        setReencode({ files: updated, patterns });
                        showFlash("Removed from re-encode list");
                      }}
                      style={{
                        background: "transparent", border: `1px solid ${PALETTE.red}`,
                        color: PALETTE.red, borderRadius: 6, padding: "4px 10px",
                        fontSize: 11, cursor: "pointer", flexShrink: 0,
                      }}
                    >
                      Remove
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Add pattern form */}
          <form
            onSubmit={async (e) => {
              e.preventDefault();
              const form = e.target;
              const pattern = form.pattern.value.trim();
              const cq = parseInt(form.cq.value, 10) || 30;
              if (!pattern) return;
              const patterns = { ...(reencode.patterns || {}), [pattern]: { cq } };
              const files = reencode.files || {};
              await api.setReencode(files, patterns);
              setReencode({ files, patterns });
              form.pattern.value = "";
              showFlash(`Added pattern "${pattern}" at CQ ${cq}`);
            }}
            style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 12 }}
          >
            <input
              name="pattern"
              placeholder="*Seinfeld*"
              style={{
                flex: 1, padding: "8px 12px", borderRadius: 8,
                background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`,
                color: PALETTE.text, fontSize: 13,
                fontFamily: "'JetBrains Mono', monospace",
              }}
            />
            <input
              name="cq"
              type="number"
              defaultValue={30}
              min={1}
              max={63}
              style={{
                width: 60, padding: "8px 10px", borderRadius: 8,
                background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`,
                color: PALETTE.text, fontSize: 13, textAlign: "center",
              }}
            />
            <button
              type="submit"
              style={{
                background: PALETTE.accent,
                color: "#000",
                border: "none",
                borderRadius: 8,
                padding: "8px 16px",
                fontSize: 12,
                fontWeight: 600,
                cursor: "pointer",
                flexShrink: 0,
                whiteSpace: "nowrap",
              }}
            >
              Add Pattern
            </button>
          </form>
        </div>
      )}

      {/* Settings */}
      <SettingsEditor />

      {/* System Health */}
      <HealthSection />
    </div>
  );
}


function HealthSection() {
  const { data: health } = usePolling(api.getHealth, 15000);
  if (!health) return null;

  const nasOk = health.nas_movies_reachable && health.nas_series_reachable;
  const stagingColour = health.staging_free_gb < 10 ? PALETTE.red : health.staging_free_gb < 50 ? PALETTE.accentWarm : PALETTE.green;

  return (
    <>
      <SectionTitle>System Health</SectionTitle>
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 24 }}>
        <StatCard
          label="NAS"
          value={nasOk ? "Connected" : "Unreachable"}
          colour={nasOk ? PALETTE.green : PALETTE.red}
          sub={!health.nas_movies_reachable ? "Movies ✗" : !health.nas_series_reachable ? "Series ✗" : "Movies ✓ Series ✓"}
        />
        <StatCard
          label="Staging Free"
          value={`${health.staging_free_gb} GB`}
          colour={stagingColour}
          sub={`of ${health.staging_total_gb} GB total`}
        />
        <StatCard
          label="FFmpeg"
          value={health.ffmpeg_version}
        />
        <StatCard
          label="GPU"
          value={health.gpu_available ? health.gpu_name : "N/A"}
          colour={health.gpu_available ? undefined : PALETTE.red}
          sub={health.gpu_temp_c != null ? `${health.gpu_temp_c}°C` : ""}
        />
        <StatCard
          label="Pipeline"
          value={health.pipeline_status}
          colour={health.pipeline_status === "running" ? PALETTE.green : PALETTE.textMuted}
          sub={health.pipeline_pid ? `PID ${health.pipeline_pid}` : ""}
        />
        <StatCard
          label="Python"
          value={health.python_version}
        />
      </div>
    </>
  );
}
