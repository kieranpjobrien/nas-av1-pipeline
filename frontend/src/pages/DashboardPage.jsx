import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "../lib/api";
import { useWebSocket } from "../lib/useWebSocket";
import { Glance } from "./dashboard/Glance";
import { Library } from "./dashboard/Library";
import { Worklist } from "./dashboard/Worklist";
import { Storage } from "./dashboard/Storage";
import { Settings } from "./dashboard/Settings";
import { Logs } from "./dashboard/Logs";
import { Queue } from "./dashboard/Queue";
import { Workers } from "./dashboard/Workers";
import { Errors } from "./dashboard/Errors";
import { History } from "./dashboard/History";
import { Toasts, useToasts } from "./dashboard/Toasts";
import {
  DEFAULT_ROUTING,
  HOSTS,
  JOB_TYPES,
  aggregateBy,
  codecCount,
  fmtNum,
  normalizeFile,
} from "./dashboard/helpers";
import "./dashboard/dashboard.css";

const VIEW_LABELS = {
  glance: "Glance",
  library: "Library",
  worklist: "Worklist",
  queue: "Encode queue",
  workers: "Workers",
  errors: "Errors",
  history: "History",
  storage: "Storage",
  settings: "Settings",
  logs: "Logs",
};

function HealthIndicator() {
  const [h, setH] = useState(null);
  const [ctl, setCtl] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      const [health, control] = await Promise.all([
        api.getHealth().catch(() => null),
        api.getControlStatus().catch(() => null),
      ]);
      if (!cancelled) {
        setH(health);
        setCtl(control);
      }
    };
    tick();
    const id = setInterval(tick, 10000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);
  if (!h) return null;

  const procStatus = h.pipeline_status || "idle";
  const pauseState = ctl?.pause_state || "running";
  const paused = pauseState !== "running";

  // Effective state: paused trumps running.
  const effective = paused
    ? pauseState.replace("paused_", "paused · ")
    : procStatus;
  const colour = paused
    ? "var(--warn)"
    : procStatus === "running"
      ? "var(--good)"
      : procStatus === "error"
        ? "var(--bad)"
        : procStatus === "finished"
          ? "var(--ink-3)"
          : "var(--warn)";

  const nasOk = h.nas_movies_reachable && h.nas_series_reachable;
  const resume = async () => {
    try {
      await api.resume();
      window.notify?.({ kind: "good", title: "Pipeline resumed", body: "pause flags cleared" });
      const c = await api.getControlStatus().catch(() => null);
      setCtl(c);
    } catch (e) {
      window.notify?.({ kind: "bad", title: "Resume failed", body: String(e.message || e) });
    }
  };

  return (
    <>
      <div style={{ display: "flex", alignItems: "center", gap: 6, color: "var(--ink-3)" }}>
        <span
          style={{
            width: 5,
            height: 5,
            borderRadius: "50%",
            background: colour,
            boxShadow: `0 0 6px ${colour}`,
          }}
        />
        daemon · {effective}
      </div>
      {paused && (
        <button
          onClick={resume}
          style={{
            fontSize: 9,
            color: "var(--warn)",
            background: "rgba(240,180,41,0.08)",
            border: "1px solid rgba(240,180,41,0.35)",
            borderRadius: 4,
            padding: "2px 6px",
            marginTop: 2,
            cursor: "pointer",
            fontFamily: "inherit",
            textTransform: "uppercase",
            letterSpacing: "0.08em",
          }}
          title="Clear pause flags and let the pipeline keep going"
        >
          Resume →
        </button>
      )}
      <div style={{ color: nasOk ? "var(--ink-3)" : "var(--bad)" }}>
        NAS · {nasOk ? "reachable" : "unreachable"}
      </div>
      {h.staging_free_gb > 0 && (
        <div>staging · {h.staging_free_gb.toFixed(0)} GB free</div>
      )}
    </>
  );
}

function Sidebar({ view, setView, data, errorCount, workersActive, workersTotal, onClassic }) {
  const total = data?.summary?.total_files;
  const needsEncode = data ? codecCount(data.codecs, "hevc") + codecCount(data.codecs, "h264") : null;
  const workersCount =
    workersTotal != null ? `${workersActive || 0}/${workersTotal}` : workersActive || null;

  const sections = [
    {
      group: "Workspace",
      items: [
        { k: "glance", l: "Glance" },
        { k: "library", l: "Library", c: total != null ? fmtNum(total) : null },
        { k: "worklist", l: "Worklist" },
      ],
    },
    {
      group: "Pipeline",
      items: [
        { k: "queue", l: "Encode queue", c: needsEncode != null ? fmtNum(needsEncode) : null },
        { k: "workers", l: "Workers", c: workersCount },
        { k: "errors", l: "Errors", c: errorCount ? fmtNum(errorCount) : null, bad: errorCount > 0 },
        { k: "history", l: "History" },
      ],
    },
    {
      group: "System",
      items: [
        { k: "storage", l: "Storage" },
        { k: "settings", l: "Settings" },
        { k: "logs", l: "Logs" },
        { k: "_classic", l: "Classic view", external: true, onClick: onClassic },
      ],
    },
  ];

  return (
    <aside className="sidebar">
      <div className="brand">
        <div className="brand-mark" />
        <div>
          <div className="brand-name">nascleanup</div>
          <div className="brand-sub">operator console</div>
        </div>
      </div>
      {sections.map((sec) => (
        <div key={sec.group}>
          <div className="nav-section">{sec.group}</div>
          {sec.items.map((it) => (
            <div
              key={it.k}
              className={`nav-item ${view === it.k ? "active" : ""}`}
              onClick={() => {
                if (it.onClick) {
                  it.onClick();
                  return;
                }
                if (!it.external) setView(it.k);
              }}
            >
              <span className="dot" style={it.bad ? { background: "var(--bad)" } : {}} />
              {it.l}
              {it.c && <span className="nav-count">{it.c}</span>}
            </div>
          ))}
        </div>
      ))}
      <div
        style={{
          marginTop: "auto",
          padding: "10px 8px",
          borderTop: "1px solid var(--line)",
          fontSize: 10,
          color: "var(--ink-4)",
          fontFamily: "JetBrains Mono,monospace",
          lineHeight: 1.7,
        }}
      >
        <HealthIndicator />
      </div>
    </aside>
  );
}

const HIST_LEN = 12;

function Telemetry() {
  const [cpuHist, setCpuHist] = useState(() => Array(HIST_LEN).fill(0));
  const [gpuHist, setGpuHist] = useState(() => Array(HIST_LEN).fill(0));
  const [netHist, setNetHist] = useState(() => Array(HIST_LEN).fill(0));
  const [host, setHost] = useState(null);
  const [gpu, setGpu] = useState(null);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const [h, g] = await Promise.all([
          api.getHostStats().catch(() => null),
          api.getGpu().catch(() => null),
        ]);
        if (cancelled) return;
        if (h?.available) {
          setHost(h);
          setCpuHist((p) => [...p.slice(1), h.cpu_pct || 0]);
          if (h.net_mbps != null) setNetHist((p) => [...p.slice(1), h.net_mbps]);
        }
        if (g?.available) {
          setGpu(g);
          setGpuHist((p) => [...p.slice(1), g.gpu_util || 0]);
        }
      } catch {}
    };
    tick();
    const id = setInterval(tick, 1500);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const spark = (hist, scale = 100) => (
    <span className="tel-spark">
      {hist.map((v, i) => (
        <i key={i} style={{ height: Math.max(1, (v / scale) * 12) + "px" }} />
      ))}
    </span>
  );

  if (!host && !gpu) {
    return (
      <div className="telemetry" title="Waiting for host-stats / nvidia-smi">
        <div className="tel-item">
          <span className="lbl">host</span>
          <span className="val">—</span>
        </div>
      </div>
    );
  }

  const cpuPct = host?.cpu_pct ?? null;
  const cpuTemp = host?.cpu_temp_c ?? null;
  const gpuPct = gpu?.gpu_util ?? null;
  const gpuTemp = gpu?.temp_c ?? null;
  const netMbps = host?.net_mbps ?? null;
  const stagingUsed = host?.staging_used_gb ?? null;
  const stagingTotal = host?.staging_total_gb ?? null;
  const netPeak = Math.max(100, ...netHist);

  const cpuCls =
    cpuPct == null ? "" : cpuPct > 85 || (cpuTemp && cpuTemp > 85) ? "bad" : cpuPct > 65 ? "warn" : "good";
  const gpuCls =
    gpuPct == null ? "" : gpuPct > 90 || (gpuTemp && gpuTemp > 82) ? "bad" : gpuPct > 70 ? "warn" : "good";
  const cachePct = stagingTotal ? (stagingUsed / stagingTotal) * 100 : null;
  const cacheCls = cachePct == null ? "" : cachePct > 80 ? "bad" : cachePct > 60 ? "warn" : "good";

  return (
    <div className="telemetry" title="psutil + nvidia-smi · polled every 1.5s">
      <div className={`tel-item ${cpuCls}`}>
        <span className="lbl">cpu</span>
        <span className="val">{cpuPct != null ? `${cpuPct.toFixed(0)}%` : "—"}</span>
        {cpuTemp != null && <span className="temp">{cpuTemp}°</span>}
        {spark(cpuHist)}
      </div>
      <div className={`tel-item ${gpuCls}`}>
        <span className="lbl">gpu</span>
        <span className="val">{gpuPct != null ? `${gpuPct}%` : "—"}</span>
        {gpuTemp != null && <span className="temp">{gpuTemp}°</span>}
        {spark(gpuHist)}
      </div>
      <div className="tel-item good">
        <span className="lbl">net</span>
        <span className="val">{netMbps != null ? netMbps.toFixed(0) : "—"}</span>
        <span className="temp">MB/s</span>
        {spark(netHist, netPeak)}
      </div>
      <div className={`tel-item ${cacheCls}`}>
        <span className="lbl">cache</span>
        <span className="val">{stagingUsed != null ? (stagingUsed / 1024).toFixed(2) : "—"}</span>
        {stagingTotal != null && (
          <span className="temp">/ {(stagingTotal / 1024).toFixed(1)} TB</span>
        )}
      </div>
    </div>
  );
}

function RunMenu({ routing, setRouting, onRun, errorCount = 0, pipelineRunning = false }) {
  const setJobHost = (jk, host) => {
    if (!HOSTS[host].can.includes(jk)) return;
    setRouting((r) => ({ ...r, [jk]: host }));
    const job = JOB_TYPES.find((j) => j.k === jk);
    window.notify?.({ kind: "info", title: `${job.l} → ${HOSTS[host].label}`, body: HOSTS[host].host });
  };
  const opts = [
    {
      k: "quick",
      title: "Quick wins",
      sub: pipelineRunning
        ? "Push audio/sub-cleanup AV1 files to the front of the running queue"
        : "Push audio/sub-cleanup AV1 files up + start the pipeline",
      count: "~20 files",
      icon: (
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M13 2 4 14h7l-1 8 9-12h-7z" />
        </svg>
      ),
    },
    {
      k: "errors",
      title: "Retry errored jobs",
      sub:
        errorCount === 0
          ? "No failed jobs — nothing to retry"
          : pipelineRunning
            ? "Reset failed files to pending; the running pipeline will pick them up"
            : "Reset failed files to pending + start the pipeline",
      count: `${fmtNum(errorCount)} ${errorCount === 1 ? "file" : "files"}`,
      disabled: errorCount === 0,
      iconColor: "var(--bad)",
      icon: (
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M3 12a9 9 0 1 0 3-6.7" />
          <path d="M3 4v5h5" />
        </svg>
      ),
    },
    pipelineRunning
      ? {
          k: "stop",
          title: "Stop pipeline",
          sub: "Graceful · finishes any in-flight upload, then terminates workers",
          count: "",
          iconColor: "var(--warn)",
          icon: (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <rect x="6" y="6" width="12" height="12" rx="1" />
            </svg>
          ),
        }
      : null,
    pipelineRunning
      ? {
          k: "kill",
          title: "Force-kill pipeline",
          sub: "Instant · terminates every worker immediately, in-flight work is lost",
          count: "",
          iconColor: "var(--bad)",
          icon: (
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <circle cx="12" cy="12" r="9" />
              <line x1="8" y1="8" x2="16" y2="16" />
              <line x1="16" y1="8" x2="8" y2="16" />
            </svg>
          ),
        }
      : null,
  ].filter(Boolean);
  return (
    <div className="run-menu">
      <div className="run-menu-head">Start a batch</div>
      {opts.map((o) => (
        <div
          key={o.k}
          className="run-opt"
          onClick={() => (o.disabled ? null : onRun(o.k))}
          style={o.disabled ? { opacity: 0.45, cursor: "not-allowed" } : undefined}
        >
          <div className="run-opt-icon" style={o.iconColor ? { color: o.iconColor } : undefined}>
            {o.icon}
          </div>
          <div>
            <div className="run-opt-title">{o.title}</div>
            <div className="run-opt-sub">{o.sub}</div>
          </div>
          <span className="run-opt-count">{o.count}</span>
        </div>
      ))}
      <div className="run-menu-sep" />
      <div
        className="run-menu-head"
        style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}
      >
        <span>Job routing</span>
        <span
          className="mono"
          style={{
            color: "var(--ink-4)",
            textTransform: "none",
            letterSpacing: 0,
            fontSize: 9,
          }}
        >
          which host runs each type
        </span>
      </div>
      {JOB_TYPES.map((j) => {
        const current = routing[j.k];
        return (
          <div key={j.k} className="run-menu-routing">
            <span
              className="mono"
              style={{ color: "var(--ink-3)", fontSize: 12, textAlign: "center" }}
            >
              {j.icon}
            </span>
            <div>
              <div style={{ fontSize: 12, fontWeight: 500 }}>{j.l}</div>
              <div style={{ fontSize: 10, color: "var(--ink-3)" }}>{j.sub}</div>
            </div>
            <div style={{ display: "flex", gap: 4 }}>
              {Object.entries(HOSTS).map(([hk, h]) => {
                const capable = h.can.includes(j.k);
                const on = current === hk;
                return (
                  <button
                    key={hk}
                    className={`pill ${on ? "on" : ""}`}
                    disabled={!capable}
                    title={
                      capable
                        ? h.host
                        : `${h.label} cannot run ${j.l.toLowerCase()} — missing capability`
                    }
                    onClick={(e) => {
                      e.stopPropagation();
                      if (capable) setJobHost(j.k, hk);
                    }}
                  >
                    {hk === "nas" ? "NAS" : "local"}
                  </button>
                );
              })}
            </div>
          </div>
        );
      })}
      <div className="run-menu-footnote">
        NAS has no GPU — re-encodes always run on {HOSTS.local.label}.
      </div>
    </div>
  );
}

function TopBar({
  view,
  setView,
  rescan,
  onRescan,
  onRunBatch,
  routing,
  setRouting,
  errorCount,
  remainingCount,
}) {
  const PRIMARY_VIEWS = ["glance", "library", "worklist"];
  const [runMenu, setRunMenu] = useState(false);
  const [pipelineStatus, setPipelineStatus] = useState(null); // "idle" | "running" | "error" | "finished"
  const [busy, setBusy] = useState(false); // true for 1-2s after clicking Run, before status poll catches up
  const runMenuRef = useRef(null);

  // Poll pipeline status so the Run button reflects reality. 2.5s is fast enough that a click
  // feels responsive but doesn't spam the endpoint.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const s = await api.getProcessStatus("pipeline");
        if (!cancelled) setPipelineStatus(s?.status || "idle");
      } catch {
        if (!cancelled) setPipelineStatus(null);
      }
    };
    tick();
    const id = setInterval(tick, 2500);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const pipelineRunning = pipelineStatus === "running";

  useEffect(() => {
    if (!runMenu) return;
    const onDoc = (e) => {
      if (runMenuRef.current && !runMenuRef.current.contains(e.target)) setRunMenu(false);
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [runMenu]);

  const fireRun = async (mode) => {
    setRunMenu(false);
    setBusy(true);
    try {
      await onRunBatch(mode);
    } finally {
      // Give the status poll a moment to catch up before unlocking the button.
      setTimeout(() => setBusy(false), 1000);
    }
  };

  return (
    <div className="topbar">
      <div className="crumbs">
        <b>{VIEW_LABELS[view] || view}</b>
        <span style={{ margin: "0 8px", color: "var(--ink-4)" }}>/</span>
        <span>all libraries</span>
      </div>
      {rescan ? (
        <div className="status warn" title={rescan.phase}>
          <span className="led" />
          <span className="mono">
            rescan · {rescan.phase.toLowerCase()} · {rescan.pct.toFixed(0)}%
          </span>
        </div>
      ) : (
        <Telemetry />
      )}
      <div className="view-switch">
        {PRIMARY_VIEWS.map((v) => (
          <button
            key={v}
            className={view === v ? "on" : ""}
            onClick={() => setView(v)}
          >
            {VIEW_LABELS[v]}
          </button>
        ))}
      </div>
      <button className="top-btn" onClick={onRescan} disabled={!!rescan}>
        <svg
          width="12"
          height="12"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          style={rescan ? { animation: "nc-spin 1.2s linear infinite" } : {}}
        >
          <path d="M3 12a9 9 0 1 0 18 0 9 9 0 0 0-18 0" />
          <path d="M12 7v5l3 2" />
        </svg>
        {rescan ? `Rescanning · ${rescan.pct.toFixed(0)}%` : "Rescan"}
      </button>
      <div className="split" ref={runMenuRef}>
        <button
          className="top-btn primary"
          onClick={() => fireRun("start")}
          disabled={pipelineRunning || busy}
          title={
            pipelineRunning
              ? "Pipeline is already running — use the dropdown for queue actions"
              : "Start the pipeline daemon — it will process the priority + main queue"
          }
        >
          {pipelineRunning ? (
            <>
              <svg
                width="12"
                height="12"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                style={{ animation: "nc-spin 2.4s linear infinite" }}
              >
                <circle cx="12" cy="12" r="9" />
                <path d="M12 3v3" />
              </svg>
              Running
            </>
          ) : (
            <>
              <svg
                width="12"
                height="12"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2.5"
              >
                <path d="M6 4v16l14-8z" />
              </svg>
              {busy ? "Starting…" : "Run next batch"}
            </>
          )}
        </button>
        <button
          className="split-caret"
          title="More run options"
          onClick={() => setRunMenu((v) => !v)}
        >
          <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
            <path d="M6 9l6 6 6-6" />
          </svg>
        </button>
        {runMenu && (
          <RunMenu
            routing={routing}
            setRouting={setRouting}
            onRun={fireRun}
            errorCount={errorCount}
            pipelineRunning={pipelineRunning}
          />
        )}
      </div>
    </div>
  );
}

function ErrorState({ error }) {
  return (
    <div className="nc-app">
      <div style={{ padding: 60, fontSize: 13, color: "var(--ink-3)" }}>
        Failed to load media report: {error}
      </div>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="nc-app">
      <div style={{ padding: 60, fontSize: 13, color: "var(--ink-3)" }}>Loading media report…</div>
    </div>
  );
}

export function DashboardPage({ onClassic, onFileClick }) {
  const [view, setView] = useState(() => localStorage.getItem("nc.view") || "glance");
  const [report, setReport] = useState(null);
  const [error, setError] = useState(null);
  const [rescan, setRescan] = useState(null);
  const [routing, setRouting] = useState(() => {
    try {
      return { ...DEFAULT_ROUTING, ...JSON.parse(localStorage.getItem("nc.routing") || "{}") };
    } catch {
      return DEFAULT_ROUTING;
    }
  });
  const { pipeline } = useWebSocket(api.getPipeline, 3000);
  const { toasts, push, update, dismiss } = useToasts();

  useEffect(() => {
    localStorage.setItem("nc.view", view);
  }, [view]);

  useEffect(() => {
    localStorage.setItem("nc.routing", JSON.stringify(routing));
  }, [routing]);

  useEffect(() => {
    window.notify = push;
    return () => {
      delete window.notify;
    };
  }, [push]);

  useEffect(() => {
    api
      .getMediaReport()
      .then(setReport)
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    const id = "nc-fonts";
    if (document.getElementById(id)) return;
    const link = document.createElement("link");
    link.id = id;
    link.rel = "stylesheet";
    link.href =
      "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Inter+Tight:wght@400;500;600;700&family=Instrument+Serif:ital@0;1&display=swap";
    document.head.appendChild(link);
  }, []);

  const data = useMemo(() => {
    if (!report) return null;
    const files = (report.files || []).map(normalizeFile);
    const sorted = [...files].sort((a, b) => b.size_gb - a.size_gb);
    const codecs = aggregateBy(files, (f) => f.codec || "Unknown");
    const resolutions = aggregateBy(files, (f) => f.res || "Unknown");
    const remainingSizeGb = files
      .filter((f) => f.codec !== "AV1")
      .reduce((s, f) => s + (f.size_gb || 0), 0);
    // Subtitle checks match Bazarr's logic: consider BOTH internal tracks and external
    // sidecar files (.srt/.ass/etc. written by Bazarr alongside the media). The scanner
    // records these on `external_subtitles` / `external_subtitle_count` per file.
    //
    // "Needs English subs" means: at least one audio track is EXPLICITLY non-English, AND
    // there's no English sub (internal or external). Files where every audio track is
    // `und` are treated as English (most libraries default to English-native, and we'd
    // rather under-report than flag 300 files that actually don't need anything).
    const hasEngSubAnywhere = (f) => {
      const internal = (f.subs || []).some((s) =>
        (s.lang || "").toLowerCase().startsWith("en")
      );
      if (internal) return true;
      return (f.externalSubs || []).some((s) =>
        (s.language || "").toLowerCase().startsWith("en")
      );
    };
    const langIsNonEng = (l) => {
      const low = (l || "").toLowerCase();
      if (!low || low === "und" || low === "unknown") return false;
      return !low.startsWith("en");
    };
    const noSubs = files.filter((f) => {
      const internal = f.subs?.length || 0;
      const external = f.externalSubs?.length || 0;
      return internal + external === 0;
    });
    const definitelyNonEnglishAudio = files.filter((f) => {
      const audio = f.audio || [];
      if (audio.length === 0) return false;
      const hasEng = audio.some((a) => (a.lang || "").toLowerCase().startsWith("en"));
      if (hasEng) return false;
      return audio.some((a) => langIsNonEng(a.lang));
    });
    const noEng = definitelyNonEnglishAudio.filter((f) => !hasEngSubAnywhere(f));
    return {
      summary: report.summary || {},
      codecs,
      resolutions,
      topTargets: sorted.slice(0, 200),
      noSubs: noSubs.slice(0, 200),
      noEng: noEng.slice(0, 200),
      remainingSizeGb,
      errorCount: report.summary?.errors || 0,
      recentEvents: null,
    };
  }, [report]);

  const liveErrorCount = useMemo(() => {
    const fromPipeline = Object.values(pipeline?.files || {}).filter((f) =>
      ["error", "errored", "failed"].includes((f.status || "").toLowerCase())
    ).length;
    return fromPipeline || data?.errorCount || 0;
  }, [pipeline, data]);

  const remainingEncodes = useMemo(() => {
    if (!data) return null;
    return codecCount(data.codecs, "hevc") + codecCount(data.codecs, "h264");
  }, [data]);

  const workers = useMemo(() => {
    const active = Object.values(pipeline?.files || {}).filter((f) =>
      ["encoding", "analyzing", "fetching", "uploading", "processing"].includes(
        (f.status || "").toLowerCase()
      )
    ).length;
    const total = pipeline?.workers?.total ?? pipeline?.stats?.worker_count ?? null;
    return { active, total };
  }, [pipeline]);

  const startRescan = useCallback(async () => {
    if (rescan) return;
    setRescan({ pct: 0, phase: "Starting scanner…", found: 0 });
    const toastId = push({
      kind: "info",
      title: "Rescan started",
      body: "Spawning tools.scanner subprocess…",
      ttl: 0,
      progress: 0,
    });
    try {
      const r = await api.startProcess("scanner");
      if (r.ok === false) {
        throw new Error(r.error || "scanner start failed");
      }
    } catch (e) {
      update(toastId, {
        kind: "bad",
        title: "Rescan failed to start",
        body: String(e.message || e),
        ttl: 4000,
      });
      setTimeout(() => dismiss(toastId), 4000);
      setRescan(null);
      return;
    }

    // Poll scanner status until it finishes; pull progress hints from the last log line.
    const poll = async () => {
      let st;
      try {
        st = await api.getProcessStatus("scanner");
      } catch {
        st = null;
      }
      let lastLog = "";
      try {
        const logs = await api.getProcessLogs("scanner", 8);
        const lines = logs.lines || logs || [];
        lastLog = Array.isArray(lines) && lines.length ? lines[lines.length - 1] : "";
      } catch {}

      const found = (() => {
        const m = lastLog.match(/(\d[\d,]*)\s*(files?|items?)/i);
        return m ? parseInt(m[1].replace(/,/g, ""), 10) : null;
      })();

      if (st?.status === "running") {
        setRescan((prev) => ({
          pct: Math.min(95, (prev?.pct || 0) + 2),
          phase: lastLog ? lastLog.slice(0, 80) : "Scanning…",
          found: found ?? prev?.found ?? 0,
        }));
        update(toastId, {
          body: lastLog ? lastLog.slice(0, 120) : "Scanning…",
          progress: null,
        });
        setTimeout(poll, 1500);
      } else {
        setRescan(null);
        const ok = st?.exit_code === 0 || st?.status === "finished";
        update(toastId, {
          kind: ok ? "good" : "bad",
          title: ok ? "Rescan complete" : "Rescan finished with errors",
          body: lastLog || (ok ? "Library index refreshed" : `exit ${st?.exit_code ?? "?"}`),
          progress: 100,
          ttl: 4500,
        });
        setTimeout(() => dismiss(toastId), 4500);
      }
    };
    setTimeout(poll, 1200);
  }, [rescan, push, update, dismiss]);

  const runBatch = useCallback(
    async (mode) => {
      const host = HOSTS[routing.encode]?.label || HOSTS.local.label;
      const errs = liveErrorCount;
      const startIfIdle = async () => {
        // Best-effort start. If it's already running, backend returns ok:false — swallow.
        try {
          const r = await api.startProcess("pipeline");
          if (r?.ok !== false) {
            return { started: true, pid: r?.pid };
          }
        } catch {}
        return { started: false };
      };

      try {
        if (mode === "start") {
          const r = await api.startProcess("pipeline");
          if (r?.ok === false) {
            push({ kind: "warn", title: "Pipeline already running", body: r.error || "" });
          } else {
            push({
              kind: "good",
              title: "Pipeline started",
              body: `pid ${r?.pid ?? "?"} · will process the priority + main queue on ${host}`,
            });
          }
          return;
        }

        if (mode === "stop") {
          const r = await api.stopProcess("pipeline");
          push({
            kind: r?.ok === false ? "warn" : "info",
            title: r?.ok === false ? "Pipeline not running" : "Pipeline stop requested",
            body: r?.method ? `method: ${r.method}` : r?.error || "",
          });
          return;
        }

        if (mode === "kill") {
          const r = await api.killProcess("pipeline");
          const killedCount = Array.isArray(r?.killed) ? r.killed.length : 0;
          push({
            kind: r?.ok === false ? "warn" : "bad",
            title: r?.ok === false ? "Nothing to kill" : "Pipeline force-killed",
            body: killedCount
              ? `${killedCount} process${killedCount === 1 ? "" : "es"} terminated (pid${killedCount === 1 ? "" : "s"} ${r.killed.join(", ")})`
              : r?.error || "",
          });
          return;
        }

        if (mode === "errors") {
          if (errs === 0) {
            push({ kind: "info", title: "Nothing to retry", body: "No errored jobs right now." });
            return;
          }
          const r = await api.resetErrors();
          const resetCount = r?.reset ?? errs;
          const started = await startIfIdle();
          push({
            kind: "good",
            title: `Requeued ${resetCount} ${resetCount === 1 ? "file" : "files"}`,
            body: started.started
              ? `Errors → pending · pipeline started (pid ${started.pid}) on ${host}`
              : `Errors → pending · running pipeline will pick them up`,
          });
          return;
        }

        if (mode === "quick") {
          const r = await api.quickWins();
          const added = r?.added ?? 0;
          if (added === 0) {
            push({
              kind: "info",
              title: "No quick wins found",
              body: "Nothing obvious to push up — the library's already clean on audio/subs.",
            });
            return;
          }
          const started = await startIfIdle();
          push({
            kind: "good",
            title: `Pushed ${added} quick wins to priority`,
            body: started.started
              ? `Force list updated · pipeline started (pid ${started.pid})`
              : `Force list updated · running pipeline will pick them up first`,
          });
          return;
        }
      } catch (e) {
        push({ kind: "bad", title: "Action failed", body: String(e.message || e) });
      }
    },
    [push, routing, liveErrorCount]
  );

  if (error) return <ErrorState error={error} />;
  if (!data) return <LoadingState />;

  return (
    <div className="nc-app" data-density="regular">
      <div className="app">
        <Sidebar
          view={view}
          setView={setView}
          data={data}
          errorCount={liveErrorCount}
          workersActive={workers.active}
          workersTotal={workers.total}
          onClassic={onClassic}
        />
        <main className="main">
          <TopBar
            view={view}
            setView={setView}
            rescan={rescan}
            onRescan={startRescan}
            onRunBatch={runBatch}
            routing={routing}
            setRouting={setRouting}
            errorCount={liveErrorCount}
            remainingCount={remainingEncodes}
          />
          {rescan && (
            <div className="rescan-strip">
              <div className="rescan-inner">
                <span className="mono" style={{ color: "var(--accent)" }}>● rescan</span>
                <span className="mono" style={{ color: "var(--ink-2)" }}>{rescan.phase}</span>
                <span className="mono" style={{ color: "var(--ink-3)" }}>
                  {rescan.found.toLocaleString()} files walked
                </span>
                <div className="rescan-bar">
                  <div className="fill" style={{ width: rescan.pct + "%" }} />
                </div>
                <span
                  className="mono"
                  style={{ color: "var(--ink-2)", minWidth: 42, textAlign: "right" }}
                >
                  {rescan.pct.toFixed(0)}%
                </span>
              </div>
            </div>
          )}
          <div className="content">
            {view === "glance" && (
              <Glance
                data={data}
                pipelineData={pipeline}
                workersActive={workers.active}
                workersTotal={workers.total}
                onNavigate={setView}
              />
            )}
            {view === "library" && (
              <Library data={data} pipelineData={pipeline} onFileOpen={onFileClick} />
            )}
            {view === "worklist" && <Worklist data={data} pipelineData={pipeline} />}
            {view === "storage" && <Storage data={data} />}
            {view === "settings" && <Settings />}
            {view === "logs" && <Logs />}
            {view === "queue" && (
              <Queue data={data} pipelineData={pipeline} onFileOpen={onFileClick} />
            )}
            {view === "workers" && <Workers pipelineData={pipeline} />}
            {view === "errors" && <Errors pipelineData={pipeline} />}
            {view === "history" && <History />}
          </div>
        </main>
      </div>
      <Toasts toasts={toasts} dismiss={dismiss} />
    </div>
  );
}
