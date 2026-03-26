import { useState, useEffect, useCallback } from "react";
import { PALETTE } from "../theme";
import { api } from "../lib/api";

const STATUS_COLOURS = {
  idle: PALETTE.textMuted,
  running: PALETTE.accent,
  finished: PALETTE.green,
  error: PALETTE.red,
};

// All processes except "pipeline" are CPU-only (ffprobe/ffmpeg decode, no NVENC)
const GPU_PROCESSES = new Set(["pipeline"]);

// Parse progress from log lines: "Progress: 50/500 (28%) ETA: ~2.1h"
const PROGRESS_RE = /(?:Progress|Scanned|Checked)[:\s]+(\d+)\s*[\/of]+\s*(\d+)/i;
const ETA_RE = /ETA:\s*(~?[\d.]+[hms])/i;

function parseProgress(lines) {
  // Walk backwards to find the most recent progress line
  for (let i = lines.length - 1; i >= 0; i--) {
    const m = lines[i].match(PROGRESS_RE);
    if (m) {
      const result = { current: parseInt(m[1], 10), total: parseInt(m[2], 10) };
      // Also try to extract ETA from the same line
      const etaMatch = lines[i].match(ETA_RE);
      if (etaMatch) result.eta = etaMatch[1];
      return result;
    }
  }
  return null;
}

function lastMeaningfulLine(lines) {
  // Find the last non-empty line
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i].trim();
    if (!line) continue;
    return line;
  }
  return null;
}

export function ProcessRow({ name, label, startLabel, onFlash }) {
  const [status, setStatus] = useState("idle");
  const [pid, setPid] = useState(null);
  const [busy, setBusy] = useState(false);
  const [logsOpen, setLogsOpen] = useState(false);
  const [logs, setLogs] = useState([]);
  const [progress, setProgress] = useState(null);
  const [lastLine, setLastLine] = useState(null);

  const poll = useCallback(async () => {
    try {
      const s = await api.getProcessStatus(name);
      setStatus(s.status);
      setPid(s.pid);
    } catch { /* ignore */ }
  }, [name]);

  useEffect(() => {
    poll();
    const id = setInterval(poll, 2000);
    return () => clearInterval(id);
  }, [poll]);

  // Poll logs while running (for progress) or when logs panel is open
  useEffect(() => {
    const shouldPoll = status === "running" || logsOpen;
    if (!shouldPoll) {
      if (status !== "running") {
        setProgress(null);
      }
      return;
    }
    const fetchLogs = async () => {
      try {
        const res = await api.getProcessLogs(name, 50);
        const lines = res.lines || [];
        setLogs(lines);
        if (status === "running") {
          setProgress(parseProgress(lines));
          setLastLine(lastMeaningfulLine(lines));
        }
      } catch { /* ignore */ }
    };
    fetchLogs();
    const id = setInterval(fetchLogs, 2000);
    return () => clearInterval(id);
  }, [status, logsOpen, name]);

  // Clear progress when process finishes
  useEffect(() => {
    if (status === "finished" || status === "idle") {
      setProgress(null);
    }
  }, [status]);

  const isRunning = status === "running";
  const isCpuOnly = !GPU_PROCESSES.has(name);
  const pct = progress && progress.total > 0
    ? Math.round((progress.current / progress.total) * 100)
    : null;

  const handleStart = async () => {
    setBusy(true);
    try {
      await api.startProcess(name);
      onFlash(`${label} started`);
    } catch (e) {
      onFlash(`Failed to start ${label}: ${e.message}`);
    }
    setBusy(false);
    poll();
  };

  const handleStop = async () => {
    setBusy(true);
    try {
      await api.stopProcess(name);
      onFlash(`${label} stopped`);
    } catch {
      try {
        const res = await api.killProcess(name);
        onFlash(`${label} killed (pids: ${res.killed?.join(", ")})`);
      } catch (e2) {
        onFlash(`Failed to stop ${label}: ${e2.message}`);
      }
    }
    setBusy(false);
    poll();
  };

  const handleKill = async () => {
    setBusy(true);
    try {
      const res = await api.killProcess(name);
      onFlash(`${label} killed (pids: ${res.killed?.join(", ")})`);
    } catch (e) {
      onFlash(`No ${label} process found`);
    }
    setBusy(false);
    poll();
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16, marginBottom: 8 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        {/* Status dot with optional progress ring */}
        <div style={{ position: "relative", width: 10, height: 10, flexShrink: 0 }}>
          <div style={{ width: 10, height: 10, borderRadius: "50%", background: STATUS_COLOURS[status] || PALETTE.textMuted }} />
        </div>

        <div style={{ flex: "1 1 120px", minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ color: PALETTE.text, fontSize: 14, fontWeight: 600 }}>{label}</span>
            {isRunning && isCpuOnly && (
              <span style={{
                fontSize: 10, color: PALETTE.green, background: PALETTE.green + "18",
                padding: "1px 6px", borderRadius: 4, whiteSpace: "nowrap",
              }}>
                CPU only — safe with pipeline
              </span>
            )}
          </div>

          {/* Status + progress */}
          <div style={{ color: PALETTE.textMuted, fontSize: 11, display: "flex", alignItems: "center", gap: 8 }}>
            <span>{status}{pid ? ` (pid ${pid})` : ""}</span>
            {isRunning && pct !== null && (
              <span style={{ color: PALETTE.accent, fontFamily: "'JetBrains Mono', monospace", fontWeight: 500 }}>
                {progress.current.toLocaleString()} / {progress.total.toLocaleString()} ({pct}%)
                {progress.eta && <span style={{ color: PALETTE.textMuted, marginLeft: 6 }}>ETA: {progress.eta}</span>}
              </span>
            )}
          </div>

          {/* Progress bar */}
          {isRunning && pct !== null && (
            <div style={{
              marginTop: 6, height: 4, background: PALETTE.bg,
              borderRadius: 2, overflow: "hidden", maxWidth: 300,
            }}>
              <div style={{
                width: `${pct}%`, height: "100%",
                background: PALETTE.accent, borderRadius: 2,
                transition: "width 0.5s ease",
              }} />
            </div>
          )}

          {/* Last log line when running (only if no progress parsed, to avoid clutter) */}
          {isRunning && pct === null && lastLine && (
            <div style={{
              color: PALETTE.textMuted, fontSize: 10, marginTop: 3,
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              maxWidth: 400, fontFamily: "'JetBrains Mono', monospace",
            }}>
              {lastLine}
            </div>
          )}
        </div>

        <div style={{ display: "flex", gap: 8 }}>
          <button
            onClick={handleStart}
            disabled={isRunning || busy}
            style={{
              background: isRunning || busy ? PALETTE.surfaceLight : PALETTE.green,
              color: isRunning || busy ? PALETTE.textMuted : "#000",
              border: "none", borderRadius: 8, padding: "8px 16px",
              fontSize: 13, fontWeight: 600,
              cursor: isRunning || busy ? "default" : "pointer",
            }}
          >
            {startLabel}
          </button>
          <button
            onClick={handleStop}
            disabled={!isRunning || busy}
            style={{
              background: !isRunning || busy ? PALETTE.surfaceLight : PALETTE.red,
              color: !isRunning || busy ? PALETTE.textMuted : "#fff",
              border: "none", borderRadius: 8, padding: "8px 16px",
              fontSize: 13, fontWeight: 600,
              cursor: !isRunning || busy ? "default" : "pointer",
            }}
          >
            Stop
          </button>
          <button
            onClick={handleKill}
            disabled={busy}
            title="Kill any running instance, even if started from terminal"
            style={{
              background: busy ? PALETTE.surfaceLight : PALETTE.accentWarm,
              color: busy ? PALETTE.textMuted : "#fff",
              border: "none", borderRadius: 8, padding: "8px 12px",
              fontSize: 13, fontWeight: 600,
              cursor: busy ? "default" : "pointer",
            }}
          >
            Kill
          </button>
          <button
            onClick={() => setLogsOpen(!logsOpen)}
            style={{
              background: PALETTE.surfaceLight, color: PALETTE.text,
              border: `1px solid ${PALETTE.border}`, borderRadius: 8,
              padding: "8px 12px", fontSize: 13, cursor: "pointer",
            }}
          >
            {logsOpen ? "Hide Logs" : "Logs"}
          </button>
        </div>
      </div>
      {logsOpen && (
        <div style={{
          marginTop: 12, background: PALETTE.bg, border: `1px solid ${PALETTE.border}`,
          borderRadius: 8, padding: 12, maxHeight: 240, overflowY: "auto",
          fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: PALETTE.textMuted,
          whiteSpace: "pre", lineHeight: 1.5,
        }}>
          {logs.length > 0 ? logs.join("\n") : "No output yet."}
        </div>
      )}
    </div>
  );
}
