import { useState, useEffect, useCallback } from "react";
import { PALETTE } from "../theme";
import { api } from "../lib/api";

const STATUS_COLOURS = {
  idle: PALETTE.textMuted,
  running: PALETTE.accent,
  finished: PALETTE.green,
  error: PALETTE.red,
};

export function ProcessRow({ name, label, startLabel, onFlash }) {
  const [status, setStatus] = useState("idle");
  const [pid, setPid] = useState(null);
  const [busy, setBusy] = useState(false);
  const [logsOpen, setLogsOpen] = useState(false);
  const [logs, setLogs] = useState([]);

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

  useEffect(() => {
    if (!logsOpen) return;
    const fetchLogs = async () => {
      try {
        const res = await api.getProcessLogs(name, 50);
        setLogs(res.lines || []);
      } catch { /* ignore */ }
    };
    fetchLogs();
    const id = setInterval(fetchLogs, 2000);
    return () => clearInterval(id);
  }, [logsOpen, name]);

  const isRunning = status === "running";

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
    } catch (e) {
      onFlash(`Failed to stop ${label}: ${e.message}`);
    }
    setBusy(false);
    poll();
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16, marginBottom: 8 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        <div style={{ width: 10, height: 10, borderRadius: "50%", background: STATUS_COLOURS[status] || PALETTE.textMuted, flexShrink: 0 }} />
        <div style={{ flex: "1 1 120px", minWidth: 0 }}>
          <div style={{ color: PALETTE.text, fontSize: 14, fontWeight: 600 }}>{label}</div>
          <div style={{ color: PALETTE.textMuted, fontSize: 11 }}>
            {status}{pid ? ` (pid ${pid})` : ""}
          </div>
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
