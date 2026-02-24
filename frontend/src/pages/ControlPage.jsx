import { useState, useEffect, useCallback } from "react";
import { PALETTE } from "../theme";
import { usePolling } from "../lib/usePolling";
import { api } from "../lib/api";
import { SectionTitle } from "../components/SectionTitle";

const STATUS_COLOURS = {
  idle: PALETTE.textMuted,
  running: PALETTE.accent,
  finished: PALETTE.green,
  error: PALETTE.red,
};

function ProcessRow({ name, label, startLabel, onFlash }) {
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

const BTN_BASE = {
  border: "none",
  borderRadius: 12,
  padding: "18px 24px",
  fontSize: 16,
  fontWeight: 700,
  cursor: "pointer",
  minHeight: 56,
  width: "100%",
  transition: "all 0.15s",
  letterSpacing: "0.02em",
};

function PauseButton({ label, active, colour, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        ...BTN_BASE,
        background: active ? PALETTE.red : colour,
        color: active ? "#fff" : "#000",
        opacity: 1,
      }}
    >
      {active ? `${label} (tap to resume)` : label}
    </button>
  );
}

function PathEditor({ title, paths, onSave }) {
  const [text, setText] = useState(paths.join("\n"));
  const [saved, setSaved] = useState(false);

  useEffect(() => { setText(paths.join("\n")); }, [paths]);

  const handleSave = async () => {
    const newPaths = text.split("\n").map(p => p.trim()).filter(Boolean);
    await onSave(newPaths);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600 }}>{title}</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {saved && <span style={{ color: PALETTE.green, fontSize: 13 }}>Saved</span>}
          <button
            onClick={handleSave}
            style={{ background: PALETTE.accent, color: "#fff", border: "none", borderRadius: 8, padding: "8px 16px", fontSize: 13, fontWeight: 600, cursor: "pointer" }}
          >
            Save
          </button>
        </div>
      </div>
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        placeholder="One path per line..."
        style={{
          width: "100%",
          minHeight: 100,
          background: PALETTE.surfaceLight,
          border: `1px solid ${PALETTE.border}`,
          borderRadius: 8,
          color: PALETTE.text,
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 12,
          padding: 12,
          resize: "vertical",
          boxSizing: "border-box",
        }}
      />
      <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 4 }}>
        {text.split("\n").filter(l => l.trim()).length} paths
      </div>
    </div>
  );
}

function GentleEditor({ overrides, onSave }) {
  const [text, setText] = useState(JSON.stringify(overrides, null, 2));
  const [saved, setSaved] = useState(false);
  const [parseError, setParseError] = useState(null);

  useEffect(() => { setText(JSON.stringify(overrides, null, 2)); }, [overrides]);

  const handleSave = async () => {
    try {
      const parsed = JSON.parse(text);
      setParseError(null);
      await onSave(parsed);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (e) {
      setParseError(e.message);
    }
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600 }}>Gentle Overrides</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {saved && <span style={{ color: PALETTE.green, fontSize: 13 }}>Saved</span>}
          {parseError && <span style={{ color: PALETTE.red, fontSize: 12 }}>Invalid JSON</span>}
          <button
            onClick={handleSave}
            style={{ background: PALETTE.accent, color: "#fff", border: "none", borderRadius: 8, padding: "8px 16px", fontSize: 13, fontWeight: 600, cursor: "pointer" }}
          >
            Save
          </button>
        </div>
      </div>
      <textarea
        value={text}
        onChange={(e) => { setText(e.target.value); setParseError(null); }}
        style={{
          width: "100%",
          minHeight: 120,
          background: PALETTE.surfaceLight,
          border: `1px solid ${parseError ? PALETTE.red : PALETTE.border}`,
          borderRadius: 8,
          color: PALETTE.text,
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 12,
          padding: 12,
          resize: "vertical",
          boxSizing: "border-box",
        }}
      />
    </div>
  );
}

export function ControlPage() {
  const { data: status, refresh } = usePolling(api.getControlStatus, 3000);
  const [skip, setSkip] = useState(null);
  const [priority, setPriority] = useState(null);
  const [gentle, setGentle] = useState(null);
  const [flash, setFlash] = useState(null);

  useEffect(() => {
    api.getSkip().then(setSkip);
    api.getPriority().then(setPriority);
    api.getGentle().then(setGentle);
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
    showFlash("Resumed — pipeline will continue");
    refresh();
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
      <ProcessRow name="scanner" label="Media Scanner" startLabel="Rescan Library" onFlash={showFlash} />
      <ProcessRow name="pipeline" label="AV1 Pipeline" startLabel="Start Pipeline" onFlash={showFlash} />
      <div style={{ marginBottom: 32 }} />

      {/* Status indicator */}
      <SectionTitle>Pipeline Control</SectionTitle>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 24 }}>
        <div style={{ width: 16, height: 16, borderRadius: "50%", background: stateInfo.colour }} />
        <span style={{ color: stateInfo.colour, fontSize: 20, fontWeight: 700 }}>{stateInfo.text}</span>
      </div>

      {/* Flash message */}
      {flash && (
        <div style={{ background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`, borderRadius: 8, padding: "10px 16px", marginBottom: 16, color: PALETTE.text, fontSize: 13 }}>
          {flash}
        </div>
      )}

      {/* Pause/Resume buttons */}
      <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 32 }}>
        <PauseButton
          label="PAUSE ALL"
          active={pauseState === "paused_all"}
          colour={PALETTE.accentWarm}
          onClick={() => handlePause("all")}
        />
        <PauseButton
          label="PAUSE FETCHING"
          active={pauseState === "paused_fetch"}
          colour={PALETTE.accent}
          onClick={() => handlePause("fetch")}
        />
        <PauseButton
          label="PAUSE ENCODING"
          active={pauseState === "paused_encode"}
          colour={PALETTE.accent}
          onClick={() => handlePause("encode")}
        />
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
        />
      )}

      {/* Gentle overrides */}
      <SectionTitle>Gentle Overrides</SectionTitle>
      {gentle && (
        <GentleEditor
          overrides={gentle.overrides || {}}
          onSave={async (overrides) => { await api.setGentle(overrides); setGentle({ overrides }); }}
        />
      )}
    </div>
  );
}
