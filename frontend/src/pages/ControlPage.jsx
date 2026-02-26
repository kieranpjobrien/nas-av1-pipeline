import { useState, useEffect } from "react";
import { PALETTE } from "../theme";
import { usePolling } from "../lib/usePolling";
import { api } from "../lib/api";
import { SectionTitle } from "../components/SectionTitle";
import { ProcessRow } from "../components/ProcessRow";
import { PauseButton, BTN_BASE } from "../components/PauseButton";
import { PathEditor } from "../components/PathEditor";
import { GentleEditor } from "../components/GentleEditor";
import { MediaSearch } from "../components/MediaSearch";

export function ControlPage() {
  const { data: status, refresh } = usePolling(api.getControlStatus, 3000);
  const [skip, setSkip] = useState(null);
  const [priority, setPriority] = useState(null);
  const [gentle, setGentle] = useState(null);
  const [flash, setFlash] = useState(null);

  useEffect(() => {
    api.getSkip().then(setSkip);
    api.getGentle().then(setGentle);
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

  const handleSearchAdd = async (list, paths) => {
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
      <ProcessRow name="scanner" label="Media Scanner" startLabel="Rescan Library" onFlash={showFlash} />
      <ProcessRow name="pipeline" label="AV1 Pipeline" startLabel="Start Pipeline" onFlash={showFlash} />
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
        />
      )}

      {/* Gentle overrides */}
      <SectionTitle>Gentle Overrides</SectionTitle>
      {gentle && (
        <GentleEditor
          gentle={gentle}
          onSave={async (data) => { await api.setGentle(data); setGentle(data); }}
        />
      )}
    </div>
  );
}
