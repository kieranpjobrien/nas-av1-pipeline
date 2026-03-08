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
    </div>
  );
}
