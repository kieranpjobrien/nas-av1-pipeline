import { useState, useEffect } from "react";
import { PALETTE, getCodecColour, getResColour } from "../theme";
import { fmt, fmtHrs } from "../lib/format";
import { api } from "../lib/api";

const CQ_TABLE = {
  movie:  { "4K": 22, "1080p": 28, "720p": 30, "480p": 30, "SD": 30 },
  series: { "4K": 24, "1080p": 30, "720p": 32, "480p": 32, "SD": 32 },
};

function fileSaving(f) {
  const codec = f.video?.codec_raw;
  if (codec === "av1") return 0;
  const size = f.file_size_gb;
  if (f.video?.codec === "H.264") return size * 0.55;
  if (f.video?.codec === "HEVC (H.265)") return size * 0.25;
  return size * 0.6;
}

function getContentType(filepath) {
  return /[/\\]Movies[/\\]/i.test(filepath) ? "movie" : "series";
}

function getBaseCQ(f) {
  const type = getContentType(f.filepath);
  const res = f.video?.resolution_class || "1080p";
  return CQ_TABLE[type]?.[res] ?? 30;
}

const BTN = {
  border: "none", borderRadius: 6, padding: "6px 14px",
  fontSize: 12, fontWeight: 600, cursor: "pointer",
};

export function TopFiles({ files, title, limit = 15 }) {
  const [dismissed, setDismissed] = useState([]);
  const [selected, setSelected] = useState(new Set());
  const [cqValue, setCqValue] = useState(30);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.getDismissed("files").then(setDismissed).catch(() => {});
  }, []);

  const filtered = files.filter((f) => !dismissed.includes(f.filepath));
  const sorted = [...filtered].sort((a, b) => b.file_size_gb - a.file_size_gb).slice(0, limit);
  const maxSize = sorted[0]?.file_size_gb || 1;
  const mono = { fontFamily: "'JetBrains Mono', monospace" };

  const toggle = (filepath) => {
    const next = new Set(selected);
    if (next.has(filepath)) next.delete(filepath);
    else next.add(filepath);
    setSelected(next);
    // Update CQ default from first selected
    if (next.size > 0) {
      const first = sorted.find((f) => next.has(f.filepath));
      if (first) setCqValue(getBaseCQ(first));
    }
  };

  const toggleAll = () => {
    if (selected.size === sorted.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(sorted.map((f) => f.filepath)));
      if (sorted.length > 0) setCqValue(getBaseCQ(sorted[0]));
    }
  };

  const dismissSelected = async (paths) => {
    const next = [...dismissed, ...paths];
    setDismissed(next);
    setSelected(new Set());
    try { await api.setDismissed("files", next); } catch { /* ignore */ }
  };

  const selectedPaths = [...selected];

  const addToPriority = async () => {
    setBusy(true);
    try {
      const current = await api.getPriority().catch(() => ({ paths: [] }));
      const merged = [...new Set([...(current.paths || []), ...selectedPaths])];
      await api.setPriority(merged);
      await dismissSelected(selectedPaths);
    } catch { /* ignore */ }
    setBusy(false);
  };

  const addToSkip = async () => {
    setBusy(true);
    try {
      const current = await api.getSkip().catch(() => ({ paths: [] }));
      const merged = [...new Set([...(current.paths || []), ...selectedPaths])];
      await api.setSkip(merged);
      await dismissSelected(selectedPaths);
    } catch { /* ignore */ }
    setBusy(false);
  };

  const addToGentle = async () => {
    setBusy(true);
    try {
      const current = await api.getGentle().catch(() => ({ paths: {}, patterns: {}, default_offset: 0 }));
      const updated = { ...current, paths: { ...current.paths } };
      for (const p of selectedPaths) {
        if (!updated.paths[p]) updated.paths[p] = { cq_offset: 2 };
      }
      await api.setGentle(updated);
      await dismissSelected(selectedPaths);
    } catch { /* ignore */ }
    setBusy(false);
  };

  const addToReencode = async () => {
    setBusy(true);
    try {
      const current = await api.getReencode().catch(() => ({ files: {}, patterns: {} }));
      const updatedFiles = { ...(current.files || {}) };
      for (const p of selectedPaths) {
        updatedFiles[p] = { cq: cqValue };
      }
      await api.setReencode(updatedFiles, current.patterns || {});
      await dismissSelected(selectedPaths);
    } catch { /* ignore */ }
    setBusy(false);
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, position: "relative" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600 }}>{title}</div>
        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          {dismissed.length > 0 && (
            <button
              onClick={async () => { setDismissed([]); try { await api.setDismissed("files", []); } catch {} }}
              style={{ background: "none", border: "none", color: PALETTE.textMuted, fontSize: 11, cursor: "pointer", textDecoration: "underline" }}
            >
              Reset {dismissed.length} dismissed
            </button>
          )}
        </div>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
        {/* Header row with select-all */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11, color: PALETTE.textMuted, marginBottom: 2 }}>
          <input
            type="checkbox"
            checked={selected.size === sorted.length && sorted.length > 0}
            onChange={toggleAll}
            style={{ width: 14, height: 14, cursor: "pointer", accentColor: PALETTE.accent }}
          />
          <span style={{ width: 20 }} />
          <span style={{ flex: "0 1 40%" }}>File</span>
          <span style={{ ...mono, minWidth: 55, textAlign: "right" }}>Size</span>
          <span style={{ ...mono, minWidth: 52, textAlign: "right" }}>Saving</span>
          <span style={{ ...mono, minWidth: 40, textAlign: "right" }}>Dur</span>
          <span style={{ minWidth: 70, textAlign: "right" }}>Codec</span>
          <span style={{ minWidth: 40, textAlign: "right" }}>Res</span>
        </div>
        {sorted.map((f, i) => {
          const saving = fileSaving(f);
          const afterSize = f.file_size_gb - saving;
          const hrs = (f.duration_seconds || 0) / 3600;
          const isSelected = selected.has(f.filepath);
          return (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: 8, fontSize: 12,
              background: isSelected ? `${PALETTE.accent}11` : "transparent",
              borderRadius: 4, padding: "1px 0",
            }}>
              <input
                type="checkbox"
                checked={isSelected}
                onChange={() => toggle(f.filepath)}
                style={{ width: 14, height: 14, cursor: "pointer", accentColor: PALETTE.accent }}
              />
              <span style={{ ...mono, color: PALETTE.textMuted, width: 20, textAlign: "right", fontSize: 11 }}>{i + 1}</span>
              <div style={{ flex: "0 1 40%", position: "relative", height: 22, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden", cursor: "pointer" }} onClick={() => toggle(f.filepath)}>
                <div style={{
                  position: "absolute", left: 0, top: 0, bottom: 0,
                  width: `${(f.file_size_gb / maxSize) * 100}%`,
                  background: getCodecColour(f.video?.codec), opacity: 0.25, borderRadius: 4,
                }} />
                {saving > 0.01 && (
                  <div style={{
                    position: "absolute", left: 0, top: 0, bottom: 0,
                    width: `${(afterSize / maxSize) * 100}%`,
                    background: PALETTE.green, opacity: 0.35, borderRadius: 4,
                  }} />
                )}
                <div style={{ position: "relative", padding: "2px 8px", color: PALETTE.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", lineHeight: "18px" }}>
                  {f.filename}
                </div>
              </div>
              <span style={{ ...mono, color: PALETTE.textMuted, minWidth: 55, textAlign: "right", fontSize: 11 }}>{fmt(f.file_size_gb)}</span>
              {saving > 0.01 ? (
                <span style={{ ...mono, color: PALETTE.green, fontSize: 11, minWidth: 52, textAlign: "right" }}>-{fmt(saving)}</span>
              ) : (
                <span style={{ minWidth: 52 }} />
              )}
              <span style={{ ...mono, color: PALETTE.accent, fontSize: 11, minWidth: 40, textAlign: "right" }}>{fmtHrs(hrs)}</span>
              <span style={{ color: getCodecColour(f.video?.codec), minWidth: 70, textAlign: "right", fontSize: 11 }}>{f.video?.codec}</span>
              <span style={{ color: getResColour(f.video?.resolution_class), minWidth: 40, textAlign: "right", fontSize: 11 }}>{f.video?.resolution_class}</span>
            </div>
          );
        })}
      </div>

      {/* Legend */}
      <div style={{ display: "flex", gap: 16, marginTop: 10, fontSize: 11, color: PALETTE.textMuted }}>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.accent, opacity: 0.4, marginRight: 4, verticalAlign: "middle" }} />Current</span>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.green, opacity: 0.5, marginRight: 4, verticalAlign: "middle" }} />Est. after AV1</span>
      </div>

      {/* Action bar */}
      {selected.size > 0 && (
        <div style={{
          marginTop: 12, padding: "10px 14px",
          background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`,
          borderRadius: 8, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap",
        }}>
          <span style={{ color: PALETTE.text, fontSize: 12, fontWeight: 600, marginRight: 4 }}>
            {selected.size} selected
          </span>
          <button onClick={addToPriority} disabled={busy} style={{ ...BTN, background: PALETTE.green, color: "#000" }}>Priority</button>
          <button onClick={addToSkip} disabled={busy} style={{ ...BTN, background: PALETTE.accentWarm, color: "#fff" }}>Skip</button>
          <button onClick={addToGentle} disabled={busy} style={{ ...BTN, background: "#8b8bef", color: "#fff" }}>Gentle +2</button>
          <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <button onClick={addToReencode} disabled={busy} style={{ ...BTN, background: PALETTE.accent, color: "#fff" }}>Re-encode</button>
            <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>CQ</span>
            <input
              type="number"
              value={cqValue}
              onChange={(e) => setCqValue(parseInt(e.target.value, 10) || 30)}
              min={1} max={63}
              style={{
                width: 44, ...mono, fontSize: 12, padding: "4px 6px",
                background: PALETTE.surface, color: PALETTE.text,
                border: `1px solid ${PALETTE.border}`, borderRadius: 4,
                textAlign: "center",
              }}
            />
          </span>
          <button
            onClick={() => setSelected(new Set())}
            style={{ ...BTN, background: "transparent", color: PALETTE.textMuted, padding: "6px 8px" }}
          >Clear</button>
        </div>
      )}
    </div>
  );
}
