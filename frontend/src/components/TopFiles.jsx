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

const CHK = { width: 13, height: 13, cursor: "pointer", margin: 0 };

export function TopFiles({ files, title, limit = 15 }) {
  const [dismissed, setDismissed] = useState([]);
  const [priority, setPriority] = useState(new Set());
  const [gentle, setGentle] = useState(new Set());
  const [reencode, setReencode] = useState({}); // filepath -> cq
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.getDismissed("files").then(setDismissed).catch(() => {});
  }, []);

  const filtered = files.filter((f) => !dismissed.includes(f.filepath));
  const sorted = [...filtered].sort((a, b) => b.file_size_gb - a.file_size_gb).slice(0, limit);
  const maxSize = sorted[0]?.file_size_gb || 1;
  const mono = { fontFamily: "'JetBrains Mono', monospace" };

  const hasActions = priority.size > 0 || gentle.size > 0 || Object.keys(reencode).length > 0;

  const togglePriority = (fp) => {
    const next = new Set(priority);
    next.has(fp) ? next.delete(fp) : next.add(fp);
    setPriority(next);
  };

  const toggleGentle = (fp) => {
    const next = new Set(gentle);
    next.has(fp) ? next.delete(fp) : next.add(fp);
    setGentle(next);
  };

  const toggleReencode = (f) => {
    const next = { ...reencode };
    if (next[f.filepath]) {
      delete next[f.filepath];
    } else {
      next[f.filepath] = getBaseCQ(f);
    }
    setReencode(next);
  };

  const setCQ = (fp, val) => {
    setReencode((prev) => ({ ...prev, [fp]: val }));
  };

  const submit = async () => {
    setBusy(true);
    const toDismiss = new Set();
    try {
      if (priority.size > 0) {
        const paths = [...priority];
        const current = await api.getPriority().catch(() => ({ paths: [] }));
        await api.setPriority([...new Set([...(current.paths || []), ...paths])]);
        paths.forEach((p) => toDismiss.add(p));
      }
      if (gentle.size > 0) {
        const paths = [...gentle];
        const current = await api.getGentle().catch(() => ({ paths: {}, patterns: {}, default_offset: 0 }));
        const updated = { ...current, paths: { ...current.paths } };
        for (const p of paths) updated.paths[p] = { cq_offset: 2 };
        await api.setGentle(updated);
        paths.forEach((p) => toDismiss.add(p));
      }
      if (Object.keys(reencode).length > 0) {
        const current = await api.getReencode().catch(() => ({ files: {}, patterns: {} }));
        const updatedFiles = { ...(current.files || {}) };
        for (const [fp, cq] of Object.entries(reencode)) {
          updatedFiles[fp] = { cq };
          toDismiss.add(fp);
        }
        await api.setReencode(updatedFiles, current.patterns || {});
      }
      if (toDismiss.size > 0) {
        const next = [...dismissed, ...toDismiss];
        setDismissed(next);
        try { await api.setDismissed("files", next); } catch {}
      }
    } catch { /* ignore */ }
    setPriority(new Set());
    setGentle(new Set());
    setReencode({});
    setBusy(false);
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600 }}>{title}</div>
        {dismissed.length > 0 && (
          <button
            onClick={async () => { setDismissed([]); try { await api.setDismissed("files", []); } catch {} }}
            style={{ background: "none", border: "none", color: PALETTE.textMuted, fontSize: 11, cursor: "pointer", textDecoration: "underline" }}
          >
            Reset {dismissed.length} dismissed
          </button>
        )}
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, color: PALETTE.textMuted, marginBottom: 2, paddingRight: 2 }}>
          <span style={{ width: 20 }} />
          <span style={{ flex: "0 1 38%" }}>File</span>
          <span style={{ ...mono, minWidth: 50, textAlign: "right" }}>Size</span>
          <span style={{ ...mono, minWidth: 48, textAlign: "right" }}>Saving</span>
          <span style={{ ...mono, minWidth: 36, textAlign: "right" }}>Dur</span>
          <span style={{ minWidth: 62, textAlign: "right" }}>Codec</span>
          <span style={{ minWidth: 36, textAlign: "right" }}>Res</span>
          <span style={{ minWidth: 16, textAlign: "center", color: PALETTE.green }} title="Priority">⚡</span>
          <span style={{ minWidth: 16, textAlign: "center", color: "#8b8bef" }} title="Gentle +2 CQ">G</span>
          <span style={{ minWidth: 16, textAlign: "center", color: PALETTE.accentWarm }} title="Re-encode">R</span>
          <span style={{ minWidth: 36 }} />
        </div>
        {sorted.map((f, i) => {
          const saving = fileSaving(f);
          const afterSize = f.file_size_gb - saving;
          const hrs = (f.duration_seconds || 0) / 3600;
          const isPri = priority.has(f.filepath);
          const isGen = gentle.has(f.filepath);
          const isRe = f.filepath in reencode;
          const anyAction = isPri || isGen || isRe;
          return (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: 6, fontSize: 12,
              background: anyAction ? `${PALETTE.accent}08` : "transparent",
              borderRadius: 4, padding: "1px 0",
            }}>
              <span style={{ ...mono, color: PALETTE.textMuted, width: 20, textAlign: "right", fontSize: 11 }}>{i + 1}</span>
              <div style={{ flex: "0 1 38%", position: "relative", height: 22, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
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
              <span style={{ ...mono, color: PALETTE.textMuted, minWidth: 50, textAlign: "right", fontSize: 11 }}>{fmt(f.file_size_gb)}</span>
              {saving > 0.01 ? (
                <span style={{ ...mono, color: PALETTE.green, fontSize: 11, minWidth: 48, textAlign: "right" }}>-{fmt(saving)}</span>
              ) : (
                <span style={{ minWidth: 48 }} />
              )}
              <span style={{ ...mono, color: PALETTE.accent, fontSize: 11, minWidth: 36, textAlign: "right" }}>{fmtHrs(hrs)}</span>
              <span style={{ color: getCodecColour(f.video?.codec), minWidth: 62, textAlign: "right", fontSize: 11 }}>{f.video?.codec}</span>
              <span style={{ color: getResColour(f.video?.resolution_class), minWidth: 36, textAlign: "right", fontSize: 11 }}>{f.video?.resolution_class}</span>
              {/* Action checkboxes */}
              <input type="checkbox" checked={isPri} onChange={() => togglePriority(f.filepath)} title="Priority" style={{ ...CHK, accentColor: PALETTE.green }} />
              <input type="checkbox" checked={isGen} onChange={() => toggleGentle(f.filepath)} title="Gentle +2 CQ" style={{ ...CHK, accentColor: "#8b8bef" }} />
              <input type="checkbox" checked={isRe} onChange={() => toggleReencode(f)} title="Re-encode" style={{ ...CHK, accentColor: PALETTE.accentWarm }} />
              {isRe ? (
                <input
                  type="number" value={reencode[f.filepath]} min={1} max={63}
                  onChange={(e) => setCQ(f.filepath, parseInt(e.target.value, 10) || 30)}
                  style={{ ...mono, width: 36, fontSize: 11, padding: "2px 3px", textAlign: "center",
                    background: PALETTE.surfaceLight, color: PALETTE.text,
                    border: `1px solid ${PALETTE.border}`, borderRadius: 3 }}
                  title="CQ value"
                />
              ) : (
                <span style={{ minWidth: 36 }} />
              )}
            </div>
          );
        })}
      </div>

      {/* Legend */}
      <div style={{ display: "flex", gap: 16, marginTop: 10, fontSize: 11, color: PALETTE.textMuted }}>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.accent, opacity: 0.4, marginRight: 4, verticalAlign: "middle" }} />Current</span>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.green, opacity: 0.5, marginRight: 4, verticalAlign: "middle" }} />Est. after AV1</span>
      </div>

      {/* Submit bar */}
      {hasActions && (
        <div style={{
          marginTop: 12, padding: "8px 14px",
          background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`,
          borderRadius: 8, display: "flex", alignItems: "center", gap: 12,
        }}>
          <span style={{ color: PALETTE.textMuted, fontSize: 12, flex: 1 }}>
            {priority.size > 0 && <span style={{ color: PALETTE.green }}>⚡{priority.size} priority </span>}
            {gentle.size > 0 && <span style={{ color: "#8b8bef" }}>G {gentle.size} gentle </span>}
            {Object.keys(reencode).length > 0 && <span style={{ color: PALETTE.accentWarm }}>R {Object.keys(reencode).length} re-encode </span>}
          </span>
          <button
            onClick={submit} disabled={busy}
            style={{
              border: "none", borderRadius: 6, padding: "7px 20px",
              fontSize: 13, fontWeight: 700, cursor: busy ? "default" : "pointer",
              background: busy ? PALETTE.surfaceLight : PALETTE.accent,
              color: busy ? PALETTE.textMuted : "#fff",
            }}
          >{busy ? "Applying..." : "Apply"}</button>
          <button
            onClick={() => { setPriority(new Set()); setGentle(new Set()); setReencode({}); }}
            style={{ background: "none", border: "none", color: PALETTE.textMuted, fontSize: 11, cursor: "pointer" }}
          >Clear</button>
        </div>
      )}
    </div>
  );
}
