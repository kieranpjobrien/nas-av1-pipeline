import { useState, useEffect } from "react";
import { PALETTE, getCodecColour, getResColour } from "../theme";
import { fmt, fmtHrs } from "../lib/format";
import { api } from "../lib/api";

function fileSaving(f) {
  const codec = f.video?.codec_raw;
  if (codec === "av1") return 0;
  const size = f.file_size_gb;
  if (f.video?.codec === "H.264") return size * 0.55;
  if (f.video?.codec === "HEVC (H.265)") return size * 0.25;
  return size * 0.6;
}

export function TopFiles({ files, title, limit = 15 }) {
  const [dismissed, setDismissed] = useState([]);

  useEffect(() => {
    api.getDismissed("files").then(setDismissed).catch(() => {});
  }, []);

  const dismiss = async (filepath) => {
    const next = [...dismissed, filepath];
    setDismissed(next);
    try { await api.setDismissed("files", next); } catch { /* ignore */ }
  };

  const filtered = files.filter((f) => !dismissed.includes(f.filepath));
  const sorted = [...filtered].sort((a, b) => b.file_size_gb - a.file_size_gb).slice(0, limit);
  const maxSize = sorted[0]?.file_size_gb || 1;
  const mono = { fontFamily: "'JetBrains Mono', monospace" };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
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
        {sorted.map((f, i) => {
          const saving = fileSaving(f);
          const afterSize = f.file_size_gb - saving;
          const hrs = (f.duration_seconds || 0) / 3600;
          return (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 12 }}>
              <span style={{ ...mono, color: PALETTE.textMuted, width: 20, textAlign: "right", fontSize: 11 }}>{i + 1}</span>
              <div style={{ flex: "0 1 40%", position: "relative", height: 22, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
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
              <span
                onClick={() => dismiss(f.filepath)}
                style={{ cursor: "pointer", color: PALETTE.textMuted, fontSize: 11, opacity: 0.4, marginLeft: 2 }}
                title="Dismiss from list"
              >✕</span>
            </div>
          );
        })}
      </div>
      <div style={{ display: "flex", gap: 16, marginTop: 10, fontSize: 11, color: PALETTE.textMuted }}>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.accent, opacity: 0.4, marginRight: 4, verticalAlign: "middle" }} />Current</span>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.green, opacity: 0.5, marginRight: 4, verticalAlign: "middle" }} />Est. after AV1</span>
      </div>
    </div>
  );
}
