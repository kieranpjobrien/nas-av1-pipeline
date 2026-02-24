import { PALETTE, getCodecColour, getResColour } from "../theme";
import { fmt } from "../lib/format";

export function TopFiles({ files, title, limit = 15 }) {
  const sorted = [...files].sort((a, b) => b.file_size_gb - a.file_size_gb).slice(0, limit);
  const maxSize = sorted[0]?.file_size_gb || 1;

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
      <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>{title}</div>
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {sorted.map((f, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12 }}>
            <span style={{ color: PALETTE.textMuted, width: 24, textAlign: "right", fontFamily: "'JetBrains Mono', monospace" }}>{i + 1}</span>
            <div style={{ flex: 1, position: "relative", height: 26, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
              <div style={{
                position: "absolute", left: 0, top: 0, bottom: 0,
                width: `${(f.file_size_gb / maxSize) * 100}%`,
                background: getCodecColour(f.video?.codec),
                opacity: 0.4,
                borderRadius: 4,
              }} />
              <div style={{ position: "relative", padding: "4px 8px", color: PALETTE.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {f.filename}
              </div>
            </div>
            <span style={{ color: PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace", minWidth: 60, textAlign: "right" }}>{fmt(f.file_size_gb)}</span>
            <span style={{ color: getCodecColour(f.video?.codec), minWidth: 80, textAlign: "right", fontSize: 11 }}>{f.video?.codec}</span>
            <span style={{ color: getResColour(f.video?.resolution_class), minWidth: 50, textAlign: "right", fontSize: 11 }}>{f.video?.resolution_class}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
