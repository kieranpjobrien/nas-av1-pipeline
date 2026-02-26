import { PALETTE, getCodecColour, getResColour } from "../theme";
import { fmt } from "../lib/format";

function fileSaving(f) {
  const codec = f.video?.codec_raw;
  if (codec === "av1") return 0;
  const size = f.file_size_gb;
  if (f.video?.codec === "H.264") return size * 0.55;
  if (f.video?.codec === "HEVC (H.265)") return size * 0.25;
  return size * 0.6;
}

export function TopFiles({ files, title, limit = 15 }) {
  const sorted = [...files].sort((a, b) => b.file_size_gb - a.file_size_gb).slice(0, limit);
  const maxSize = sorted[0]?.file_size_gb || 1;

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
      <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>{title}</div>
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {sorted.map((f, i) => {
          const saving = fileSaving(f);
          const afterSize = f.file_size_gb - saving;
          return (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12 }}>
              <span style={{ color: PALETTE.textMuted, width: 24, textAlign: "right", fontFamily: "'JetBrains Mono', monospace" }}>{i + 1}</span>
              <div style={{ flex: 1, position: "relative", height: 26, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
                {/* Current size bar (codec-coloured) */}
                <div style={{
                  position: "absolute", left: 0, top: 0, bottom: 0,
                  width: `${(f.file_size_gb / maxSize) * 100}%`,
                  background: getCodecColour(f.video?.codec),
                  opacity: 0.25,
                  borderRadius: 4,
                }} />
                {/* Estimated post-AV1 bar */}
                {saving > 0.01 && (
                  <div style={{
                    position: "absolute", left: 0, top: 0, bottom: 0,
                    width: `${(afterSize / maxSize) * 100}%`,
                    background: PALETTE.green,
                    opacity: 0.35,
                    borderRadius: 4,
                  }} />
                )}
                <div style={{ position: "relative", padding: "4px 8px", color: PALETTE.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {f.filename}
                </div>
              </div>
              <span style={{ color: PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace", minWidth: 60, textAlign: "right" }}>{fmt(f.file_size_gb)}</span>
              {saving > 0.01 && (
                <span style={{ color: PALETTE.green, fontFamily: "'JetBrains Mono', monospace", fontSize: 11, minWidth: 60, textAlign: "right" }}>-{fmt(saving)}</span>
              )}
              <span style={{ color: getCodecColour(f.video?.codec), minWidth: 80, textAlign: "right", fontSize: 11 }}>{f.video?.codec}</span>
              <span style={{ color: getResColour(f.video?.resolution_class), minWidth: 50, textAlign: "right", fontSize: 11 }}>{f.video?.resolution_class}</span>
            </div>
          );
        })}
      </div>
      <div style={{ display: "flex", gap: 16, marginTop: 12, fontSize: 11, color: PALETTE.textMuted }}>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.accent, opacity: 0.4, marginRight: 4, verticalAlign: "middle" }} />Current</span>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.green, opacity: 0.5, marginRight: 4, verticalAlign: "middle" }} />Est. after AV1</span>
      </div>
    </div>
  );
}
