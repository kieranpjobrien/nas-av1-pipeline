import { PALETTE } from "../theme";

function tempColour(temp) {
  if (temp >= 85) return PALETTE.red;
  if (temp >= 70) return PALETTE.accentWarm;
  return PALETTE.green;
}

export function GpuWidget({ gpu }) {
  if (!gpu || !gpu.available) return null;

  const memPct = gpu.mem_total_mb > 0
    ? Math.round((gpu.mem_used_mb / gpu.mem_total_mb) * 100)
    : 0;

  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      gap: 12,
      padding: "6px 14px",
      background: PALETTE.surface,
      border: `1px solid ${PALETTE.border}`,
      borderRadius: 8,
      fontSize: 12,
      fontFamily: "'JetBrains Mono', monospace",
      whiteSpace: "nowrap",
    }}>
      {/* GPU name */}
      <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>
        {gpu.name?.replace("NVIDIA ", "").replace("GeForce ", "") || "GPU"}
      </span>

      {/* Temperature */}
      <span style={{ color: tempColour(gpu.temp_c), fontWeight: 600 }}>
        {gpu.temp_c}°C
      </span>

      {/* Encoder utilisation */}
      <span style={{ color: PALETTE.text }}>
        <span style={{ color: PALETTE.textMuted }}>ENC </span>
        {gpu.encoder_util}%
      </span>

      {/* GPU utilisation */}
      <span style={{ color: PALETTE.text }}>
        <span style={{ color: PALETTE.textMuted }}>GPU </span>
        {gpu.gpu_util}%
      </span>

      {/* VRAM bar */}
      <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
        <span style={{ color: PALETTE.textMuted }}>VRAM</span>
        <div style={{
          width: 48,
          height: 6,
          background: PALETTE.bg,
          borderRadius: 3,
          overflow: "hidden",
        }}>
          <div style={{
            width: `${memPct}%`,
            height: "100%",
            background: memPct > 90 ? PALETTE.red : PALETTE.accent,
            borderRadius: 3,
            transition: "width 0.3s",
          }} />
        </div>
        <span style={{ color: PALETTE.textMuted, fontSize: 10 }}>{memPct}%</span>
      </div>

      {/* Power */}
      <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>
        {Math.round(gpu.power_w)}W
      </span>
    </div>
  );
}
