import { PALETTE } from "../theme";

export function StatCard({ label, value, sub, colour }) {
  return (
    <div style={{
      background: PALETTE.surface,
      border: `1px solid ${PALETTE.border}`,
      borderRadius: 12,
      padding: "20px 24px",
      flex: "1 1 180px",
      minWidth: 160,
    }}>
      <div style={{ color: PALETTE.textMuted, fontSize: 12, fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>{label}</div>
      <div style={{ color: colour || PALETTE.text, fontSize: 28, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", lineHeight: 1.1 }}>{value}</div>
      {sub && <div style={{ color: PALETTE.textMuted, fontSize: 12, marginTop: 4 }}>{sub}</div>}
    </div>
  );
}
