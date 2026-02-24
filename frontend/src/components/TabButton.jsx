import { PALETTE } from "../theme";

export function TabButton({ active, onClick, children }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: active ? PALETTE.accent : "transparent",
        color: active ? "#fff" : PALETTE.textMuted,
        border: `1px solid ${active ? PALETTE.accent : PALETTE.border}`,
        borderRadius: 8,
        padding: "8px 18px",
        fontSize: 13,
        fontWeight: 500,
        cursor: "pointer",
        transition: "all 0.15s",
      }}
    >
      {children}
    </button>
  );
}
