import { PALETTE } from "../theme";

const BTN_BASE = {
  border: "none",
  borderRadius: 12,
  padding: "18px 24px",
  fontSize: 16,
  fontWeight: 700,
  cursor: "pointer",
  minHeight: 56,
  width: "100%",
  transition: "all 0.15s",
  letterSpacing: "0.02em",
};

export { BTN_BASE };

export function PauseButton({ label, active, colour, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        ...BTN_BASE,
        background: active ? PALETTE.red : colour,
        color: active ? "#fff" : "#000",
        opacity: 1,
      }}
    >
      {active ? `${label} (tap to resume)` : label}
    </button>
  );
}
