import { PALETTE } from "../theme";

export function SectionTitle({ children }) {
  return (
    <h2 style={{ color: PALETTE.text, fontSize: 18, fontWeight: 600, margin: "32px 0 16px", borderBottom: `1px solid ${PALETTE.border}`, paddingBottom: 8 }}>
      {children}
    </h2>
  );
}
