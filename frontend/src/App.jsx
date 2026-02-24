import { useState } from "react";
import { PALETTE } from "./theme";
import { PipelinePage } from "./pages/PipelinePage";
import { LibraryPage } from "./pages/LibraryPage";
import { ControlPage } from "./pages/ControlPage";

const TABS = [
  { id: "pipeline", label: "Pipeline" },
  { id: "library", label: "Library" },
  { id: "controls", label: "Controls" },
];

export default function App() {
  const [tab, setTab] = useState("pipeline");

  return (
    <div style={{
      minHeight: "100vh",
      background: PALETTE.bg,
      fontFamily: "'Instrument Sans', system-ui, -apple-system, sans-serif",
      color: PALETTE.text,
    }}>
      <link href="https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />

      {/* Top nav */}
      <nav style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "12px 24px",
        borderBottom: `1px solid ${PALETTE.border}`,
        background: PALETTE.surface,
        position: "sticky",
        top: 0,
        zIndex: 100,
      }}>
        <span style={{ fontWeight: 700, fontSize: 16, marginRight: 16 }}>AV1 Pipeline</span>
        {TABS.map(({ id, label }) => (
          <button
            key={id}
            onClick={() => setTab(id)}
            style={{
              background: tab === id ? PALETTE.accent : "transparent",
              color: tab === id ? "#fff" : PALETTE.textMuted,
              border: `1px solid ${tab === id ? PALETTE.accent : PALETTE.border}`,
              borderRadius: 8,
              padding: "8px 18px",
              fontSize: 13,
              fontWeight: 500,
              cursor: "pointer",
              transition: "all 0.15s",
            }}
          >
            {label}
          </button>
        ))}
      </nav>

      {/* Page content */}
      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "24px 24px 64px" }}>
        {tab === "pipeline" && <PipelinePage />}
        {tab === "library" && <LibraryPage />}
        {tab === "controls" && <ControlPage />}
      </div>
    </div>
  );
}
