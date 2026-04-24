import { useState } from "react";
import { PALETTE } from "./theme";
import { PipelinePage } from "./pages/PipelinePage";
import { LibraryPage } from "./pages/LibraryPage";
import { ControlPage } from "./pages/ControlPage";
import { HistoryPage } from "./pages/HistoryPage";
import { DashboardPage } from "./pages/DashboardPage";
import { UpgradesPage } from "./pages/UpgradesPage";
import { GpuWidget } from "./components/GpuWidget";
import { FileDrawer } from "./components/FileDrawer";
import { useWebSocket } from "./lib/useWebSocket";
import { api } from "./lib/api";

const TABS = [
  { id: "dashboard", label: "Dashboard" },
  { id: "pipeline", label: "Pipeline" },
  { id: "library", label: "Library" },
  { id: "upgrades", label: "Upgrades" },
  { id: "controls", label: "Controls" },
  { id: "history", label: "History" },
];

export default function App() {
  const [tab, setTab] = useState(() => localStorage.getItem("nc.tab") || "dashboard");
  const [drawerPath, setDrawerPath] = useState(null);
  const { pipeline, gpu, control, connected } = useWebSocket(api.getPipeline, 3000);

  const setTabPersist = (t) => {
    setTab(t);
    localStorage.setItem("nc.tab", t);
  };

  // When Dashboard is selected, hand over the full viewport to the operator console —
  // it has its own sidebar/topbar and doesn't co-exist well with a second chrome above it.
  if (tab === "dashboard") {
    return (
      <>
        <DashboardPage onClassic={() => setTabPersist("pipeline")} onFileClick={setDrawerPath} />
        {drawerPath && <FileDrawer path={drawerPath} onClose={() => setDrawerPath(null)} />}
      </>
    );
  }

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
        {/* Prominent "back to new UI" button — previously the Dashboard tab was styled
            identically to the other tabs and easy to miss. This one jumps out. */}
        <button
          onClick={() => setTabPersist("dashboard")}
          title="Back to the operator console"
          style={{
            background: PALETTE.accent,
            color: "#fff",
            border: "none",
            borderRadius: 8,
            padding: "8px 16px",
            fontSize: 13,
            fontWeight: 600,
            cursor: "pointer",
            marginRight: 8,
          }}
        >
          ← Dashboard
        </button>
        {TABS.filter(({ id }) => id !== "dashboard").map(({ id, label }) => (
          <button
            key={id}
            onClick={() => setTabPersist(id)}
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

        {/* Spacer */}
        <div style={{ flex: 1 }} />

        {/* GPU widget */}
        <GpuWidget gpu={gpu} />

        {/* WS connection indicator */}
        <div
          title={connected ? "Live (WebSocket)" : "Polling (WebSocket disconnected)"}
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: connected ? PALETTE.green : PALETTE.accentWarm,
            marginLeft: 4,
            flexShrink: 0,
          }}
        />
      </nav>

      {/* Page content */}
      <div style={{ maxWidth: 1200, margin: "0 auto", padding: "24px 24px 64px" }}>
        {tab === "pipeline" && <PipelinePage wsData={pipeline} onFileClick={setDrawerPath} />}
        {tab === "library" && <LibraryPage onFileClick={setDrawerPath} />}
        {tab === "upgrades" && <UpgradesPage onFileClick={setDrawerPath} />}
        {tab === "controls" && <ControlPage wsControl={control} />}
        {tab === "history" && <HistoryPage />}
      </div>

      {/* File detail drawer */}
      {drawerPath && <FileDrawer path={drawerPath} onClose={() => setDrawerPath(null)} />}
    </div>
  );
}
