import { useState, useEffect } from "react";
import { PALETTE } from "../theme";

export function PathEditor({ title, paths, onSave }) {
  const [text, setText] = useState(paths.join("\n"));
  const [saved, setSaved] = useState(false);

  useEffect(() => { setText(paths.join("\n")); }, [paths]);

  const handleSave = async () => {
    const newPaths = text.split("\n").map(p => p.trim()).filter(Boolean);
    await onSave(newPaths);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600 }}>{title}</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {saved && <span style={{ color: PALETTE.green, fontSize: 13 }}>Saved</span>}
          <button
            onClick={handleSave}
            style={{ background: PALETTE.accent, color: "#fff", border: "none", borderRadius: 8, padding: "8px 16px", fontSize: 13, fontWeight: 600, cursor: "pointer" }}
          >
            Save
          </button>
        </div>
      </div>
      <textarea
        value={text}
        onChange={(e) => setText(e.target.value)}
        placeholder="One path per line..."
        style={{
          width: "100%",
          minHeight: 100,
          background: PALETTE.surfaceLight,
          border: `1px solid ${PALETTE.border}`,
          borderRadius: 8,
          color: PALETTE.text,
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 12,
          padding: 12,
          resize: "vertical",
          boxSizing: "border-box",
        }}
      />
      <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 4 }}>
        {text.split("\n").filter(l => l.trim()).length} paths
      </div>
    </div>
  );
}
