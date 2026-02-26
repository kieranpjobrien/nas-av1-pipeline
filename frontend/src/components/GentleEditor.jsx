import { useState, useEffect } from "react";
import { PALETTE } from "../theme";

export function GentleEditor({ gentle, onSave }) {
  const [text, setText] = useState(JSON.stringify(gentle, null, 2));
  const [saved, setSaved] = useState(false);
  const [parseError, setParseError] = useState(null);

  useEffect(() => { setText(JSON.stringify(gentle, null, 2)); }, [gentle]);

  const handleSave = async () => {
    try {
      const parsed = JSON.parse(text);
      setParseError(null);
      await onSave(parsed);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (e) {
      setParseError(e.message);
    }
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600 }}>Gentle Overrides</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {saved && <span style={{ color: PALETTE.green, fontSize: 13 }}>Saved</span>}
          {parseError && <span style={{ color: PALETTE.red, fontSize: 12 }}>Invalid JSON</span>}
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
        onChange={(e) => { setText(e.target.value); setParseError(null); }}
        style={{
          width: "100%",
          minHeight: 120,
          background: PALETTE.surfaceLight,
          border: `1px solid ${parseError ? PALETTE.red : PALETTE.border}`,
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
        Format: {"{"} "paths": {"{"} "Z:\\path": {"{"}"cq_offset": 2{"}"} {"}"}, "patterns": {"{"}{"}"},  "default_offset": 0 {"}"}
      </div>
    </div>
  );
}
