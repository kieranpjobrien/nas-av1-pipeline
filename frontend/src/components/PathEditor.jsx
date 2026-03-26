import { useState, useEffect, useRef } from "react";
import { PALETTE } from "../theme";

export function PathEditor({ title, paths, onSave, reorderable = false }) {
  const [text, setText] = useState(paths.join("\n"));
  const [items, setItems] = useState(paths);
  const [saved, setSaved] = useState(false);
  const [listView, setListView] = useState(reorderable && paths.length > 0);
  const dragIdx = useRef(null);
  const [dragOver, setDragOver] = useState(null);

  useEffect(() => {
    setText(paths.join("\n"));
    setItems([...paths]);
  }, [paths]);

  const handleSave = async () => {
    const newPaths = listView
      ? items.filter(Boolean)
      : text.split("\n").map(p => p.trim()).filter(Boolean);
    await onSave(newPaths);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  const handleDragStart = (idx) => { dragIdx.current = idx; };
  const handleDragOver = (e, idx) => { e.preventDefault(); setDragOver(idx); };
  const handleDragLeave = () => setDragOver(null);
  const handleDrop = (e, dropIdx) => {
    e.preventDefault();
    setDragOver(null);
    const from = dragIdx.current;
    if (from === null || from === dropIdx) return;
    const newItems = [...items];
    const [moved] = newItems.splice(from, 1);
    newItems.splice(dropIdx, 0, moved);
    setItems(newItems);
    dragIdx.current = null;
  };

  const removeItem = (idx) => {
    const newItems = items.filter((_, i) => i !== idx);
    setItems(newItems);
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600 }}>{title}</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {reorderable && (
            <button
              onClick={() => {
                if (listView) {
                  // Sync list → text
                  setText(items.filter(Boolean).join("\n"));
                } else {
                  // Sync text → list
                  setItems(text.split("\n").map(p => p.trim()).filter(Boolean));
                }
                setListView(!listView);
              }}
              style={{
                background: "transparent", color: PALETTE.textMuted, border: `1px solid ${PALETTE.border}`,
                borderRadius: 6, padding: "6px 12px", fontSize: 11, cursor: "pointer",
              }}
            >
              {listView ? "Edit as text" : "Reorder list"}
            </button>
          )}
          {saved && <span style={{ color: PALETTE.green, fontSize: 13 }}>Saved</span>}
          <button
            onClick={handleSave}
            style={{ background: PALETTE.accent, color: "#fff", border: "none", borderRadius: 8, padding: "8px 16px", fontSize: 13, fontWeight: 600, cursor: "pointer" }}
          >
            Save
          </button>
        </div>
      </div>

      {listView ? (
        <div style={{ maxHeight: 400, overflowY: "auto" }}>
          {items.map((item, idx) => (
            <div
              key={`${idx}-${item}`}
              draggable
              onDragStart={() => handleDragStart(idx)}
              onDragOver={(e) => handleDragOver(e, idx)}
              onDragLeave={handleDragLeave}
              onDrop={(e) => handleDrop(e, idx)}
              style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "6px 8px", marginBottom: 2, borderRadius: 6,
                background: dragOver === idx ? PALETTE.accent + "22" : PALETTE.surfaceLight,
                borderTop: dragOver === idx ? `2px solid ${PALETTE.accent}` : "2px solid transparent",
                cursor: "grab", fontSize: 12, fontFamily: "'JetBrains Mono', monospace",
                color: PALETTE.text, transition: "background 0.1s",
              }}
            >
              <span style={{ color: PALETTE.textMuted, fontSize: 10, width: 24, textAlign: "right", flexShrink: 0 }}>{idx + 1}</span>
              <span style={{ color: PALETTE.border, cursor: "grab", fontSize: 14, flexShrink: 0 }}>⠿</span>
              <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {item.split("\\").pop().split("/").pop()}
              </span>
              <button
                onClick={() => removeItem(idx)}
                style={{
                  background: "transparent", border: "none", color: PALETTE.red,
                  cursor: "pointer", fontSize: 14, padding: "0 4px", flexShrink: 0,
                }}
              >×</button>
            </div>
          ))}
          {items.length === 0 && (
            <div style={{ color: PALETTE.textMuted, fontSize: 12, padding: 12 }}>No items</div>
          )}
        </div>
      ) : (
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
      )}
      <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 4 }}>
        {(listView ? items : text.split("\n").filter(l => l.trim())).length} paths
      </div>
    </div>
  );
}
