import { useState, useEffect, useMemo } from "react";
import { PALETTE, getCodecColour, getResColour } from "../theme";
import { api } from "../lib/api";

function formatSize(bytes) {
  if (!bytes) return "?";
  const gb = bytes / (1024 ** 3);
  return gb >= 1 ? `${gb.toFixed(1)} GB` : `${(gb * 1024).toFixed(0)} MB`;
}

export function MediaSearch({ onAdd }) {
  const [files, setFiles] = useState([]);
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState(new Set());
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getMediaReport()
      .then((report) => setFiles(report.files || []))
      .catch(() => setFiles([]))
      .finally(() => setLoading(false));
  }, []);

  // All matches (unlimited) — used for "select all" and counts
  const allMatches = useMemo(() => {
    if (!query.trim()) return [];
    const q = query.toLowerCase();
    return files.filter((f) =>
      f.filename.toLowerCase().includes(q) || f.filepath.toLowerCase().includes(q)
    );
  }, [query, files]);

  // Display subset (capped for performance)
  const DISPLAY_LIMIT = 50;
  const results = useMemo(() => allMatches.slice(0, DISPLAY_LIMIT), [allMatches]);

  const toggle = (filepath) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(filepath)) next.delete(filepath);
      else next.add(filepath);
      return next;
    });
  };

  const selectAll = () => {
    // Select/deselect ALL matches, not just the displayed subset
    if (allMatches.every((r) => selected.has(r.filepath))) {
      setSelected((prev) => {
        const next = new Set(prev);
        allMatches.forEach((r) => next.delete(r.filepath));
        return next;
      });
    } else {
      setSelected((prev) => {
        const next = new Set(prev);
        allMatches.forEach((r) => next.add(r.filepath));
        return next;
      });
    }
  };

  const handleAdd = (list) => {
    const paths = [...selected];
    if (paths.length === 0) return;
    onAdd(list, paths);
    setSelected(new Set());
  };

  const selectedCount = selected.size;

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
      <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 12 }}>
        Search Media Library
      </div>

      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={loading ? "Loading media report..." : `Search ${files.length} files...`}
        disabled={loading}
        style={{
          width: "100%",
          background: PALETTE.surfaceLight,
          border: `1px solid ${PALETTE.border}`,
          borderRadius: 8,
          color: PALETTE.text,
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 13,
          padding: "10px 12px",
          boxSizing: "border-box",
          marginBottom: 8,
        }}
      />

      {results.length > 0 && (
        <>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
            <button onClick={selectAll} style={{
              background: "transparent", border: "none", color: PALETTE.accent,
              fontSize: 12, cursor: "pointer", padding: "2px 0",
            }}>
              {allMatches.every((r) => selected.has(r.filepath)) ? "Deselect all" : `Select all ${allMatches.length}`}
            </button>
            <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>
              {allMatches.length} result{allMatches.length !== 1 ? "s" : ""}
              {allMatches.length > DISPLAY_LIMIT && ` (showing ${DISPLAY_LIMIT})`}
            </span>
          </div>

          <div style={{
            maxHeight: 280, overflowY: "auto",
            border: `1px solid ${PALETTE.border}`, borderRadius: 8,
            marginBottom: 12,
          }}>
            {results.map((f) => (
              <label
                key={f.filepath}
                style={{
                  display: "flex", alignItems: "center", gap: 10,
                  padding: "8px 12px",
                  borderBottom: `1px solid ${PALETTE.border}`,
                  cursor: "pointer",
                  background: selected.has(f.filepath) ? PALETTE.surfaceLight : "transparent",
                }}
              >
                <input
                  type="checkbox"
                  checked={selected.has(f.filepath)}
                  onChange={() => toggle(f.filepath)}
                  style={{ flexShrink: 0 }}
                />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{
                    color: PALETTE.text, fontSize: 12,
                    whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
                  }}>
                    {f.filename}
                  </div>
                  <div style={{ display: "flex", gap: 8, marginTop: 2 }}>
                    {f.video && (
                      <>
                        <span style={{ color: getCodecColour(f.video.codec), fontSize: 10, fontWeight: 600 }}>
                          {f.video.codec}
                        </span>
                        <span style={{ color: getResColour(f.video.resolution_class), fontSize: 10, fontWeight: 600 }}>
                          {f.video.resolution_class}
                        </span>
                      </>
                    )}
                    <span style={{ color: PALETTE.textMuted, fontSize: 10 }}>
                      {formatSize(f.file_size_bytes)}
                    </span>
                  </div>
                </div>
              </label>
            ))}
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <button
              onClick={() => handleAdd("skip")}
              disabled={selectedCount === 0}
              style={{
                flex: 1, padding: "8px 0", borderRadius: 8, border: "none",
                fontSize: 13, fontWeight: 600, cursor: selectedCount ? "pointer" : "default",
                background: selectedCount ? PALETTE.accentWarm : PALETTE.surfaceLight,
                color: selectedCount ? "#000" : PALETTE.textMuted,
              }}
            >
              Skip ({selectedCount})
            </button>
            <button
              onClick={() => handleAdd("priority")}
              disabled={selectedCount === 0}
              style={{
                flex: 1, padding: "8px 0", borderRadius: 8, border: "none",
                fontSize: 13, fontWeight: 600, cursor: selectedCount ? "pointer" : "default",
                background: selectedCount ? PALETTE.accent : PALETTE.surfaceLight,
                color: selectedCount ? "#fff" : PALETTE.textMuted,
              }}
            >
              Priority ({selectedCount})
            </button>
            <button
              onClick={() => handleAdd("gentle")}
              disabled={selectedCount === 0}
              style={{
                flex: 1, padding: "8px 0", borderRadius: 8, border: "none",
                fontSize: 13, fontWeight: 600, cursor: selectedCount ? "pointer" : "default",
                background: selectedCount ? PALETTE.purple : PALETTE.surfaceLight,
                color: selectedCount ? "#fff" : PALETTE.textMuted,
              }}
            >
              Gentle +2 CQ ({selectedCount})
            </button>
          </div>
        </>
      )}

      {query.trim() && results.length === 0 && !loading && (
        <div style={{ color: PALETTE.textMuted, fontSize: 12, padding: "8px 0" }}>
          No matching files found.
        </div>
      )}
    </div>
  );
}
