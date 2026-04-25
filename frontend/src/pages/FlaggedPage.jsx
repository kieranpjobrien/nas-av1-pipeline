import { useCallback, useEffect, useState } from "react";
import { PALETTE } from "../theme";
import { api } from "../lib/api";

/**
 * FlaggedPage — files the qualifier flagged for human review.
 *
 * Three flag classes:
 *   * FLAGGED_FOREIGN_AUDIO  — audio language ≠ TMDb original_language
 *                              (Bluey Swedish dub, Amelie English-dub-only,
 *                              Spirited Away English-dub-only)
 *   * FLAGGED_UNDETERMINED   — audio is `und` and whisper exhausted
 *   * FLAGGED_MANUAL         — catch-all for ambiguous cases
 *
 * Three actions per row:
 *   * Delete + Redownload — delete file, ask Radarr/Sonarr to grab again
 *                            via Quality+ profile
 *   * Encode anyway       — override the flag, queue for normal encode
 *   * Dismiss             — accept as-is, mark DONE without further work
 */
export function FlaggedPage({ onFileClick }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [busyPath, setBusyPath] = useState(null);
  const [statusFilter, setStatusFilter] = useState("all");

  const load = useCallback(async () => {
    try {
      setLoading(true);
      setErr(null);
      const r = await api.getFlaggedFiles();
      setItems(r.items || []);
    } catch (e) {
      setErr(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const performAction = async (filepath, action, label) => {
    if (busyPath) return;
    if (action === "delete_redownload") {
      const ok = window.confirm(
        `Delete this file from the NAS and trigger Radarr/Sonarr to find a replacement?\n\n${filepath}`,
      );
      if (!ok) return;
    }
    try {
      setBusyPath(filepath);
      const result = await api.flaggedAction(filepath, action);
      // Remove the row optimistically — it's no longer flagged after any of these actions.
      setItems((prev) => prev.filter((i) => i.filepath !== filepath));
      // Tiny inline confirmation
      window.notify?.({
        kind: "good",
        title: label,
        body: action === "delete_redownload" && result?.arr?.queued
          ? `Deleted; Radarr/Sonarr search queued (${result.arr.profile_name || "default"})`
          : `${label} applied`,
      });
    } catch (e) {
      window.notify?.({
        kind: "bad",
        title: `${label} failed`,
        body: e.detail || e.message || String(e),
      });
    } finally {
      setBusyPath(null);
    }
  };

  const filtered = statusFilter === "all" ? items : items.filter((i) => i.status === statusFilter);

  const counts = {
    all: items.length,
    flagged_foreign_audio: items.filter((i) => i.status === "flagged_foreign_audio").length,
    flagged_undetermined: items.filter((i) => i.status === "flagged_undetermined").length,
    flagged_manual: items.filter((i) => i.status === "flagged_manual").length,
  };

  return (
    <div style={{ color: PALETTE.text }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
        <h2 style={{ margin: 0, fontSize: 22, fontWeight: 600 }}>Flagged files</h2>
        <span style={{ color: PALETTE.textMuted, fontSize: 13 }}>
          requires action — language doesn&apos;t match original / undetermined audio
        </span>

        <div
          style={{
            display: "flex",
            background: PALETTE.surfaceLight,
            border: `1px solid ${PALETTE.border}`,
            borderRadius: 8,
            padding: 2,
          }}
        >
          {[
            { k: "all", l: "All" },
            { k: "flagged_foreign_audio", l: "Foreign audio" },
            { k: "flagged_undetermined", l: "Undetermined" },
            { k: "flagged_manual", l: "Manual" },
          ].map(({ k, l }) => (
            <button
              key={k}
              onClick={() => setStatusFilter(k)}
              style={{
                background: statusFilter === k ? PALETTE.accent : "transparent",
                color: statusFilter === k ? "#fff" : PALETTE.textMuted,
                border: "none",
                borderRadius: 6,
                padding: "5px 12px",
                fontSize: 12,
                fontWeight: 500,
                cursor: "pointer",
              }}
            >
              {l} <span style={{ opacity: 0.7 }}>({counts[k] || 0})</span>
            </button>
          ))}
        </div>

        <div style={{ flex: 1 }} />
        <button onClick={load} style={ghostBtn} disabled={loading}>
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>

      {err && (
        <div
          style={{
            background: "#3a1218",
            border: `1px solid ${PALETTE.red}`,
            color: PALETTE.text,
            padding: "10px 14px",
            borderRadius: 8,
            marginBottom: 16,
            fontSize: 13,
          }}
        >
          {err}
        </div>
      )}

      <div
        style={{
          background: PALETTE.surface,
          border: `1px solid ${PALETTE.border}`,
          borderRadius: 12,
          overflow: "hidden",
        }}
      >
        {loading && (
          <div style={{ padding: 40, textAlign: "center", color: PALETTE.textMuted }}>Loading…</div>
        )}
        {!loading && filtered.length === 0 && (
          <div style={{ padding: 40, textAlign: "center", color: PALETTE.textMuted, fontSize: 14 }}>
            {items.length === 0 ? (
              <>
                No flagged files. Either everything is in order, or the audit hasn&apos;t run yet.
                <pre style={preStyle}>uv run python -m tools.qualify_audit</pre>
              </>
            ) : (
              <>No items match the &ldquo;{statusFilter}&rdquo; filter.</>
            )}
          </div>
        )}
        {!loading && filtered.length > 0 && (
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ background: PALETTE.surfaceLight, textAlign: "left" }}>
                <Th>Title</Th>
                <Th width="60px">Year</Th>
                <Th width="60px">Type</Th>
                <Th>Issue</Th>
                <Th width="90px">Detected</Th>
                <Th width="80px">Original</Th>
                <Th width="280px">Actions</Th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((item) => (
                <Row
                  key={item.filepath}
                  item={item}
                  onFileClick={onFileClick}
                  onAction={performAction}
                  busy={busyPath === item.filepath}
                />
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div style={{ marginTop: 12, color: PALETTE.textMuted, fontSize: 12 }}>
        {items.length} flagged total · status filter shows {filtered.length}
      </div>
    </div>
  );
}

function Row({ item, onFileClick, onAction, busy }) {
  const detectedLabel = item.detected_language
    ? `${item.detected_language}${item.detection_confidence ? ` (${(item.detection_confidence * 100).toFixed(0)}%)` : ""}`
    : item.audio_language_tag || "—";
  const titleDisplay = item.title || item.filename;
  const issueLabel =
    item.status === "flagged_foreign_audio"
      ? "Foreign audio"
      : item.status === "flagged_undetermined"
        ? "Undetermined"
        : "Manual review";

  return (
    <tr
      style={{
        borderTop: `1px solid ${PALETTE.border}`,
        cursor: item.filepath ? "pointer" : "default",
      }}
      onClick={() => item.filepath && onFileClick && onFileClick(item.filepath)}
    >
      <Td style={{ fontWeight: 500 }}>
        <div>{titleDisplay}</div>
        {item.title && (
          <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 2 }}>
            {item.filename}
          </div>
        )}
      </Td>
      <Td style={{ color: PALETTE.textMuted }}>{item.year ?? "—"}</Td>
      <Td style={{ color: PALETTE.textMuted, textTransform: "capitalize" }}>
        {item.library_type || "—"}
      </Td>
      <Td style={{ color: PALETTE.textMuted, fontSize: 12, maxWidth: 320 }}>
        <div style={{ color: issueColour(item.status), fontWeight: 500, marginBottom: 2 }}>
          {issueLabel}
        </div>
        <div title={item.reason}>{truncate(item.reason, 100)}</div>
      </Td>
      <Td style={{ color: PALETTE.textMuted, whiteSpace: "nowrap" }}>{detectedLabel}</Td>
      <Td style={{ color: PALETTE.textMuted, whiteSpace: "nowrap" }}>
        {item.original_language || "—"}
      </Td>
      <Td onClick={(e) => e.stopPropagation()}>
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          <button
            onClick={() => onAction(item.filepath, "delete_redownload", "Delete + redownload")}
            disabled={busy}
            style={{ ...ghostBtn, color: PALETTE.red, borderColor: PALETTE.red, fontSize: 11, padding: "4px 8px" }}
            title="Delete from NAS + tell Radarr/Sonarr to find a better source"
          >
            {busy ? "…" : "Delete + Redownload"}
          </button>
          <button
            onClick={() => onAction(item.filepath, "encode_anyway", "Encode anyway")}
            disabled={busy}
            style={{ ...ghostBtn, color: PALETTE.accent, borderColor: PALETTE.accent, fontSize: 11, padding: "4px 8px" }}
            title="Override the flag — queue for normal encode"
          >
            Encode anyway
          </button>
          <button
            onClick={() => onAction(item.filepath, "dismiss", "Dismissed")}
            disabled={busy}
            style={{ ...ghostBtn, fontSize: 11, padding: "4px 8px" }}
            title="Accept as-is, mark DONE without further work"
          >
            Dismiss
          </button>
        </div>
      </Td>
    </tr>
  );
}

function issueColour(status) {
  if (status === "flagged_foreign_audio") return PALETTE.accentWarm;
  if (status === "flagged_undetermined") return PALETTE.purple;
  return PALETTE.textMuted;
}

function Th({ children, width }) {
  return (
    <th
      style={{
        padding: "10px 12px",
        color: PALETTE.textMuted,
        fontSize: 11,
        fontWeight: 500,
        textTransform: "uppercase",
        letterSpacing: 0.5,
        width,
        textAlign: "left",
      }}
    >
      {children}
    </th>
  );
}

function Td({ children, style, onClick }) {
  return (
    <td
      onClick={onClick}
      style={{
        padding: "10px 12px",
        verticalAlign: "top",
        ...(style || {}),
      }}
    >
      {children}
    </td>
  );
}

function truncate(s, n) {
  if (!s) return "";
  if (s.length <= n) return s;
  return s.slice(0, n - 1).trimEnd() + "…";
}

const ghostBtn = {
  background: "transparent",
  color: PALETTE.text,
  border: `1px solid ${PALETTE.border}`,
  borderRadius: 6,
  padding: "5px 10px",
  fontSize: 12,
  cursor: "pointer",
};

const preStyle = {
  marginTop: 12,
  padding: "10px 14px",
  background: PALETTE.bg,
  border: `1px solid ${PALETTE.border}`,
  borderRadius: 8,
  fontFamily: "JetBrains Mono, monospace",
  fontSize: 12,
  textAlign: "left",
  color: PALETTE.text,
  display: "inline-block",
};
