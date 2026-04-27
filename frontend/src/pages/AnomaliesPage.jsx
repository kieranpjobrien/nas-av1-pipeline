import { useEffect, useMemo, useState } from "react";
import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import { api } from "../lib/api";

// Operator-console palette — keep these in sync with dashboard.css if it ever drifts.
const C = {
  bg: "#0e0f0c",
  bgElev: "#16180f",
  bgCard: "#1c1f15",
  line: "#2a2e20",
  lineStrong: "#3a3f2d",
  ink: "#f2f1ea",
  ink2: "#c9c7b6",
  ink3: "#888871",
  ink4: "#5a5b4a",
  accent: "#d4ff4a",
  accentDim: "#8fa32e",
  good: "#6fcf97",
  warn: "#f0b429",
  bad: "#e0654b",
};

const STATUS_COLOURS = {
  flagged_corrupt: C.bad,
  flagged_undetermined: C.warn,
  flagged_foreign_audio: C.warn,
  flagged_manual: C.warn,
  done: C.good,
  pending: C.ink3,
  processing: C.accent,
  uploading: C.accentDim,
  error: C.bad,
};

function statusColour(status) {
  if (!status) return C.ink4;
  return STATUS_COLOURS[status] || C.accentDim;
}

function fmtSize(bytes) {
  if (!bytes) return "0";
  const gb = bytes / 1024 ** 3;
  if (gb >= 1) return `${gb.toFixed(2)} GB`;
  const mb = bytes / 1024 ** 2;
  return `${mb.toFixed(1)} MB`;
}

function fmtMinutes(secs) {
  if (!secs) return "—";
  if (secs >= 3600) return `${(secs / 3600).toFixed(1)}h`;
  return `${Math.round(secs / 60)}m`;
}

// AV1 baseline target: ~50 MB / minute for 1080p AV1 at our quality profile.
// "Suspicious" means density is below 1/5 of that — corrupt, sample, or
// truncated. Keeps the filter signal-rich without dragging in well-compressed
// outliers that just happen to be skinny.
const AV1_BASELINE_MB_PER_MIN = 50;
const SUSPICION_RATIO = 0.2;

function isSuspicious(point) {
  // User-managed allowlist for shows whose AV1 density legitimately falls
  // below the threshold (animated sitcoms, etc.). Edit
  // F:\AV1_Staging\control\density_whitelist.json to add/remove.
  if (point.density_whitelisted) return false;
  if (!point.duration_seconds || point.duration_seconds <= 0) return true;
  if (!point.size_bytes) return true;
  const mbPerMin = point.size_bytes / 1024 ** 2 / (point.duration_seconds / 60);
  return mbPerMin < AV1_BASELINE_MB_PER_MIN * SUSPICION_RATIO;
}

// Y axis is log — zero / sub-1-minute values can't render. Floor them to 0.5
// so they're visible at the very bottom of the chart (a "cannot determine
// duration" stripe) instead of disappearing.
const Y_FLOOR_MIN = 0.5;
function clampYMin(durationSec) {
  const min = (durationSec || 0) / 60;
  return min > Y_FLOOR_MIN ? min : Y_FLOOR_MIN;
}

// X axis floor — drop the 1 MB tick. Most real files start at ~10 MB; below
// that is metadata stubs we don't want to render anyway.
const X_FLOOR_GB = 0.005; // 5 MB

const X_TICKS = [0.01, 0.1, 1, 10, 100];
// Pick log-spaced minute values that map cleanly to "1m / 10m / 1h / 3h / 10h".
const Y_TICKS = [1, 10, 60, 180, 600];

function fmtXTick(v) {
  if (v < 1) return `${Math.round(v * 1024)} MB`;
  return `${v} GB`;
}

function fmtYTick(v) {
  if (v >= 60) {
    const h = v / 60;
    return Number.isInteger(h) ? `${h}h` : `${h.toFixed(1)}h`;
  }
  return `${v}m`;
}

export function AnomaliesPage({ onFileClick }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [selected, setSelected] = useState(null);
  const [deleting, setDeleting] = useState(false);
  const [filter, setFilter] = useState("suspect"); // "suspect" | "all" | "flagged_corrupt"
  const [typeFilter, setTypeFilter] = useState("all"); // "all" | "movie" | "series"

  const load = () => {
    setLoading(true);
    api
      .getSizeVsDuration()
      .then((d) => {
        setData(d);
        setLoading(false);
      })
      .catch((e) => {
        setErr(e.message);
        setLoading(false);
      });
  };

  useEffect(() => {
    load();
  }, []);

  const typeMatches = (p) => {
    if (typeFilter === "all") return true;
    return p.library_type === typeFilter;
  };

  const points = useMemo(() => {
    if (!data?.points) return [];
    let pts = data.points.filter(typeMatches);
    if (filter === "suspect") pts = pts.filter(isSuspicious);
    else if (filter === "flagged_corrupt") pts = pts.filter((p) => p.status === "flagged_corrupt");
    return pts.map((p) => ({
      ...p,
      sizeGB: Math.max(p.size_bytes / 1024 ** 3, X_FLOOR_GB),
      durationMin: clampYMin(p.duration_seconds),
      colour: statusColour(p.status),
      flooredY: !p.duration_seconds || p.duration_seconds / 60 < Y_FLOOR_MIN,
    }));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, filter, typeFilter]);

  const totalCount = useMemo(
    () => (data?.points || []).filter(typeMatches).length,
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [data, typeFilter]
  );
  const suspectCount = useMemo(
    () => (data?.points || []).filter(typeMatches).filter(isSuspicious).length,
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [data, typeFilter]
  );
  const corruptCount = useMemo(
    () => (data?.points || []).filter(typeMatches).filter((p) => p.status === "flagged_corrupt").length,
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [data, typeFilter]
  );

  const handleDelete = async () => {
    if (!selected) return;
    if (!window.confirm(`Delete this file from the NAS?\n\n${selected.filepath}\n\nThis cannot be undone.`)) return;
    setDeleting(true);
    try {
      await api.deleteFile(selected.filepath);
      setSelected(null);
      load();
    } catch (e) {
      alert(`Delete failed: ${e.message}`);
    }
    setDeleting(false);
  };

  if (loading) return <div style={{ color: C.ink3, padding: 40 }}>Loading anomaly chart…</div>;
  if (err) return <div style={{ color: C.bad, padding: 40 }}>Failed to load: {err}</div>;

  const filterButton = (id, label, count) => (
    <button
      key={id}
      onClick={() => setFilter(id)}
      style={{
        background: filter === id ? C.accent : "transparent",
        color: filter === id ? C.bg : C.ink2,
        border: `1px solid ${filter === id ? C.accent : C.line}`,
        borderRadius: 6,
        padding: "6px 14px",
        fontSize: 13,
        cursor: "pointer",
        fontWeight: filter === id ? 600 : 400,
        fontFamily: "inherit",
      }}
    >
      {label} <span style={{ opacity: 0.7 }}>({count})</span>
    </button>
  );

  return (
    <div>
      <div
        style={{
          fontSize: 22,
          fontWeight: 600,
          color: C.ink,
          marginBottom: 6,
          letterSpacing: "-0.01em",
        }}
      >
        Size vs duration — corrupt-file detector
      </div>
      <p style={{ color: C.ink3, fontSize: 13, marginTop: 0, marginBottom: 16, maxWidth: 760 }}>
        X = file size (GB, log). Y = duration in minutes (log; TMDb runtime where known, file metadata otherwise).
        Files in the bottom-left corner — small for their duration — are samples, truncated downloads, or audio-loss
        casualties from the 2026-04-23 incident. Click a point to inspect, then delete.
      </p>

      <div style={{ display: "flex", gap: 8, marginBottom: 8, flexWrap: "wrap" }}>
        {filterButton("suspect", "Suspicious", suspectCount)}
        {filterButton("flagged_corrupt", "FLAGGED_CORRUPT", corruptCount)}
        {filterButton("all", "All files", totalCount)}
      </div>
      <div style={{ display: "flex", gap: 8, marginBottom: 12, alignItems: "center" }}>
        <span style={{ color: C.ink3, fontSize: 12, marginRight: 4 }}>type:</span>
        {[
          { id: "all", label: "All" },
          { id: "movie", label: "Movies" },
          { id: "series", label: "Series" },
        ].map(({ id, label }) => (
          <button
            key={id}
            onClick={() => setTypeFilter(id)}
            style={{
              background: typeFilter === id ? C.bgElev : "transparent",
              color: typeFilter === id ? C.ink : C.ink3,
              border: `1px solid ${typeFilter === id ? C.lineStrong : C.line}`,
              borderRadius: 6,
              padding: "4px 12px",
              fontSize: 12,
              cursor: "pointer",
              fontFamily: "inherit",
            }}
          >
            {label}
          </button>
        ))}
      </div>

      <div
        style={{
          background: C.bgCard,
          border: `1px solid ${C.line}`,
          borderRadius: 8,
          padding: 16,
          height: 540,
        }}
      >
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 16, right: 32, bottom: 32, left: 32 }}>
            <CartesianGrid stroke={C.line} strokeDasharray="3 3" />
            <XAxis
              type="number"
              dataKey="sizeGB"
              name="Size"
              scale="log"
              domain={[X_FLOOR_GB, 100]}
              ticks={X_TICKS}
              tickFormatter={fmtXTick}
              tick={{ fill: C.ink3, fontSize: 11 }}
              stroke={C.line}
              label={{
                value: "File size (log)",
                position: "insideBottom",
                offset: -16,
                fill: C.ink3,
                fontSize: 12,
              }}
            />
            <YAxis
              type="number"
              dataKey="durationMin"
              name="Duration"
              scale="log"
              domain={[Y_FLOOR_MIN, 1500]}
              ticks={Y_TICKS}
              tickFormatter={fmtYTick}
              tick={{ fill: C.ink3, fontSize: 11 }}
              stroke={C.line}
              label={{
                value: "Duration (log)",
                angle: -90,
                position: "insideLeft",
                fill: C.ink3,
                fontSize: 12,
              }}
            />
            <ZAxis range={[60, 60]} />
            <Tooltip
              cursor={{ strokeDasharray: "3 3", stroke: C.lineStrong }}
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const p = payload[0].payload;
                return (
                  <div
                    style={{
                      background: C.bg,
                      border: `1px solid ${C.line}`,
                      borderRadius: 6,
                      padding: 10,
                      fontSize: 12,
                      maxWidth: 380,
                      color: C.ink,
                    }}
                  >
                    <div style={{ fontWeight: 600, marginBottom: 4 }}>{p.filename}</div>
                    <div style={{ color: C.ink3 }}>
                      {fmtSize(p.size_bytes)} · {fmtMinutes(p.duration_seconds)} ({p.duration_source})
                    </div>
                    <div style={{ color: p.colour, fontSize: 11, marginTop: 4 }}>
                      status: {p.status || "—"} · {p.is_av1 ? "AV1" : "needs encode"}
                      {p.flooredY ? " · clamped to Y floor (no real duration)" : ""}
                    </div>
                    <div style={{ color: C.ink3, fontSize: 11, marginTop: 4 }}>
                      Click point to select for deletion
                    </div>
                  </div>
                );
              }}
            />
            <Scatter
              data={points}
              fill={C.accent}
              onClick={(p) => setSelected(p)}
              shape={(props) => {
                const { cx, cy, payload } = props;
                if (cx == null || cy == null) return null;
                const isSelected = selected && selected.filepath === payload.filepath;
                return (
                  <circle
                    cx={cx}
                    cy={cy}
                    r={isSelected ? 7 : 4}
                    fill={payload.colour}
                    stroke={isSelected ? C.ink : "none"}
                    strokeWidth={isSelected ? 2 : 0}
                    fillOpacity={0.78}
                    style={{ cursor: "pointer" }}
                  />
                );
              }}
            />
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      {/* Selected point detail + delete */}
      {selected && (
        <div
          style={{
            marginTop: 16,
            padding: 16,
            background: C.bgCard,
            border: `1px solid ${C.line}`,
            borderRadius: 8,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontWeight: 600, color: C.ink, marginBottom: 4 }}>{selected.filename}</div>
              <div
                style={{
                  color: C.ink3,
                  fontSize: 12,
                  fontFamily: "'JetBrains Mono', monospace",
                  overflowWrap: "anywhere",
                }}
              >
                {selected.filepath}
              </div>
              <div style={{ color: C.ink2, fontSize: 13, marginTop: 8 }}>
                size: <strong style={{ color: C.ink }}>{fmtSize(selected.size_bytes)}</strong>
                {" · "}
                duration: <strong style={{ color: C.ink }}>{fmtMinutes(selected.duration_seconds)}</strong>{" "}
                ({selected.duration_source})
                {" · "}
                status: <strong style={{ color: statusColour(selected.status) }}>{selected.status || "untracked"}</strong>
              </div>
              {selected.duration_seconds > 0 && selected.size_bytes > 0 && (
                <div style={{ color: C.ink3, fontSize: 12, marginTop: 4 }}>
                  density: {(selected.size_bytes / 1024 ** 2 / (selected.duration_seconds / 60)).toFixed(1)} MB/min
                  {" "}(target ~{AV1_BASELINE_MB_PER_MIN} MB/min for AV1 1080p)
                </div>
              )}
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              <button
                onClick={() => onFileClick?.(selected.filepath)}
                disabled={!onFileClick}
                style={{
                  background: C.bgElev,
                  color: C.ink,
                  border: `1px solid ${C.line}`,
                  borderRadius: 6,
                  padding: "8px 16px",
                  fontSize: 13,
                  cursor: onFileClick ? "pointer" : "not-allowed",
                  opacity: onFileClick ? 1 : 0.5,
                  fontFamily: "inherit",
                }}
              >
                Inspect
              </button>
              <button
                onClick={handleDelete}
                disabled={deleting}
                style={{
                  background: C.bad,
                  color: C.ink,
                  border: "none",
                  borderRadius: 6,
                  padding: "8px 16px",
                  fontSize: 13,
                  fontWeight: 600,
                  cursor: deleting ? "wait" : "pointer",
                  opacity: deleting ? 0.6 : 1,
                  fontFamily: "inherit",
                }}
              >
                {deleting ? "Deleting…" : "Delete file"}
              </button>
              <button
                onClick={() => setSelected(null)}
                style={{
                  background: "transparent",
                  color: C.ink3,
                  border: "none",
                  fontSize: 12,
                  cursor: "pointer",
                  fontFamily: "inherit",
                }}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
