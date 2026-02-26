import { useState, useEffect } from "react";
import { PALETTE } from "../theme";
import { usePolling } from "../lib/usePolling";
import { api } from "../lib/api";
import { StatCard } from "../components/StatCard";
import { SectionTitle } from "../components/SectionTitle";

function fmt(bytes) {
  if (bytes >= 1024 ** 4) return `${(bytes / 1024 ** 4).toFixed(2)} TB`;
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(0)} MB`;
  return `${(bytes / 1024).toFixed(0)} KB`;
}

function formatETA(secs) {
  if (secs == null || secs < 0) return null;
  secs = Math.round(secs);
  if (secs < 60) return `~${secs}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `~${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return rm > 0 ? `~${h}h ${rm}m` : `~${h}h`;
}

function computeOverallETA(data) {
  const stats = data.stats || {};
  const files = data.files || {};
  const completed = stats.completed || 0;
  const totalEncodeTime = stats.total_encode_time_secs || 0;
  const tierStats = stats.tier_stats || {};

  const overallAvg = completed > 0 && totalEncodeTime > 0 ? totalEncodeTime / completed : 0;
  if (overallAvg <= 0) return null;

  const doneStatuses = ["completed", "replaced", "done", "skipped", "error", "failed", "verified"];
  let totalSecs = 0;
  let remaining = 0;
  for (const info of Object.values(files)) {
    if (doneStatuses.includes((info.status || "").toLowerCase())) continue;
    remaining++;
    const resKey = info.res_key || "";
    const tier = tierStats[resKey];
    if (tier && tier.completed >= 2 && tier.total_encode_time_secs > 0) {
      totalSecs += tier.total_encode_time_secs / tier.completed;
    } else {
      totalSecs += overallAvg;
    }
  }
  return remaining > 0 ? totalSecs : null;
}

function getTierSavings(stats) {
  const tierStats = stats.tier_stats || {};
  return Object.entries(tierStats)
    .map(([key, t]) => ({
      tier: key,
      completed: t.completed || 0,
      bytes_saved: t.bytes_saved || 0,
      total_input: t.total_input_bytes || 0,
      total_output: t.total_output_bytes || 0,
      encode_time: t.total_encode_time_secs || 0,
    }))
    .filter((t) => t.completed > 0)
    .sort((a, b) => b.bytes_saved - a.bytes_saved);
}

const STATUS_GROUPS = {
  Queued: ["queued", "pending", "waiting", "fetched", "encoded", "uploaded"],
  "In Progress": ["fetching", "encoding", "uploading", "verifying", "replacing"],
  Done: ["completed", "replaced", "done", "verified"],
  Skipped: ["skipped"],
  Error: ["error", "failed"],
};

function groupStatuses(files) {
  const groups = { Queued: 0, "In Progress": 0, Done: 0, Skipped: 0, Error: 0 };
  for (const info of Object.values(files)) {
    const s = (info.status || "unknown").toLowerCase();
    let found = false;
    for (const [group, statuses] of Object.entries(STATUS_GROUPS)) {
      if (statuses.includes(s)) { groups[group]++; found = true; break; }
    }
    if (!found) groups.Queued++;
  }
  return groups;
}

function getTierProgress(files) {
  const tiers = {};
  for (const info of Object.values(files)) {
    const tier = info.tier || "Unknown";
    if (!tiers[tier]) tiers[tier] = { total: 0, done: 0 };
    tiers[tier].total++;
    const s = (info.status || "").toLowerCase();
    if (["completed", "replaced", "done", "verified"].includes(s)) tiers[tier].done++;
  }
  return Object.entries(tiers).sort((a, b) => b[1].total - a[1].total);
}

function getErrors(files) {
  return Object.entries(files)
    .filter(([, info]) => ["error", "failed"].includes((info.status || "").toLowerCase()))
    .map(([path, info]) => ({ path, error: info.error || info.status }));
}

function getActiveFiles(data) {
  const files = data.files || {};
  const active = [];
  for (const [path, info] of Object.entries(files)) {
    const s = (info.status || "").toLowerCase();
    if (["fetching", "encoding", "uploading", "verifying", "replacing"].includes(s)) {
      const elapsed = info.last_updated
        ? Math.max(0, (Date.now() - new Date(info.last_updated).getTime()) / 1000)
        : null;
      active.push({ path, status: info.status, elapsed, last_updated: info.last_updated });
    }
  }
  // Most recently updated first
  active.sort((a, b) => (b.last_updated || "").localeCompare(a.last_updated || ""));
  return active;
}

function getUpNext(data, priorityPaths, limit = 15) {
  const files = data.files || {};
  const doneStatuses = ["completed", "replaced", "done", "verified", "skipped", "error", "failed"];
  const activeStatuses = ["fetching", "encoding", "uploading", "verifying", "replacing"];
  const seen = new Set();
  const upcoming = [];

  // Priority items first — show them unless they're done or actively processing
  for (const path of priorityPaths) {
    const info = files[path];
    const s = info ? (info.status || "").toLowerCase() : "";
    if (doneStatuses.includes(s) || activeStatuses.includes(s)) continue;
    const filename = path.split(/[\\/]/).pop();
    const status = info?.status || "priority";
    upcoming.push({ path, filename, status, priority: true });
    seen.add(path);
  }

  // Then pipeline state items (fetched/pending/encoded/uploaded)
  for (const [path, info] of Object.entries(files)) {
    if (seen.has(path)) continue;
    const s = (info.status || "").toLowerCase();
    if (["fetched", "pending", "encoded", "uploaded"].includes(s)) {
      const filename = path.split(/[\\/]/).pop();
      upcoming.push({ path, filename, status: info.status, priority: false, added: info.added || info.last_updated });
    }
  }

  // Sort: priority first, then by readiness (fetched > encoded > uploaded > pending)
  const statusOrder = { fetched: 0, encoded: 1, uploaded: 2, pending: 3, priority: 4 };
  upcoming.sort((a, b) => {
    if (a.priority !== b.priority) return a.priority ? -1 : 1;
    const oa = statusOrder[(a.status || "").toLowerCase()] ?? 9;
    const ob = statusOrder[(b.status || "").toLowerCase()] ?? 9;
    if (oa !== ob) return oa - ob;
    return (a.added || "").localeCompare(b.added || "");
  });
  return upcoming.slice(0, limit);
}

export function PipelinePage() {
  const { data, error } = usePolling(api.getPipeline, 3000);
  const [starting, setStarting] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [priorityPaths, setPriorityPaths] = useState([]);

  useEffect(() => {
    const load = () => api.getPriority().then((p) => setPriorityPaths(p?.paths || [])).catch(() => {});
    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, []);

  const handleResetErrors = async () => {
    setResetting(true);
    try {
      await api.resetErrors();
    } catch { /* ignore */ }
    setResetting(false);
  };

  const handleStart = async () => {
    setStarting(true);
    try {
      await api.startProcess("pipeline");
    } catch { /* ignore */ }
    setStarting(false);
  };

  if (error) {
    return <div style={{ color: PALETTE.red, padding: 40 }}>Error loading pipeline state: {error}</div>;
  }
  if (!data || data.status === "no_state") {
    return (
      <div style={{ padding: 40, textAlign: "center" }}>
        <div style={{ fontSize: 48, marginBottom: 16, opacity: 0.5 }}>...</div>
        <div style={{ color: PALETTE.textMuted, fontSize: 16 }}>Pipeline hasn't run yet</div>
        <button
          onClick={handleStart}
          disabled={starting}
          style={{
            marginTop: 16, background: starting ? PALETTE.surfaceLight : PALETTE.green,
            color: starting ? PALETTE.textMuted : "#000",
            border: "none", borderRadius: 8, padding: "12px 24px",
            fontSize: 15, fontWeight: 700,
            cursor: starting ? "default" : "pointer",
          }}
        >
          {starting ? "Starting..." : "Start Pipeline"}
        </button>
      </div>
    );
  }

  const stats = data.stats || {};
  const files = data.files || {};
  const total = Object.keys(files).length;
  const completed = stats.completed || 0;
  const pct = total > 0 ? ((completed / total) * 100) : 0;
  const groups = groupStatuses(files);
  const tierProgress = getTierProgress(files);
  const tierSavings = getTierSavings(stats);
  const errors = getErrors(files);
  const activeFiles = getActiveFiles(data);
  const upNext = getUpNext(data, priorityPaths);
  const overallETA = computeOverallETA(data);

  const GROUP_COLOURS = { Queued: PALETTE.textMuted, "In Progress": PALETTE.accent, Done: PALETTE.green, Skipped: PALETTE.textMuted, Error: PALETTE.red };

  return (
    <div>
      {/* Hero */}
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 16, padding: "32px 40px", marginBottom: 24, textAlign: "center" }}>
        <div style={{ fontSize: 56, fontWeight: 800, fontFamily: "'JetBrains Mono', monospace", color: PALETTE.green, lineHeight: 1 }}>
          {pct.toFixed(1)}%
        </div>
        <div style={{ margin: "16px auto", maxWidth: 400, height: 8, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
          <div style={{ height: "100%", width: `${pct}%`, background: PALETTE.green, borderRadius: 4, transition: "width 0.5s ease" }} />
        </div>
        <div style={{ color: PALETTE.textMuted, fontSize: 13 }}>
          {completed} / {total} files · {fmt(stats.bytes_saved || 0)} saved
        </div>
        {completed > 0 && overallETA != null ? (
          <div style={{ color: PALETTE.accent, fontSize: 13, marginTop: 4, fontFamily: "'JetBrains Mono', monospace" }}>
            ETA: {formatETA(overallETA)} remaining
          </div>
        ) : completed === 0 && total > 0 ? (
          <div style={{ color: PALETTE.textMuted, fontSize: 12, marginTop: 4, fontStyle: "italic" }}>
            Calculating ETA...
          </div>
        ) : null}
        {data.last_updated && (
          <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 4 }}>
            Updated: {new Date(data.last_updated).toLocaleString()}
          </div>
        )}
      </div>

      {/* Current activity */}
      {activeFiles.length > 0 && (
        <>
          <SectionTitle>Current Activity ({activeFiles.length})</SectionTitle>
          <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 24 }}>
            {activeFiles.map(({ path, status, elapsed }, i) => {
              const isStale = elapsed != null && elapsed > 3600;
              return (
                <div key={i} style={{
                  background: PALETTE.surface,
                  border: `1px solid ${isStale ? PALETTE.red + "44" : PALETTE.border}`,
                  borderRadius: 10, padding: "12px 16px",
                  display: "flex", alignItems: "center", gap: 12,
                }}>
                  <div style={{
                    width: 8, height: 8, borderRadius: "50%", flexShrink: 0,
                    background: isStale ? PALETTE.red : PALETTE.accent,
                    animation: isStale ? "none" : "pulse 1.5s infinite",
                  }} />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ color: PALETTE.textMuted, fontSize: 12, wordBreak: "break-all" }}>{path}</div>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 12, flexShrink: 0 }}>
                    <span style={{
                      fontSize: 11, fontWeight: 600, padding: "2px 8px", borderRadius: 4,
                      background: PALETTE.surfaceLight, color: PALETTE.accent,
                    }}>{status}</span>
                    {elapsed != null && (
                      <span style={{
                        fontSize: 11, fontFamily: "'JetBrains Mono', monospace",
                        color: isStale ? PALETTE.red : PALETTE.textMuted,
                      }}>{formatETA(elapsed)}</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* Up Next */}
      {upNext.length > 0 && (
        <>
          <SectionTitle>Up Next</SectionTitle>
          <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 24 }}>
            {upNext.map(({ path, filename, status, priority: isPrio }, i) => {
              const sl = (status || "").toLowerCase();
              const badgeColor = isPrio ? PALETTE.accentWarm : sl === "fetched" ? PALETTE.green : PALETTE.textMuted;
              const badgeBg = isPrio ? PALETTE.accentWarm + "22" : sl === "fetched" ? PALETTE.green + "22" : PALETTE.surfaceLight;
              return (
                <div key={i} style={{
                  background: PALETTE.surface,
                  border: `1px solid ${isPrio ? PALETTE.accentWarm + "44" : PALETTE.border}`,
                  borderRadius: 8, padding: "8px 14px",
                  display: "flex", alignItems: "center", gap: 10,
                }}>
                  <span style={{ color: PALETTE.textMuted, fontSize: 11, fontFamily: "'JetBrains Mono', monospace", width: 20, textAlign: "right", flexShrink: 0 }}>{i + 1}</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ color: PALETTE.text, fontSize: 12, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{filename}</div>
                    <div style={{ color: PALETTE.textMuted, fontSize: 10, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{path}</div>
                  </div>
                  {isPrio && <span style={{ fontSize: 9, fontWeight: 700, color: PALETTE.accentWarm, letterSpacing: 0.5 }}>PRIORITY</span>}
                  <span style={{
                    fontSize: 10, fontWeight: 600, padding: "2px 8px", borderRadius: 4, flexShrink: 0,
                    background: badgeBg, color: badgeColor,
                  }}>{status}</span>
                </div>
              );
            })}
            {groups.Queued > upNext.length && (
              <div style={{ color: PALETTE.textMuted, fontSize: 11, padding: "4px 0", textAlign: "center" }}>
                +{groups.Queued - upNext.length} more queued
              </div>
            )}
          </div>
        </>
      )}

      {/* Status groups */}
      <SectionTitle>Status Breakdown</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 24 }}>
        {Object.entries(groups).map(([name, count]) => (
          <StatCard key={name} label={name} value={count} colour={GROUP_COLOURS[name]} />
        ))}
      </div>

      {/* Tier progress */}
      {tierProgress.length > 0 && (
        <>
          <SectionTitle>Tier Progress</SectionTitle>
          <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 24 }}>
            {tierProgress.map(([tier, { total: t, done }]) => (
              <div key={tier} style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 8, padding: "12px 16px" }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                  <span style={{ color: PALETTE.text, fontSize: 13, fontWeight: 500 }}>{tier}</span>
                  <span style={{ color: PALETTE.textMuted, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>{done}/{t}</span>
                </div>
                <div style={{ height: 6, background: PALETTE.surfaceLight, borderRadius: 3, overflow: "hidden" }}>
                  <div style={{ height: "100%", width: `${t > 0 ? (done / t) * 100 : 0}%`, background: PALETTE.green, borderRadius: 3, transition: "width 0.5s ease" }} />
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Space Savings */}
      {tierSavings.length > 0 && (
        <>
          <SectionTitle>Space Savings</SectionTitle>
          <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 24 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 16 }}>
              <StatCard label="Total Saved" value={fmt(stats.bytes_saved || 0)} colour={PALETTE.green} />
              <StatCard label="Total Input" value={fmt(tierSavings.reduce((s, t) => s + t.total_input, 0))} />
              <StatCard label="Total Output" value={fmt(tierSavings.reduce((s, t) => s + t.total_output, 0))} />
              <StatCard
                label="Avg Reduction"
                value={(() => {
                  const inp = tierSavings.reduce((s, t) => s + t.total_input, 0);
                  return inp > 0 ? `${((1 - tierSavings.reduce((s, t) => s + t.total_output, 0) / inp) * 100).toFixed(1)}%` : "—";
                })()}
                colour={PALETTE.accent}
              />
            </div>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${PALETTE.border}` }}>
                  {["Tier", "Done", "Input", "Output", "Saved", "Reduction", "Avg Speed"].map((h) => (
                    <th key={h} style={{ padding: "8px 10px", textAlign: "left", color: PALETTE.textMuted, fontWeight: 500, fontSize: 11 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tierSavings.map((t) => (
                  <tr key={t.tier} style={{ borderBottom: `1px solid ${PALETTE.border}22` }}>
                    <td style={{ padding: "8px 10px", color: PALETTE.text, fontWeight: 500 }}>{t.tier}</td>
                    <td style={{ padding: "8px 10px", color: PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace" }}>{t.completed}</td>
                    <td style={{ padding: "8px 10px", color: PALETTE.textMuted }}>{fmt(t.total_input)}</td>
                    <td style={{ padding: "8px 10px", color: PALETTE.textMuted }}>{fmt(t.total_output)}</td>
                    <td style={{ padding: "8px 10px", color: PALETTE.green, fontFamily: "'JetBrains Mono', monospace" }}>{fmt(t.bytes_saved)}</td>
                    <td style={{ padding: "8px 10px", color: PALETTE.accent }}>{t.total_input > 0 ? `${((1 - t.total_output / t.total_input) * 100).toFixed(1)}%` : "—"}</td>
                    <td style={{ padding: "8px 10px", color: PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace" }}>{t.encode_time > 0 ? `${(t.total_input / t.encode_time / (1024 ** 2)).toFixed(1)} MB/s` : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Errors */}
      {errors.length > 0 && (
        <>
          <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between" }}>
            <SectionTitle>Errors ({errors.length})</SectionTitle>
            <button
              onClick={handleResetErrors}
              disabled={resetting}
              style={{
                background: resetting ? PALETTE.surfaceLight : PALETTE.red,
                color: resetting ? PALETTE.textMuted : "#fff",
                border: "none", borderRadius: 6, padding: "6px 14px",
                fontSize: 12, fontWeight: 600,
                cursor: resetting ? "default" : "pointer",
              }}
            >
              {resetting ? "Resetting..." : "Retry All"}
            </button>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {errors.map(({ path, error: err }, i) => (
              <div key={i} style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.red}33`, borderRadius: 8, padding: "10px 14px", fontSize: 12 }}>
                <div style={{ color: PALETTE.text, wordBreak: "break-all" }}>{path}</div>
                <div style={{ color: PALETTE.red, marginTop: 4 }}>{err}</div>
              </div>
            ))}
          </div>
        </>
      )}

      <style>{`@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }`}</style>
    </div>
  );
}
