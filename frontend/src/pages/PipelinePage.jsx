import { useState, useEffect } from "react";
import { PALETTE } from "../theme";
import { usePolling } from "../lib/usePolling";
import { api } from "../lib/api";
import { StatCard } from "../components/StatCard";
import { SectionTitle } from "../components/SectionTitle";
import { Timeline } from "../components/Timeline";

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

function computeOverallETA(data, libraryTotal) {
  const stats = data.stats || {};
  const files = data.files || {};
  const completed = stats.completed || 0;
  const totalEncodeTime = stats.total_encode_time_secs || 0;
  const tierStats = stats.tier_stats || {};

  const overallAvg = completed > 0 && totalEncodeTime > 0 ? totalEncodeTime / completed : 0;
  if (overallAvg <= 0) return null;

  const doneStatuses = ["completed", "replaced", "done", "skipped", "error", "failed", "verified"];
  let totalSecs = 0;
  let knownRemaining = 0;
  for (const info of Object.values(files)) {
    if (doneStatuses.includes((info.status || "").toLowerCase())) continue;
    knownRemaining++;
    const resKey = info.res_key || "";
    const tier = tierStats[resKey];
    if (tier && tier.completed >= 2 && tier.total_encode_time_secs > 0) {
      totalSecs += tier.total_encode_time_secs / tier.completed;
    } else {
      totalSecs += overallAvg;
    }
  }

  // Account for files not yet in the pipeline queue
  const queueTotal = Object.keys(files).length;
  if (libraryTotal && libraryTotal > queueTotal) {
    const unseenFiles = libraryTotal - queueTotal;
    totalSecs += unseenFiles * overallAvg;
  }

  const totalRemaining = knownRemaining + ((libraryTotal && libraryTotal > queueTotal) ? libraryTotal - queueTotal : 0);
  return totalRemaining > 0 ? totalSecs : null;
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
  const nowSecs = Date.now() / 1000;
  const STALE_SECS = 300;

  // Categorise by thread type
  const threads = { encoding: null, fetching: null, uploading: null, gap_fill: null };
  const recentlyDone = [];

  for (const [path, info] of Object.entries(files)) {
    const s = (info.status || "").toLowerCase();
    const stage = (info.stage || "").toLowerCase();
    const mode = (info.mode || "").toLowerCase();
    const lastUpdatedSecs = info.last_updated ? new Date(info.last_updated).getTime() / 1000 : null;
    const age = lastUpdatedSecs ? nowSecs - lastUpdatedSecs : null;

    if (age !== null && age > STALE_SECS && s !== "done") continue;

    const filename = path.split(/[\\/]/).pop();
    const item = { path, filename, elapsed: age, last_updated: info.last_updated, info };

    if (s === "processing" && mode === "full_gamut") {
      const label = stage === "encoding" ? "ENCODING" : stage === "language_detect" ? "DETECTING" : "PROCESSING";
      if (!threads.encoding || (item.last_updated || "") > (threads.encoding.last_updated || ""))
        threads.encoding = { ...item, thread: label, colour: PALETTE.accent };
    } else if (s === "fetching") {
      if (!threads.fetching || (item.last_updated || "") > (threads.fetching.last_updated || ""))
        threads.fetching = { ...item, thread: "FETCHING", colour: PALETTE.cyan || "#22d3ee" };
    } else if (s === "uploading" && stage === "pending_upload") {
      if (!threads.uploading || (item.last_updated || "") > (threads.uploading.last_updated || ""))
        threads.uploading = { ...item, thread: "AWAITING UPLOAD", colour: "#f97316" };
    } else if (s === "uploading" && (stage === "upload" || stage === "verify" || stage === "replace")) {
      if (!threads.uploading || (item.last_updated || "") > (threads.uploading.last_updated || ""))
        threads.uploading = { ...item, thread: stage === "upload" ? "UPLOADING" : stage === "verify" ? "VERIFYING" : "REPLACING", colour: PALETTE.cyan || "#22d3ee" };
    } else if (s === "processing" && (mode === "gap_filler" || stage === "gap_fill")) {
      if (!threads.gap_fill || (item.last_updated || "") > (threads.gap_fill.last_updated || ""))
        threads.gap_fill = { ...item, thread: "GAP FILL", colour: "#eab308" };
    } else if (s === "done" && age !== null && age < 120) {
      recentlyDone.push({ ...item, thread: "DONE", colour: PALETTE.green, done: true });
    }
  }

  recentlyDone.sort((a, b) => (b.last_updated || "").localeCompare(a.last_updated || ""));

  // Build ordered list: encoding first, then fetching, uploading, gap fill, done
  const result = [];
  if (threads.encoding) result.push(threads.encoding);
  if (threads.fetching) result.push(threads.fetching);
  if (threads.uploading) result.push(threads.uploading);
  if (threads.gap_fill) result.push(threads.gap_fill);
  result.push(...recentlyDone.slice(0, 3));

  return result;
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
    upcoming.push({ path, filename, status, priority: true, res_key: info?.res_key, tier: info?.tier });
    seen.add(path);
  }

  // Then pipeline state items (fetched/pending/encoded/uploaded)
  for (const [path, info] of Object.entries(files)) {
    if (seen.has(path)) continue;
    const s = (info.status || "").toLowerCase();
    if (["fetched", "pending", "encoded", "uploaded"].includes(s)) {
      const filename = path.split(/[\\/]/).pop();
      upcoming.push({ path, filename, status: info.status, priority: false,
        added: info.added || info.last_updated, res_key: info?.res_key, tier: info?.tier });
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

function estimateFileETA(item, tierStats, overallAvg) {
  if (!tierStats || overallAvg <= 0) return null;
  const rk = item.res_key;
  if (rk && tierStats[rk]) {
    const t = tierStats[rk];
    if (t.completed >= 2 && t.total_encode_time_secs > 0) {
      return t.total_encode_time_secs / t.completed;
    }
  }
  return overallAvg;
}

function fmtETA(secs) {
  if (!secs || secs <= 0) return "";
  if (secs < 60) return `~${Math.round(secs)}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `~${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return rm > 0 ? `~${h}h ${rm}m` : `~${h}h`;
}

function MissingFilesDrillDown({ category, data, onClose, onRefresh }) {
  const [renaming, setRenaming] = useState(null);
  const [renameValue, setRenameValue] = useState("");

  if (!data) return <div style={{ marginTop: 16, color: PALETTE.textMuted, fontSize: 11 }}>Loading...</div>;

  const handleRename = async (filepath) => {
    if (!renameValue.trim()) return;
    try {
      await api.renameFile(filepath, renameValue);
      setRenaming(null);
      onRefresh?.();
    } catch (e) {
      alert(`Rename failed: ${e.message}`);
    }
  };

  const langLabel = (lang) => {
    if (!lang || lang === "und") return "und";
    return lang;
  };

  const trackBadge = (lang, isEnglish) => ({
    background: isEnglish ? PALETTE.green + "22" : PALETTE.red + "22",
    color: isEnglish ? PALETTE.green : PALETTE.red,
    padding: "1px 4px", borderRadius: 3, fontSize: 9, fontWeight: 600,
  });

  const engLangs = new Set(["eng", "en", "english"]);

  return (
    <div style={{ marginTop: 16, background: PALETTE.surfaceLight, borderRadius: 8, padding: 12, maxHeight: 400, overflow: "auto", textAlign: "left" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
        <span style={{ color: PALETTE.text, fontSize: 12, fontWeight: 600 }}>
          {data.count} files — {category}
        </span>
        <button onClick={onClose} style={{ background: "transparent", border: "none", color: PALETTE.textMuted, cursor: "pointer", fontSize: 11 }}>close</button>
      </div>

      {data.files.map((f, i) => (
        <div key={i} style={{ padding: "6px 0", borderBottom: `1px solid ${PALETTE.border}22` }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            {!f.has_english_audio && (
              <span style={{ background: PALETTE.red + "33", color: PALETTE.red, padding: "1px 5px", borderRadius: 3, fontSize: 8, fontWeight: 700 }}>NO ENG AUDIO</span>
            )}
            <span style={{ color: PALETTE.text, fontSize: 11, flex: 1 }}>{f.filename}</span>
            <span style={{ color: PALETTE.textMuted, fontSize: 9 }}>{f.video_codec}</span>

            {/* Rename button */}
            {category === "filename" && f.suggested_name && renaming !== i && (
              <button onClick={() => { setRenaming(i); setRenameValue(f.suggested_name); }}
                style={{ background: PALETTE.accent, color: "#fff", border: "none", borderRadius: 4, padding: "2px 6px", fontSize: 9, cursor: "pointer" }}>
                rename
              </button>
            )}
          </div>

          {/* Rename input */}
          {renaming === i && (
            <div style={{ display: "flex", gap: 4, marginTop: 4 }}>
              <input value={renameValue} onChange={e => setRenameValue(e.target.value)}
                style={{ flex: 1, background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 4, padding: "3px 6px", color: PALETTE.text, fontSize: 10 }} />
              <button onClick={() => handleRename(f.filepath)}
                style={{ background: PALETTE.green, color: "#fff", border: "none", borderRadius: 4, padding: "3px 8px", fontSize: 9, cursor: "pointer" }}>save</button>
              <button onClick={() => setRenaming(null)}
                style={{ background: "transparent", border: `1px solid ${PALETTE.border}`, borderRadius: 4, padding: "3px 8px", fontSize: 9, cursor: "pointer", color: PALETTE.textMuted }}>cancel</button>
            </div>
          )}

          {/* Audio tracks */}
          {(category === "audio" || category === "langs") && f.audio_tracks && f.audio_tracks.length > 0 && (
            <div style={{ marginTop: 3, display: "flex", gap: 4, flexWrap: "wrap" }}>
              {f.audio_tracks.map((t, j) => (
                <span key={j} style={trackBadge(t.language, engLangs.has(t.language?.toLowerCase()))}>
                  {t.codec} {langLabel(t.language)} {t.channels}ch
                </span>
              ))}
            </div>
          )}

          {/* Sub tracks */}
          {category === "subs" && f.sub_tracks && f.sub_tracks.length > 0 && (
            <div style={{ marginTop: 3, display: "flex", gap: 4, flexWrap: "wrap" }}>
              {f.sub_tracks.map((t, j) => (
                <span key={j} style={trackBadge(t.language, engLangs.has(t.language?.toLowerCase()) || t.language === "und")}>
                  {t.codec} {langLabel(t.language)}
                </span>
              ))}
            </div>
          )}

          {/* Suggested filename */}
          {category === "filename" && f.suggested_name && renaming !== i && (
            <div style={{ marginTop: 2, color: PALETTE.accent, fontSize: 9 }}>{"\u2192"} {f.suggested_name}</div>
          )}
        </div>
      ))}

      {data.count > 500 && (
        <div style={{ color: PALETTE.textMuted, fontSize: 10, marginTop: 8 }}>Showing first 500 of {data.count}</div>
      )}
    </div>
  );
}


export function PipelinePage({ wsData, onFileClick }) {
  // Use WebSocket data if available, fall back to polling
  const { data: polledData, error } = usePolling(api.getPipeline, 3000, { enabled: !wsData });
  const data = wsData || polledData;
  const [starting, setStarting] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [priorityPaths, setPriorityPaths] = useState([]);
  const [libraryTotal, setLibraryTotal] = useState(null);
  const [completion, setCompletion] = useState(null);
  const [quickWinsBusy, setQuickWinsBusy] = useState(false);
  const [forceList, setForceList] = useState(null);
  const [showForce, setShowForce] = useState(false);
  const [missingCategory, setMissingCategory] = useState(null);
  const [missingFiles, setMissingFiles] = useState(null);

  const handleBarClick = async (catKey) => {
    if (missingCategory === catKey) {
      setMissingCategory(null);
      setMissingFiles(null);
      return;
    }
    setMissingCategory(catKey);
    setMissingFiles(null);
    try {
      const data = await api.getCompletionMissing(catKey);
      setMissingFiles(data);
    } catch { setMissingFiles(null); }
  };

  useEffect(() => {
    const load = () => api.getPriority().then((p) => setPriorityPaths(p?.paths || [])).catch(() => {});
    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const load = () => {
      api.getLibraryCompletion().then(setCompletion).catch(() => {});
      fetch("/api/media-report")
        .then((r) => r.ok ? r.json() : null)
        .then((d) => { if (d?.summary?.total_files) setLibraryTotal(d.summary.total_files); })
        .catch(() => {});
    };
    load();
    const id = setInterval(load, 30000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    const load = () => api.getForceList().then(setForceList).catch(() => {});
    load();
    const id = setInterval(load, 10000);
    return () => clearInterval(id);
  }, []);

  const handleRemoveForce = async (path) => {
    await api.removeForce(path);
    api.getForceList().then(setForceList).catch(() => {});
  };

  const handleQuickWins = async () => {
    setQuickWinsBusy(true);
    try {
      await api.startProcess("gap_filler");
    } catch { /* ignore */ }
    setQuickWinsBusy(false);
  };

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
  const queueTotal = Object.keys(files).length;
  const total = libraryTotal || queueTotal;
  const completed = stats.completed || 0;
  const pct = total > 0 ? ((completed / total) * 100) : 0;
  const groups = groupStatuses(files);
  const tierProgress = getTierProgress(files);
  const tierSavings = getTierSavings(stats);
  const errors = getErrors(files);
  const activeFiles = getActiveFiles(data);
  const upNext = getUpNext(data, priorityPaths);
  const overallETA = computeOverallETA(data, libraryTotal);

  const GROUP_COLOURS = { Queued: PALETTE.textMuted, "In Progress": PALETTE.accent, Done: PALETTE.green, Skipped: PALETTE.textMuted, Error: PALETTE.red };

  return (
    <div>
      {/* Hero — true completion */}
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 16, padding: "32px 40px", marginBottom: 24, textAlign: "center" }}>
        {completion ? (
          <>
            <div style={{ fontSize: 56, fontWeight: 800, fontFamily: "'JetBrains Mono', monospace", color: PALETTE.green, lineHeight: 1 }}>
              {completion.pct_done.toFixed(1)}%
            </div>
            <div style={{ color: PALETTE.textMuted, fontSize: 12, marginTop: 4 }}>fully done</div>
            <div style={{ margin: "16px auto", maxWidth: 500, height: 8, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
              <div style={{ height: "100%", width: `${Math.min(completion.pct_done, 100)}%`, background: PALETTE.green, borderRadius: 4, transition: "width 0.5s ease" }} />
            </div>
            <div style={{ color: PALETTE.textMuted, fontSize: 13, marginTop: 8 }}>
              {completion.fully_done.toLocaleString()} / {completion.total.toLocaleString()} files · {fmt(stats.bytes_saved || 0)} saved
            </div>

            {/* Breakdown bars */}
            <div style={{ display: "flex", justifyContent: "center", gap: 16, marginTop: 16, flexWrap: "wrap" }}>
              {[
                { key: "video", label: "AV1 Video", pct: completion.pct_video, count: completion.av1, colour: PALETTE.accent },
                { key: "audio", label: "EAC-3 Audio", pct: completion.pct_audio, count: completion.eac3_done, colour: PALETTE.cyan || "#22d3ee" },
                { key: "subs", label: "1 Eng Sub", pct: completion.pct_subs, count: completion.subs_done, colour: "#a78bfa" },
                { key: "foreign_subs", label: "No Foreign Subs", pct: completion.pct_no_foreign_subs || 0, count: completion.no_foreign_subs || 0, colour: "#c084fc" },
                { key: "tmdb", label: "TMDb Metadata", pct: completion.pct_tmdb || 0, count: completion.has_tmdb || 0, colour: "#f59e0b" },
                { key: "langs", label: "Langs Known", pct: completion.pct_langs_known || 0, count: completion.total - (completion.und_audio_files || 0) - (completion.und_sub_files || 0), colour: "#10b981" },
                { key: "filename", label: "Clean Filenames", pct: completion.pct_filename || 0, count: completion.has_clean_filename || 0, colour: "#6366f1" },
              ].map(({ key, label, pct: p, count, colour }) => {
                const remaining = Math.max(0, completion.total - count);
                return (
                  <div key={key} onClick={() => handleBarClick(key)}
                    style={{ minWidth: 110, textAlign: "center", cursor: "pointer", opacity: missingCategory && missingCategory !== key ? 0.5 : 1 }}>
                    <div style={{ fontSize: 16, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: colour }}>
                      {p.toFixed(1)}%
                    </div>
                    <div style={{ margin: "4px auto", width: 80, height: 4, background: PALETTE.surfaceLight, borderRadius: 2, overflow: "hidden" }}>
                      <div style={{ height: "100%", width: `${Math.min(p, 100)}%`, background: colour, borderRadius: 2 }} />
                    </div>
                    <div style={{ fontSize: 9, color: PALETTE.textMuted, lineHeight: 1.4 }}>
                      {label}
                      <br />
                      <span style={{ color: remaining > 0 ? "#f59e0b" : PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace" }}>
                        {remaining > 0 ? `${remaining.toLocaleString()} to go` : "✓ all done"}
                      </span>
                      <br />
                      <span style={{ fontSize: 8, opacity: 0.7 }}>{count.toLocaleString()} / {completion.total.toLocaleString()}</span>
                    </div>
                  </div>
                );
              })}
            </div>

            {/* Missing files drill-down */}
            {missingCategory && (
              <MissingFilesDrillDown
                category={missingCategory}
                data={missingFiles}
                onClose={() => { setMissingCategory(null); setMissingFiles(null); }}
                onRefresh={() => handleBarClick(missingCategory)}
              />
            )}

            {/* Quick Wins */}
            {(completion.gap_fill_count > 0) && (
              <div style={{ marginTop: 16 }}>
                <button
                  onClick={handleQuickWins}
                  disabled={quickWinsBusy}
                  style={{
                    background: quickWinsBusy ? PALETTE.surfaceLight : PALETTE.accent,
                    color: quickWinsBusy ? PALETTE.textMuted : "#fff",
                    border: "none", borderRadius: 8, padding: "8px 20px",
                    fontSize: 12, fontWeight: 600, cursor: quickWinsBusy ? "default" : "pointer",
                  }}
                >
                  {quickWinsBusy ? "Starting..." : `Run Gap Filler (${completion.gap_fill_count.toLocaleString()} files need work)`}
                </button>
              </div>
            )}
          </>
        ) : (
          <div style={{ fontSize: 56, fontWeight: 800, fontFamily: "'JetBrains Mono', monospace", color: PALETTE.green, lineHeight: 1 }}>
            {pct.toFixed(1)}%
          </div>
        )}

        {/* Session Progress */}
        {(() => {
          const gapFilled = stats.gap_filled || 0;
          const errCount = Object.values(files).filter(f => ["error", "failed"].includes((f.status || "").toLowerCase())).length;
          const speed = stats.total_content_duration_secs > 0 && stats.total_encode_time_secs > 0
            ? (stats.total_encode_time_secs / (stats.total_content_duration_secs / 3600)).toFixed(1)
            : null;
          const speedGb = stats.total_source_size_bytes > 0 && stats.total_encode_time_secs > 0
            ? (stats.total_encode_time_secs / 60 / (stats.total_source_size_bytes / (1024 ** 3))).toFixed(1)
            : null;
          const inputSize = stats.total_source_size_bytes || 0;
          const outputSize = inputSize - (stats.bytes_saved || 0);

          const statStyle = { display: "inline-flex", alignItems: "baseline", gap: 4, fontSize: 13 };
          const valStyle = (colour) => ({ fontFamily: "'JetBrains Mono', monospace", fontWeight: 700, color: colour || PALETTE.text });
          const sepStyle = { color: PALETTE.border, margin: "0 6px" };

          return (
            <div style={{ marginTop: 16, textAlign: "center", lineHeight: 2 }}>
              <div>
                {completed > 0 && (
                  <span style={statStyle}>
                    <span style={{ color: PALETTE.textMuted }}>Encoded:</span>
                    <span style={valStyle(PALETTE.accent)}>{completed}</span>
                    <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>({fmt(inputSize)} {"\u2192"} {fmt(outputSize > 0 ? outputSize : 0)})</span>
                  </span>
                )}
                {completed > 0 && gapFilled > 0 && <span style={sepStyle}>{"\u00b7"}</span>}
                {gapFilled > 0 && (
                  <span style={statStyle}>
                    <span style={{ color: PALETTE.textMuted }}>Gap filled:</span>
                    <span style={valStyle("#eab308")}>{gapFilled}</span>
                  </span>
                )}
                {errCount > 0 && (
                  <>
                    <span style={sepStyle}>{"\u00b7"}</span>
                    <span style={statStyle}>
                      <span style={{ color: PALETTE.textMuted }}>Errors:</span>
                      <span style={valStyle(PALETTE.red)}>{errCount}</span>
                    </span>
                  </>
                )}
              </div>
              <div>
                {speed && (
                  <span style={statStyle}>
                    <span style={{ color: PALETTE.textMuted }}>Speed:</span>
                    <span style={valStyle(PALETTE.accent)}>{speed}</span>
                    <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>min/hr</span>
                    {speedGb && <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>({speedGb} min/GB)</span>}
                  </span>
                )}
                {speed && stats.bytes_saved > 0 && <span style={sepStyle}>{"\u00b7"}</span>}
                {stats.bytes_saved > 0 && (
                  <span style={statStyle}>
                    <span style={valStyle(PALETTE.green)}>{fmt(stats.bytes_saved)}</span>
                    <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>saved</span>
                  </span>
                )}
                {overallETA != null && (
                  <>
                    <span style={sepStyle}>{"\u00b7"}</span>
                    <span style={statStyle}>
                      <span style={{ color: PALETTE.textMuted }}>ETA:</span>
                      <span style={valStyle(PALETTE.accent)}>{formatETA(overallETA)}</span>
                    </span>
                  </>
                )}
              </div>
            </div>
          );
        })()}
      </div>

      {/* Force List */}
      {forceList && forceList.count > 0 && (
        <>
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
            <SectionTitle>Force Next ({forceList.count})</SectionTitle>
            <button onClick={() => setShowForce(!showForce)} style={{
              background: "transparent", border: `1px solid ${PALETTE.border}`, borderRadius: 6,
              color: PALETTE.textMuted, padding: "2px 8px", fontSize: 10, cursor: "pointer",
            }}>{showForce ? "Hide" : "Show"}</button>
          </div>
          {showForce && (
            <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 8, padding: 12, marginBottom: 24, maxHeight: 250, overflow: "auto" }}>
              {forceList.items.map((item, i) => (
                <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "4px 0", borderBottom: `1px solid ${PALETTE.border}22`, fontSize: 12 }}>
                  <span style={{ color: item.exists ? PALETTE.text : PALETTE.red, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {i === 0 ? ">> " : `${i + 1}. `}{item.filename}
                  </span>
                  <button onClick={() => handleRemoveForce(item.filepath)} style={{
                    background: "transparent", border: "none", color: PALETTE.red, cursor: "pointer", fontSize: 10, padding: "2px 6px",
                  }}>remove</button>
                </div>
              ))}
            </div>
          )}
        </>
      )}

      {/* Current activity — grouped by thread */}
      {activeFiles.length > 0 && (
        <>
          <SectionTitle>Current Activity</SectionTitle>
          <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 24 }}>
            {activeFiles.map((item, i) => (
              <div key={i} style={{
                background: PALETTE.surface,
                border: `1px solid ${item.done ? PALETTE.green + "22" : PALETTE.border}`,
                borderRadius: 8, padding: "10px 14px",
                display: "flex", alignItems: "center", gap: 10,
                opacity: item.done ? 0.5 : 1,
              }}>
                <div style={{
                  width: 7, height: 7, borderRadius: "50%", flexShrink: 0,
                  background: item.colour,
                  animation: item.done ? "none" : "pulse 1.5s infinite",
                }} />
                <span style={{
                  fontSize: 10, fontWeight: 700, letterSpacing: 0.5,
                  color: item.colour, minWidth: 90, flexShrink: 0,
                  fontFamily: "'JetBrains Mono', monospace",
                }}>{item.thread}</span>
                <span style={{
                  color: PALETTE.text, fontSize: 12, flex: 1,
                  overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                }}>{item.filename}</span>
                {item.elapsed != null && (
                  <span style={{
                    fontSize: 11, fontFamily: "'JetBrains Mono', monospace",
                    color: PALETTE.textMuted, flexShrink: 0,
                  }}>{item.done ? `${Math.round(item.elapsed)}s ago` : formatETA(item.elapsed)}</span>
                )}
              </div>
            ))}
          </div>
        </>
      )}

      {/* Timeline — stage breakdown for recent encodes */}
      {data?.files && (
        (() => {
          const hasTimeline = Object.values(data.files).some((f) => f.encode_time_secs > 0 && (f.status === "replaced" || f.status === "verified"));
          return hasTimeline ? (
            <>
              <SectionTitle>Recent Encodes — Stage Breakdown</SectionTitle>
              <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16, marginBottom: 24 }}>
                <Timeline files={data.files} />
              </div>
            </>
          ) : null;
        })()
      )}

      {/* Up Next */}
      {upNext.length > 0 && (
        <>
          <SectionTitle>Up Next</SectionTitle>
          <div style={{ display: "flex", flexDirection: "column", gap: 4, marginBottom: 24 }}>
            {upNext.map((item, i) => {
              const { path, filename, status, priority: isPrio, res_key } = item;
              const sl = (status || "").toLowerCase();
              const badgeColor = isPrio ? PALETTE.accentWarm : sl === "fetched" ? PALETTE.green : PALETTE.textMuted;
              const badgeBg = isPrio ? PALETTE.accentWarm + "22" : sl === "fetched" ? PALETTE.green + "22" : PALETTE.surfaceLight;
              const tierStats = data?.stats?.tier_stats || {};
              const overallCompleted = data?.stats?.completed || 0;
              const overallTime = data?.stats?.total_encode_time_secs || 0;
              const overallAvg = overallCompleted > 0 ? overallTime / overallCompleted : 0;
              const eta = estimateFileETA(item, tierStats, overallAvg);
              return (
                <div key={i} style={{
                  background: PALETTE.surface,
                  border: `1px solid ${isPrio ? PALETTE.accentWarm + "44" : PALETTE.border}`,
                  borderRadius: 8, padding: "8px 14px",
                  display: "flex", alignItems: "center", gap: 10,
                  cursor: onFileClick ? "pointer" : "default",
                }} onClick={() => onFileClick?.(path)}>
                  <span style={{ color: PALETTE.textMuted, fontSize: 11, fontFamily: "'JetBrains Mono', monospace", width: 20, textAlign: "right", flexShrink: 0 }}>{i + 1}</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ color: PALETTE.text, fontSize: 12, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{filename}</div>
                    <div style={{ color: PALETTE.textMuted, fontSize: 10, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{path}</div>
                  </div>
                  {eta > 0 && <span style={{ fontSize: 10, color: PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace", flexShrink: 0 }}>{fmtETA(eta)}</span>}
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

      {/* Remaining Work (from media report, not pipeline state) */}
      {completion?.tiers?.length > 0 && (
        <>
          <SectionTitle>Remaining Work</SectionTitle>
          <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 24 }}>
            {completion.tiers.filter(t => t.name !== "Done").map((t) => (
              <div key={t.name} style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 8, padding: "12px 16px" }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                  <span style={{ color: PALETTE.text, fontSize: 13, fontWeight: 500 }}>{t.name}</span>
                  <span style={{ color: PALETTE.textMuted, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>{t.total.toLocaleString()} files</span>
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
                <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12 }}>
                  <div style={{ minWidth: 0 }}>
                    <div style={{ color: PALETTE.text, wordBreak: "break-all" }}>{path}</div>
                    <div style={{ color: PALETTE.red, marginTop: 4 }}>{err}</div>
                  </div>
                  {err === "duration mismatch" && (
                    <button
                      onClick={async () => {
                        await api.forceAccept(path);
                      }}
                      style={{
                        flexShrink: 0,
                        background: PALETTE.surfaceLight,
                        color: PALETTE.textMuted,
                        border: `1px solid ${PALETTE.border}`,
                        borderRadius: 5, padding: "4px 10px",
                        fontSize: 11, fontWeight: 600, cursor: "pointer", whiteSpace: "nowrap",
                      }}
                      title="Override duration check and send to replace"
                    >
                      Force Accept
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      <style>{`@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }`}</style>
    </div>
  );
}
