import { useState, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import { PALETTE, getCodecColour, getResColour, getAudioColour } from "../theme";
import { fmt, fmtNum, aggregate } from "../lib/format";
import { api } from "../lib/api";
import { usePolling } from "../lib/usePolling";
import { StatCard } from "../components/StatCard";
import { SectionTitle } from "../components/SectionTitle";
import { PieSection } from "../components/PieSection";
import { TabButton } from "../components/TabButton";
import { TopFiles } from "../components/TopFiles";

function BitrateDistribution({ files, title }) {
  const buckets = [
    { label: "<2", min: 0, max: 2000 },
    { label: "2-5", min: 2000, max: 5000 },
    { label: "5-10", min: 5000, max: 10000 },
    { label: "10-20", min: 10000, max: 20000 },
    { label: "20-40", min: 20000, max: 40000 },
    { label: "40-80", min: 40000, max: 80000 },
    { label: "80+", min: 80000, max: Infinity },
  ];
  const data = buckets.map((b) => ({
    name: b.label,
    count: files.filter((f) => {
      const br = f.overall_bitrate_kbps;
      return br != null && br >= b.min && br < b.max;
    }).length,
    size_gb: files.filter((f) => {
      const br = f.overall_bitrate_kbps;
      return br != null && br >= b.min && br < b.max;
    }).reduce((s, f) => s + f.file_size_gb, 0),
  }));

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, flex: "1 1 400px", minWidth: 340 }}>
      <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>{title}</div>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
          <XAxis dataKey="name" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} axisLine={{ stroke: PALETTE.border }} tickLine={false} label={{ value: "Mbps", position: "insideBottomRight", offset: -5, fill: PALETTE.textMuted, fontSize: 11 }} />
          <YAxis tick={{ fill: PALETTE.textMuted, fontSize: 11 }} axisLine={false} tickLine={false} />
          <Tooltip
            contentStyle={{ background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`, borderRadius: 8, color: PALETTE.text, fontSize: 13 }}
            formatter={(v, name) => [name === "size_gb" ? fmt(v) : fmtNum(v), name === "size_gb" ? "Size" : "Files"]}
          />
          <Bar dataKey="count" fill={PALETTE.accent} radius={[4, 4, 0, 0]} name="Files" />
          <Bar dataKey="size_gb" fill={PALETTE.purple} radius={[4, 4, 0, 0]} name="Size (GB)" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function AudioAnalysis({ files }) {
  const losslessFiles = files.filter((f) => f.audio_streams?.some((a) => a.lossless));
  const multiAudio = files.filter((f) => f.audio_stream_count > 1);
  const totalAudioEstGB = files.reduce((s, f) => s + (f.audio_estimated_size_gb || 0), 0);
  const losslessAudioGB = losslessFiles.reduce((s, f) => s + (f.audio_estimated_size_gb || 0), 0);
  const totalSizeGB = files.reduce((s, f) => s + f.file_size_gb, 0);

  const codecData = aggregate(
    files.filter((f) => f.audio_streams?.length > 0),
    (f) => f.audio_streams[0].codec
  );

  const allStreams = [];
  files.forEach((f) => { f.audio_streams?.forEach((a) => { allStreams.push({ ...a, file_size_gb: 0 }); }); });
  const totalStreams = allStreams.length;
  const losslessStreams = allStreams.filter((a) => a.lossless).length;

  return (
    <div>
      <SectionTitle>Audio Analysis</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <StatCard label="Est. Audio Size" value={fmt(totalAudioEstGB)} sub={totalSizeGB > 0 ? `${((totalAudioEstGB / totalSizeGB) * 100).toFixed(1)}% of library` : ""} />
        <StatCard label="Lossless Audio" value={fmt(losslessAudioGB)} sub={`${losslessFiles.length} files`} colour={PALETTE.purple} />
        <StatCard label="Multi-Track Files" value={fmtNum(multiAudio.length)} sub={files.length > 0 ? `${((multiAudio.length / files.length) * 100).toFixed(1)}% of files` : ""} />
        <StatCard label="Total Streams" value={fmtNum(totalStreams)} sub={`${losslessStreams} lossless`} />
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 16 }}>
        <PieSection data={codecData} colourFn={getAudioColour} title="Primary Audio Codec (by size)" />
      </div>
    </div>
  );
}

const FILENAME_PATTERNS = [
  { key: "dots", label: "Dot-separated", test: (stem) => (stem.match(/\./g) || []).length >= 3 },
  { key: "resolution", label: "Resolution tags", test: (stem) => /\b(1080p|720p|480p|2160p|4K|UHD)\b/i.test(stem) },
  { key: "source", label: "Source tags", test: (stem) => /\b(WEB[-.]?DL|WEBRip|BluRay|BDRip|HDTV|DVDRip|REMUX)\b/i.test(stem) },
  { key: "codec", label: "Codec tags", test: (stem) => /\b(x264|x265|H\.?264|H\.?265|HEVC|AVC)\b/i.test(stem) },
  { key: "group", label: "Release groups", test: (stem) => /\[-?[A-Za-z0-9]+\]\s*$/.test(stem) || /\[[-A-Za-z0-9.]+\]/.test(stem) },
  { key: "service", label: "Service tags", test: (stem) => /\b(AMZN|DSNP|NF|HULU|MAX|HBO|ATVP|PCOK|PMTP)\b/i.test(stem) },
];

function analyseFilenames(files) {
  const issues = [];
  for (const f of files) {
    const name = f.filepath.replace(/\\/g, "/").split("/").pop() || "";
    const stem = name.replace(/\.[^.]+$/, "");
    const matched = FILENAME_PATTERNS.filter((p) => p.test(stem)).map((p) => p.key);
    if (matched.length > 0) {
      issues.push({ file: f, name, matched });
    }
  }
  return issues;
}

function countKeywordMatches(files, keyword) {
  if (!keyword) return 0;
  try {
    const re = new RegExp(`\\b${keyword.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
    return files.filter((f) => {
      const name = f.filepath.replace(/\\/g, "/").split("/").pop() || "";
      const stem = name.replace(/\.[^.]+$/, "");
      return re.test(stem);
    }).length;
  } catch {
    return 0;
  }
}

function FilenameHealth({ files, onReload }) {
  const [expanded, setExpanded] = useState(null);
  const [stripRunning, setStripRunning] = useState(false);
  const [rescanning, setRescanning] = useState(false);
  const [customTags, setCustomTags] = useState([]);
  const [newTag, setNewTag] = useState("");

  useEffect(() => {
    api.getCustomTags().then((d) => setCustomTags(d.keywords || [])).catch(() => {});
  }, []);

  const addTag = () => {
    const tag = newTag.trim();
    if (!tag || customTags.includes(tag)) return;
    const updated = [...customTags, tag];
    setCustomTags(updated);
    setNewTag("");
    api.setCustomTags(updated).catch(() => {});
  };

  const removeTag = (tag) => {
    const updated = customTags.filter((t) => t !== tag);
    setCustomTags(updated);
    api.setCustomTags(updated).catch(() => {});
  };

  const issues = analyseFilenames(files);
  const byCat = {};
  for (const p of FILENAME_PATTERNS) byCat[p.key] = 0;
  for (const i of issues) {
    for (const k of i.matched) byCat[k]++;
  }

  // Group by folder
  const byFolder = {};
  for (const i of issues) {
    const folder = getFolder(i.file.filepath);
    if (!byFolder[folder]) byFolder[folder] = [];
    byFolder[folder].push(i);
  }
  const folderEntries = Object.entries(byFolder).sort((a, b) => b[1].length - a[1].length);

  const handleStrip = async () => {
    setStripRunning(true);
    try {
      await api.startProcess("strip_tags");
    } catch {
      setStripRunning(false);
    }
  };

  // Poll strip_tags -> when done, auto-start scanner -> when done, reload report
  useEffect(() => {
    if (!stripRunning) return;
    const id = setInterval(async () => {
      try {
        const s = await api.getProcessStatus("strip_tags");
        if (s.status !== "running") {
          setStripRunning(false);
          setRescanning(true);
          try { await api.startProcess("scanner"); } catch { setRescanning(false); }
        }
      } catch { /* ignore */ }
    }, 2000);
    return () => clearInterval(id);
  }, [stripRunning]);

  useEffect(() => {
    if (!rescanning) return;
    const id = setInterval(async () => {
      try {
        const s = await api.getProcessStatus("scanner");
        if (s.status !== "running") {
          setRescanning(false);
          onReload?.();
        }
      } catch { /* ignore */ }
    }, 2000);
    return () => clearInterval(id);
  }, [rescanning, onReload]);

  if (issues.length === 0) return null;

  const pct = ((issues.length / files.length) * 100).toFixed(1);

  return (
    <div>
      <SectionTitle>Filename Health</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20, alignItems: "flex-start" }}>
        <StatCard label="Messy Filenames" value={fmtNum(issues.length)} sub={`${pct}% of ${fmtNum(files.length)} files`} colour={PALETTE.accentWarm} />
        {FILENAME_PATTERNS.map((p) => byCat[p.key] > 0 && (
          <StatCard key={p.key} label={p.label} value={fmtNum(byCat[p.key])} />
        ))}
      </div>

      {/* Action button */}
      <div style={{ marginBottom: 20 }}>
        <button
          onClick={handleStrip}
          disabled={stripRunning || rescanning}
          style={{
            background: (stripRunning || rescanning) ? PALETTE.surfaceLight : PALETTE.accentWarm,
            color: (stripRunning || rescanning) ? PALETTE.textMuted : "#fff",
            border: "none", borderRadius: 8, padding: "8px 18px",
            fontSize: 13, fontWeight: 600,
            cursor: (stripRunning || rescanning) ? "default" : "pointer",
          }}
        >
          {stripRunning ? "Renaming files..." : rescanning ? "Rescanning library..." : "Run Strip Tags"}
        </button>
        <span style={{ color: PALETTE.textMuted, fontSize: 11, marginLeft: 10 }}>
          Renames files to clean format (series only). Don't run while pipeline is encoding.
        </span>
      </div>

      {/* Custom tag keywords */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Custom Tag Keywords</div>
        <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 10 }}>
          <input
            type="text"
            value={newTag}
            onChange={(e) => setNewTag(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && addTag()}
            placeholder="Add keyword (e.g. RARBG)"
            style={{
              background: PALETTE.surfaceLight, color: PALETTE.text,
              border: `1px solid ${PALETTE.border}`, borderRadius: 6,
              padding: "6px 10px", fontSize: 13, width: 220,
              outline: "none",
            }}
          />
          <button
            onClick={addTag}
            disabled={!newTag.trim()}
            style={{
              background: newTag.trim() ? PALETTE.accent : PALETTE.surfaceLight,
              color: newTag.trim() ? "#fff" : PALETTE.textMuted,
              border: "none", borderRadius: 6, padding: "6px 14px",
              fontSize: 13, fontWeight: 600, cursor: newTag.trim() ? "pointer" : "default",
            }}
          >
            Add
          </button>
          {newTag.trim() && (
            <span style={{ color: PALETTE.textMuted, fontSize: 12 }}>
              {countKeywordMatches(files, newTag.trim())} matches
            </span>
          )}
        </div>
        {customTags.length > 0 && (
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {customTags.map((tag) => {
              const count = countKeywordMatches(files, tag);
              return (
                <span
                  key={tag}
                  style={{
                    display: "inline-flex", alignItems: "center", gap: 6,
                    background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`,
                    borderRadius: 16, padding: "4px 10px", fontSize: 12, color: PALETTE.text,
                  }}
                >
                  {tag}
                  <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>{count}</span>
                  <span
                    onClick={() => removeTag(tag)}
                    style={{ cursor: "pointer", color: PALETTE.textMuted, fontWeight: 700, fontSize: 13, lineHeight: 1 }}
                    title="Remove"
                  >
                    x
                  </span>
                </span>
              );
            })}
          </div>
        )}
        <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 6 }}>
          Keywords are included in Strip Tags runs. Match counts show files containing each keyword.
        </div>
      </div>

      {/* Grouped file list */}
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16 }}>
        <div style={{ color: PALETTE.text, fontSize: 14, fontWeight: 600, marginBottom: 12 }}>
          Affected files by folder ({folderEntries.length} folders)
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {folderEntries.map(([folder, items]) => {
            const label = folder.split("\\").pop() || folder;
            const isOpen = expanded === folder;
            return (
              <div key={folder}>
                <div
                  onClick={() => setExpanded(isOpen ? null : folder)}
                  style={{
                    display: "flex", alignItems: "center", gap: 8,
                    padding: "6px 10px", borderRadius: 6,
                    cursor: "pointer", fontSize: 13,
                    background: isOpen ? PALETTE.surfaceLight : "transparent",
                    color: PALETTE.text,
                  }}
                >
                  <span style={{ color: PALETTE.textMuted, fontSize: 11, fontFamily: "monospace", width: 16, textAlign: "center" }}>
                    {isOpen ? "\u25BC" : "\u25B6"}
                  </span>
                  <span style={{ flex: 1 }}>{label}</span>
                  <span style={{ color: PALETTE.textMuted, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>{items.length}</span>
                </div>
                {isOpen && (
                  <div style={{ padding: "4px 0 8px 34px" }}>
                    {items.map((item, j) => (
                      <div key={j} style={{ fontSize: 12, color: PALETTE.textMuted, lineHeight: 1.8, display: "flex", gap: 8, alignItems: "baseline" }}>
                        <span style={{ color: PALETTE.text, flex: 1, wordBreak: "break-all" }}>{item.name}</span>
                        <span style={{ flexShrink: 0, fontSize: 10, color: PALETTE.accentWarm }}>
                          {item.matched.map((k) => FILENAME_PATTERNS.find((p) => p.key === k)?.label).join(", ")}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function SavingsEstimate({ files }) {
  const nonAV1 = files.filter((f) => f.video?.codec_raw !== "av1");
  const h264Files = files.filter((f) => f.video?.codec === "H.264");
  const hevcFiles = files.filter((f) => f.video?.codec === "HEVC (H.265)");
  const otherFiles = nonAV1.filter((f) => f.video?.codec !== "H.264" && f.video?.codec !== "HEVC (H.265)");

  const h264Size = h264Files.reduce((s, f) => s + f.file_size_gb, 0);
  const hevcSize = hevcFiles.reduce((s, f) => s + f.file_size_gb, 0);
  const otherSize = otherFiles.reduce((s, f) => s + f.file_size_gb, 0);

  const h264Saving = h264Size * 0.55;
  const hevcSaving = hevcSize * 0.25;
  const otherSaving = otherSize * 0.6;
  const totalSaving = h264Saving + hevcSaving + otherSaving;
  const currentTotal = files.reduce((s, f) => s + f.file_size_gb, 0);

  const data = [
    { name: "H.264 → AV1", current: h264Size, saving: h264Saving, count: h264Files.length, fill: "#3b82f6" },
    { name: "HEVC → AV1", current: hevcSize, saving: hevcSaving, count: hevcFiles.length, fill: "#10b981" },
    { name: "Other → AV1", current: otherSize, saving: otherSaving, count: otherFiles.length, fill: "#f97316" },
  ].filter(d => d.count > 0);

  const alreadyAV1 = files.filter(f => f.video?.codec_raw === "av1");
  const av1Size = alreadyAV1.reduce((s, f) => s + f.file_size_gb, 0);

  return (
    <div>
      <SectionTitle>Estimated AV1 Conversion Savings</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <StatCard label="Current Library" value={fmt(currentTotal)} />
        <StatCard label="Est. After AV1" value={fmt(currentTotal - totalSaving)} colour={PALETTE.green} />
        <StatCard label="Est. Savings" value={fmt(totalSaving)} sub={currentTotal > 0 ? `~${((totalSaving / currentTotal) * 100).toFixed(0)}% reduction` : ""} colour={PALETTE.accentWarm} />
        <StatCard label="Already AV1" value={fmtNum(alreadyAV1.length)} sub={fmt(av1Size)} colour={PALETTE.purple} />
      </div>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>Savings Breakdown</div>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={data} layout="vertical" margin={{ left: 100, right: 20, top: 5, bottom: 5 }}>
            <XAxis type="number" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={(v) => fmt(v)} />
            <YAxis dataKey="name" type="category" tick={{ fill: PALETTE.text, fontSize: 12 }} axisLine={false} tickLine={false} width={90} />
            <Tooltip
              contentStyle={{ background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`, borderRadius: 8, color: PALETTE.text, fontSize: 13 }}
              formatter={(v, name) => [fmt(v), name === "current" ? "Current Size" : "Est. Saving"]}
            />
            <Bar dataKey="current" fill={PALETTE.border} radius={[0, 4, 4, 0]} name="Current Size" />
            <Bar dataKey="saving" radius={[0, 4, 4, 0]} name="Est. Saving">
              {data.map((d, i) => <Cell key={i} fill={d.fill} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 12, fontStyle: "italic" }}>
          Estimates assume CRF-based encoding. H.264→AV1 ~55% saving, HEVC→AV1 ~25% saving. Actual results vary with content.
        </div>
      </div>
    </div>
  );
}

/** Estimate per-file AV1 saving in GB using the same ratios as SavingsEstimate. */
function estimateSaving(f) {
  const codec = f.video?.codec_raw;
  if (codec === "av1") return 0;
  const size = f.file_size_gb;
  if (f.video?.codec === "H.264") return size * 0.55;
  if (f.video?.codec === "HEVC (H.265)") return size * 0.25;
  return size * 0.6; // other non-AV1
}

function getFolder(filepath) {
  // For series: group by show folder (2 levels up from file), e.g. Z:\Series\Bluey (2018)
  // For movies: parent folder, e.g. Z:\Movies\300 (2007)
  const parts = filepath.replace(/\//g, "\\").split("\\");
  // Remove filename
  parts.pop();
  // For series paths like Z:\Series\Show\Season X, go up one more
  if (parts.length >= 3 && /^season\s/i.test(parts[parts.length - 1])) {
    parts.pop();
  }
  return parts.join("\\");
}

function TopFolders({ files, limit = 15 }) {
  const folders = {};
  for (const f of files) {
    const folder = getFolder(f.filepath);
    if (!folders[folder]) folders[folder] = { path: folder, size_gb: 0, saving_gb: 0, count: 0 };
    folders[folder].size_gb += f.file_size_gb;
    folders[folder].saving_gb += estimateSaving(f);
    folders[folder].count += 1;
  }
  const sorted = Object.values(folders).sort((a, b) => b.size_gb - a.size_gb).slice(0, limit);
  const maxSize = sorted[0]?.size_gb || 1;
  const folderLabel = (p) => p.split("\\").pop() || p;

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 8 }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {sorted.map((f, i) => {
          const afterSize = f.size_gb - f.saving_gb;
          return (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12 }}>
              <span style={{ color: PALETTE.textMuted, width: 24, textAlign: "right", fontFamily: "'JetBrains Mono', monospace" }}>{i + 1}</span>
              <div style={{ flex: 1, position: "relative", height: 26, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
                {/* Current size bar */}
                <div style={{
                  position: "absolute", left: 0, top: 0, bottom: 0,
                  width: `${(f.size_gb / maxSize) * 100}%`,
                  background: PALETTE.accent,
                  opacity: 0.2,
                  borderRadius: 4,
                }} />
                {/* Estimated post-AV1 bar */}
                <div style={{
                  position: "absolute", left: 0, top: 0, bottom: 0,
                  width: `${(afterSize / maxSize) * 100}%`,
                  background: PALETTE.green,
                  opacity: 0.35,
                  borderRadius: 4,
                }} />
                <div style={{ position: "relative", padding: "4px 8px", color: PALETTE.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {folderLabel(f.path)}
                </div>
              </div>
              <span style={{ color: PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace", minWidth: 60, textAlign: "right" }}>{fmt(f.size_gb)}</span>
              {f.saving_gb > 0.01 && (
                <span style={{ color: PALETTE.green, fontFamily: "'JetBrains Mono', monospace", fontSize: 11, minWidth: 60, textAlign: "right" }}>-{fmt(f.saving_gb)}</span>
              )}
              <span style={{ color: PALETTE.textMuted, fontSize: 11, minWidth: 50, textAlign: "right" }}>{f.count} file{f.count !== 1 ? "s" : ""}</span>
            </div>
          );
        })}
      </div>
      <div style={{ display: "flex", gap: 16, marginTop: 12, fontSize: 11, color: PALETTE.textMuted }}>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.accent, opacity: 0.3, marginRight: 4, verticalAlign: "middle" }} />Current</span>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.green, opacity: 0.5, marginRight: 4, verticalAlign: "middle" }} />Est. after AV1</span>
      </div>
    </div>
  );
}

export function LibraryPage() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [tab, setTab] = useState("all");
  const [scanning, setScanning] = useState(false);

  const loadReport = useCallback(() => {
    api.getMediaReport()
      .then(setData)
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => { loadReport(); }, [loadReport]);

  const [scanLogs, setScanLogs] = useState([]);

  // Poll scanner status + logs while scanning, refetch report when done
  useEffect(() => {
    if (!scanning) return;
    const id = setInterval(async () => {
      try {
        const [s, logs] = await Promise.all([
          api.getProcessStatus("scanner"),
          api.getProcessLogs("scanner", 30),
        ]);
        setScanLogs(logs.lines || []);
        if (s.status !== "running") {
          setScanning(false);
          setScanLogs([]);
          loadReport();
        }
      } catch { /* ignore */ }
    }, 2000);
    return () => clearInterval(id);
  }, [scanning, loadReport]);

  const handleRescan = async () => {
    setScanning(true);
    try {
      await api.startProcess("scanner");
    } catch {
      setScanning(false);
    }
  };

  if (error) {
    return (
      <div style={{ padding: 40, textAlign: "center" }}>
        <div style={{ color: PALETTE.red, fontSize: 16, marginBottom: 8 }}>Failed to load media report</div>
        <div style={{ color: PALETTE.textMuted, fontSize: 13 }}>{error}</div>
        <button
          onClick={handleRescan}
          disabled={scanning}
          style={{
            marginTop: 16, background: scanning ? PALETTE.surfaceLight : PALETTE.accent,
            color: scanning ? PALETTE.textMuted : "#fff",
            border: "none", borderRadius: 8, padding: "10px 20px",
            fontSize: 14, fontWeight: 600,
            cursor: scanning ? "default" : "pointer",
          }}
        >
          {scanning ? "Scanning..." : "Scan Library"}
        </button>
      </div>
    );
  }

  if (!data) {
    return <div style={{ padding: 40, textAlign: "center", color: PALETTE.textMuted }}>Loading media report...</div>;
  }

  const { summary } = data;
  const allFiles = data.files || [];
  const files = tab === "all" ? allFiles : allFiles.filter((f) => f.library_type === tab);

  const codecData = aggregate(files, (f) => f.video?.codec);
  const resData = aggregate(files, (f) => f.video?.resolution_class);
  const hdrFiles = files.filter((f) => f.video?.hdr);

  return (
    <div>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 24 }}>
        <div style={{ color: PALETTE.textMuted, fontSize: 12 }}>
          Scanned {summary.scan_date?.slice(0, 10)} · {fmtNum(summary.total_files)} files · {fmt(summary.total_size_gb)}
        </div>
        <button
          onClick={handleRescan}
          disabled={scanning}
          style={{
            background: scanning ? PALETTE.surfaceLight : PALETTE.accent,
            color: scanning ? PALETTE.textMuted : "#fff",
            border: "none", borderRadius: 8, padding: "6px 14px",
            fontSize: 12, fontWeight: 600,
            cursor: scanning ? "default" : "pointer",
          }}
        >
          {scanning ? "Scanning..." : "Rescan"}
        </button>
      </div>

      {/* Scanner logs while scanning */}
      {scanning && scanLogs.length > 0 && (
        <div style={{
          background: PALETTE.surface, border: `1px solid ${PALETTE.border}`,
          borderRadius: 8, padding: "12px 16px", marginBottom: 16,
          fontFamily: "'JetBrains Mono', monospace", fontSize: 11,
          maxHeight: 180, overflowY: "auto", color: PALETTE.textMuted,
        }}>
          {scanLogs.map((line, i) => (
            <div key={i} style={{ whiteSpace: "pre-wrap", lineHeight: 1.5 }}>{line}</div>
          ))}
        </div>
      )}

      {/* Tabs */}
      <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
        <TabButton active={tab === "all"} onClick={() => setTab("all")}>All ({fmtNum(allFiles.length)})</TabButton>
        <TabButton active={tab === "movie"} onClick={() => setTab("movie")}>Movies ({fmtNum(summary.movies?.count || 0)})</TabButton>
        <TabButton active={tab === "series"} onClick={() => setTab("series")}>Series ({fmtNum(summary.series?.count || 0)})</TabButton>
      </div>

      {/* Summary Cards */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 24 }}>
        <StatCard label="Total Files" value={fmtNum(files.length)} />
        <StatCard label="Total Size" value={fmt(files.reduce((s, f) => s + f.file_size_gb, 0))} />
        <StatCard label="HDR Content" value={fmtNum(hdrFiles.length)} sub={files.length > 0 ? `${((hdrFiles.length / files.length) * 100).toFixed(1)}%` : ""} colour={PALETTE.purple} />
        <StatCard label="Avg File Size" value={files.length > 0 ? fmt(files.reduce((s, f) => s + f.file_size_gb, 0) / files.length) : "—"} />
      </div>

      {/* Codec & Resolution pies */}
      <SectionTitle>Video Codecs &amp; Resolution</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 16, marginBottom: 8 }}>
        <PieSection data={codecData} colourFn={getCodecColour} title="Video Codec (by storage)" />
        <PieSection data={resData} colourFn={getResColour} title="Resolution (by storage)" />
      </div>

      {/* Bitrate */}
      <SectionTitle>Bitrate Distribution</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 16 }}>
        <BitrateDistribution files={files} title="Overall Bitrate (Mbps)" />
      </div>

      {/* Audio */}
      <AudioAnalysis files={files} />

      {/* Filename Health */}
      <FilenameHealth files={files} onReload={loadReport} />

      {/* Savings */}
      <SavingsEstimate files={files} />

      {/* Largest folders */}
      <SectionTitle>Largest Folders</SectionTitle>
      <TopFolders files={files} limit={15} />

      {/* Largest files */}
      <SectionTitle>Largest Files</SectionTitle>
      <TopFiles files={files} title="Top 15 by file size" />
    </div>
  );
}
