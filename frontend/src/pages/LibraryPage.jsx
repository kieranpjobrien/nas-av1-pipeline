import { useState, useEffect, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";
import { PALETTE, getCodecColour, getResColour, getAudioColour } from "../theme";
import { fmt, fmtNum, fmtHrs, aggregate } from "../lib/format";
import { api } from "../lib/api";
import { usePolling } from "../lib/usePolling";
import { StatCard } from "../components/StatCard";
import { SectionTitle } from "../components/SectionTitle";
import { PieSection } from "../components/PieSection";
import { TabButton } from "../components/TabButton";
import { TopFiles } from "../components/TopFiles";

function BitrateDistribution({ files, title, onBarClick, activeBar }) {
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
    min: b.min,
    max: b.max,
    count: files.filter((f) => {
      const br = f.overall_bitrate_kbps;
      return br != null && br >= b.min && br < b.max;
    }).length,
    size_gb: files.filter((f) => {
      const br = f.overall_bitrate_kbps;
      return br != null && br >= b.min && br < b.max;
    }).reduce((s, f) => s + f.file_size_gb, 0),
  }));

  const handleClick = (state) => {
    if (onBarClick && state?.activePayload?.[0]?.payload) {
      const d = state.activePayload[0].payload;
      onBarClick({ label: d.name, min: d.min, max: d.max });
    }
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, flex: "1 1 400px", minWidth: 340 }}>
      <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>{title}</div>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}
          onClick={onBarClick ? handleClick : undefined}
          style={onBarClick ? { cursor: "pointer" } : undefined}
        >
          <XAxis dataKey="name" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} axisLine={{ stroke: PALETTE.border }} tickLine={false} label={{ value: "Mbps", position: "insideBottomRight", offset: -5, fill: PALETTE.textMuted, fontSize: 11 }} />
          <YAxis tick={{ fill: PALETTE.textMuted, fontSize: 11 }} axisLine={false} tickLine={false} />
          <Tooltip
            contentStyle={{ background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`, borderRadius: 8, color: PALETTE.text, fontSize: 13 }}
            formatter={(v, name) => [name === "size_gb" ? fmt(v) : fmtNum(v), name === "size_gb" ? "Size" : "Files"]}
          />
          <Bar dataKey="count" radius={[4, 4, 0, 0]} name="Files">
            {data.map((d, i) => (
              <Cell key={i} fill={PALETTE.accent} opacity={activeBar && activeBar !== d.name ? 0.3 : 1} />
            ))}
          </Bar>
          <Bar dataKey="size_gb" radius={[4, 4, 0, 0]} name="Size (GB)">
            {data.map((d, i) => (
              <Cell key={i} fill={PALETTE.purple} opacity={activeBar && activeBar !== d.name ? 0.3 : 1} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function AudioAnalysis({ files, onAudioClick, activeAudio }) {
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
        <PieSection data={codecData} colourFn={getAudioColour} title="Primary Audio Codec (by size)"
          onSegmentClick={onAudioClick} activeSegment={activeAudio}
        />
      </div>
    </div>
  );
}

const FILENAME_PATTERNS = [
  { key: "dots", label: "Dot-separated", test: (stem) => (stem.match(/\./g) || []).length >= 3 },
  { key: "resolution", label: "Resolution tags", test: (stem) => /\b(1080p|720p|480p|2160p|4K|UHD)\b/i.test(stem) },
  { key: "source", label: "Source tags", test: (stem) => /\b(WEB[-.]?DL|WEBRip|BluRay|BDRip|HDTV|DVDRip|REMUX)\b/i.test(stem) },
  { key: "codec", label: "Codec tags", test: (stem) => /\b(x264|x265|H\.?264|H\.?265|HEVC|AVC)\b/i.test(stem) },
  { key: "group", label: "Release groups", test: (stem) => {
    // Bracket at end: flag only if content has digits, technical keywords, or is ALL-CAPS (case-sensitive)
    const endBracket = stem.match(/\[([A-Za-z0-9.-]+)\]\s*$/);
    if (endBracket) {
      const inner = endBracket[1];
      if (/[0-9]|x26[45]|rip|web|blu/i.test(inner)) return true; // tech content
      if (/^[A-Z0-9]{2,}$/.test(inner)) return true;              // ALL-CAPS only (case-sensitive, e.g. YIFY, NTb)
    }
    // Bracket with tech content anywhere in name
    return /\[[-A-Za-z0-9.]*(?:x264|x265|WEB|BluRay|HDTV|DL|Rip|720|1080|2160)[-A-Za-z0-9.]*\]/i.test(stem);
  }},
  { key: "service", label: "Service tags", test: (stem) => /\b(AMZN|DSNP|HULU|HBO|ATVP|PCOK|PMTP)\b/i.test(stem) || /\bMAX\b/.test(stem) },
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

function DuplicateGroups({ onReload, onFileClick }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(null);
  const [deleting, setDeleting] = useState(null); // filepath being deleted
  const [confirmDelete, setConfirmDelete] = useState(null); // filepath awaiting confirmation

  const load = () => {
    setLoading(true);
    api.getDuplicates().then(setData).catch(() => setData(null)).finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  if (loading && !data) return (
    <div>
      <SectionTitle>Duplicates</SectionTitle>
      <div style={{ color: PALETTE.textMuted, padding: 20 }}>Scanning for duplicates...</div>
    </div>
  );
  if (!data || data.total_groups === 0) return null;

  const handleDelete = async (filepath) => {
    setDeleting(filepath);
    try {
      await api.deleteFile(filepath);
      setConfirmDelete(null);
      load(); // refresh
      onReload?.(); // refresh media report too
    } catch (e) {
      alert(`Delete failed: ${e.message}`);
    }
    setDeleting(null);
  };

  const handleDeleteGroup = async (group) => {
    const toDelete = group.members.filter((m) => !m.keep);
    for (const m of toDelete) {
      setDeleting(m.filepath);
      try { await api.deleteFile(m.filepath); } catch { /* continue */ }
    }
    setDeleting(null);
    setConfirmDelete(null);
    load();
    onReload?.();
  };

  const mono = { fontFamily: "'JetBrains Mono', monospace" };

  return (
    <div>
      <SectionTitle>Duplicates</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <StatCard label="Duplicate Groups" value={fmtNum(data.total_groups)} colour={PALETTE.accentWarm} />
        <StatCard label="Extra Files" value={fmtNum(data.total_dupes)} sub="could be deleted" />
        <StatCard label="Wasted Space" value={fmt(data.wasted_gb)} colour={PALETTE.red} />
      </div>

      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16 }}>
        <div style={{ color: PALETTE.text, fontSize: 14, fontWeight: 600, marginBottom: 12 }}>
          {data.total_groups} groups ({data.total_dupes} extra files, {fmt(data.wasted_gb)} reclaimable)
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {data.groups.map((g) => {
            const isOpen = expanded === g.group_id;
            return (
              <div key={g.group_id}>
                <div
                  onClick={() => setExpanded(isOpen ? null : g.group_id)}
                  style={{
                    display: "flex", justifyContent: "space-between", alignItems: "center",
                    padding: "8px 10px", cursor: "pointer", borderRadius: 6,
                    background: isOpen ? PALETTE.surfaceLight : "transparent",
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>{isOpen ? "v" : ">"}</span>
                    <span style={{ color: PALETTE.text, fontSize: 13 }}>{g.title}</span>
                    <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>({g.members.length} copies)</span>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <span style={{ ...mono, color: PALETTE.red, fontSize: 11 }}>{fmt(g.wasted_gb)} wasted</span>
                  </div>
                </div>
                {isOpen && (
                  <div style={{ padding: "8px 10px 12px 28px" }}>
                    {g.members.map((m) => (
                      <div key={m.filepath} style={{
                        display: "flex", alignItems: "center", gap: 8, fontSize: 12,
                        padding: "6px 8px", borderRadius: 4, marginBottom: 2,
                        background: m.keep ? `${PALETTE.green}11` : "transparent",
                      }}>
                        <span
                          onClick={() => onFileClick?.(m.filepath)}
                          style={{ color: PALETTE.text, flex: 1, cursor: "pointer", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                          title={m.filepath}
                        >
                          {m.filename}
                        </span>
                        <span style={{ ...mono, color: PALETTE.textMuted, fontSize: 11, width: 50, textAlign: "right" }}>{fmt(m.file_size_gb)}</span>
                        <span style={{ ...mono, color: PALETTE.textMuted, fontSize: 11, width: 70, textAlign: "right" }}>{m.codec}</span>
                        <span style={{ ...mono, color: PALETTE.textMuted, fontSize: 11, width: 40, textAlign: "right" }}>{m.resolution}</span>
                        <span style={{ ...mono, color: PALETTE.accent, fontSize: 11, width: 30, textAlign: "right" }}>{m.score}</span>
                        {m.keep ? (
                          <span style={{
                            background: PALETTE.green, color: "#000", fontSize: 10, fontWeight: 700,
                            padding: "2px 6px", borderRadius: 4, width: 36, textAlign: "center",
                          }}>KEEP</span>
                        ) : (
                          confirmDelete === m.filepath ? (
                            <span style={{ display: "flex", gap: 4 }}>
                              <button
                                onClick={() => handleDelete(m.filepath)}
                                disabled={deleting === m.filepath}
                                style={{
                                  background: PALETTE.red, color: "#fff", border: "none", borderRadius: 4,
                                  padding: "2px 6px", fontSize: 10, fontWeight: 600, cursor: "pointer",
                                }}
                              >{deleting === m.filepath ? "..." : "Yes"}</button>
                              <button
                                onClick={() => setConfirmDelete(null)}
                                style={{
                                  background: PALETTE.surfaceLight, color: PALETTE.textMuted, border: "none",
                                  borderRadius: 4, padding: "2px 6px", fontSize: 10, cursor: "pointer",
                                }}
                              >No</button>
                            </span>
                          ) : (
                            <button
                              onClick={() => setConfirmDelete(m.filepath)}
                              style={{
                                background: "transparent", color: PALETTE.red, border: `1px solid ${PALETTE.red}44`,
                                borderRadius: 4, padding: "2px 6px", fontSize: 10, fontWeight: 600,
                                cursor: "pointer", width: 36, textAlign: "center",
                              }}
                            >DEL</button>
                          )
                        )}
                      </div>
                    ))}
                    {/* Delete all non-keepers button */}
                    {g.members.filter((m) => !m.keep).length > 1 && (
                      <div style={{ marginTop: 8 }}>
                        {confirmDelete === `group-${g.group_id}` ? (
                          <span style={{ display: "flex", gap: 6, alignItems: "center" }}>
                            <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>Delete {g.members.filter((m) => !m.keep).length} files?</span>
                            <button
                              onClick={() => handleDeleteGroup(g)}
                              disabled={!!deleting}
                              style={{
                                background: PALETTE.red, color: "#fff", border: "none", borderRadius: 4,
                                padding: "4px 10px", fontSize: 11, fontWeight: 600, cursor: "pointer",
                              }}
                            >{deleting ? "Deleting..." : "Confirm"}</button>
                            <button
                              onClick={() => setConfirmDelete(null)}
                              style={{
                                background: PALETTE.surfaceLight, color: PALETTE.textMuted, border: "none",
                                borderRadius: 4, padding: "4px 10px", fontSize: 11, cursor: "pointer",
                              }}
                            >Cancel</button>
                          </span>
                        ) : (
                          <button
                            onClick={() => setConfirmDelete(`group-${g.group_id}`)}
                            style={{
                              background: "transparent", color: PALETTE.red, border: `1px solid ${PALETTE.red}44`,
                              borderRadius: 6, padding: "4px 12px", fontSize: 11, fontWeight: 600, cursor: "pointer",
                            }}
                          >Delete All Lower ({g.members.filter((m) => !m.keep).length} files, {fmt(g.wasted_gb)})</button>
                        )}
                      </div>
                    )}
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

const EN_TOKENS = new Set(["eng", "en", "english"]);

function hasEnglishSubs(file) {
  for (const s of file.subtitle_streams || []) {
    const lang = (s.language || "und").toLowerCase();
    if (EN_TOKENS.has(lang)) return true;
    const title = (s.title || "").toLowerCase();
    if ([...EN_TOKENS].some((t) => title.includes(t))) return true;
  }
  return false;
}

function SubtitleHealth({ files, onFileClick }) {
  const noSubs = files.filter((f) => (f.subtitle_count || 0) === 0);
  const noEnglish = files.filter((f) => f.subtitle_count > 0 && !hasEnglishSubs(f));
  const total = noSubs.length + noEnglish.length;
  if (total === 0) return null;

  return (
    <>
      <SectionTitle>Subtitle Health</SectionTitle>
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
        <StatCard label="No Subtitles" value={noSubs.length} colour={PALETTE.red} sub={`${(100 * noSubs.length / files.length).toFixed(1)}% of library`} />
        <StatCard label="No English Subs" value={noEnglish.length} colour={PALETTE.accentWarm} sub="Has subs but not English" />
        <StatCard label="OK" value={files.length - total} colour={PALETTE.green} />
      </div>
      {total > 0 && (
        <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16, marginBottom: 24, maxHeight: 300, overflow: "auto" }}>
          {noSubs.slice(0, 30).map((f) => (
            <div key={f.filepath} onClick={() => onFileClick?.(f.filepath)} style={{ padding: "4px 0", fontSize: 12, color: PALETTE.textMuted, cursor: onFileClick ? "pointer" : "default" }}>
              <span style={{ color: PALETTE.red, marginRight: 8 }}>NO SUBS</span>
              {f.filename}
            </div>
          ))}
          {noEnglish.slice(0, 20).map((f) => (
            <div key={f.filepath} onClick={() => onFileClick?.(f.filepath)} style={{ padding: "4px 0", fontSize: 12, color: PALETTE.textMuted, cursor: onFileClick ? "pointer" : "default" }}>
              <span style={{ color: PALETTE.accentWarm, marginRight: 8 }}>NO ENG</span>
              {f.filename}
              <span style={{ marginLeft: 8, color: PALETTE.border }}>
                ({(f.subtitle_streams || []).map((s) => s.language).filter(Boolean).join(", ")})
              </span>
            </div>
          ))}
          {total > 50 && <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 8 }}>...and {total - 50} more</div>}
        </div>
      )}
    </>
  );
}

function PlexDrillDown({ titles, label, onClose }) {
  if (!titles?.length) return null;
  return (
    <div style={{
      background: PALETTE.bg, border: `1px solid ${PALETTE.accent}44`,
      borderRadius: 10, padding: 14, marginTop: 10,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
        <span style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600 }}>{label} — {fmtNum(titles.length)}</span>
        <button onClick={onClose} style={{
          background: "transparent", border: `1px solid ${PALETTE.border}`, borderRadius: 5,
          color: PALETTE.textMuted, padding: "2px 8px", fontSize: 11, cursor: "pointer",
        }}>✕</button>
      </div>
      <div style={{ maxHeight: 280, overflowY: "auto", columns: titles.length > 20 ? 2 : 1, columnGap: 16 }}>
        {titles.map((t) => (
          <div key={t} style={{ color: PALETTE.textMuted, fontSize: 12, padding: "2px 0", breakInside: "avoid" }}>{t}</div>
        ))}
      </div>
    </div>
  );
}

function PlexMetadataPanel({ s }) {
  const [drill, setDrill] = useState(null); // { label, titles }

  const isMovie = s.section_type !== "show";
  const totalLabel = isMovie ? "Total Movies" : "Total Shows";
  const totalCount = isMovie ? s.total_movies : s.total_shows;

  const topGenres = Object.entries(s.genres || {}).sort((a, b) => b[1] - a[1]).slice(0, 15);
  const topCollections = Object.entries(s.collections || {}).sort((a, b) => b[1] - a[1]).slice(0, 15);
  const topRatings = Object.entries(s.content_ratings || {}).sort((a, b) => b[1] - a[1]);

  const clickRow = (label, titles) => {
    if (drill?.label === label) { setDrill(null); return; }
    setDrill({ label, titles: titles || [] });
  };

  const rowStyle = (active) => ({
    display: "flex", justifyContent: "space-between", fontSize: 12,
    padding: "3px 6px", borderRadius: 4, cursor: "pointer",
    background: active ? PALETTE.accent + "22" : "transparent",
    transition: "background 0.1s",
  });

  return (
    <div style={{ marginBottom: 24 }}>
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 16 }}>
        <StatCard label={totalLabel} value={fmtNum(totalCount)} />
        <StatCard
          label="Unrated" value={fmtNum(s.unrated_count)}
          colour={s.unrated_count > 0 ? PALETTE.accentWarm : PALETTE.green}
          onClick={() => clickRow("Unrated", s.unrated_titles)}
        />
        <StatCard
          label="No Genre" value={fmtNum(s.no_genre_count)}
          colour={s.no_genre_count > 0 ? PALETTE.accentWarm : PALETTE.green}
          onClick={() => clickRow("No Genre", s.no_genre_titles)}
        />
        {isMovie && (
          <StatCard label="No Collection" value={fmtNum(s.no_collection_count)} />
        )}
      </div>

      <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
        {/* Content Ratings */}
        <div style={{ flex: "1 1 260px", background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16 }}>
          <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Content Ratings</div>
          {topRatings.map(([r, c]) => {
            const active = drill?.label === `Rating: ${r}`;
            return (
              <div
                key={r}
                style={rowStyle(active)}
                onClick={() => clickRow(`Rating: ${r}`, s.rating_titles?.[r])}
                onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = PALETTE.surfaceLight; }}
                onMouseLeave={(e) => { e.currentTarget.style.background = active ? PALETTE.accent + "22" : "transparent"; }}
              >
                <span style={{ color: r === "(unrated)" ? PALETTE.accentWarm : PALETTE.textMuted }}>{r}</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", color: PALETTE.textMuted }}>{c}</span>
              </div>
            );
          })}
        </div>

        {/* Top Genres */}
        <div style={{ flex: "1 1 260px", background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16 }}>
          <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Top Genres</div>
          {topGenres.map(([g, c]) => {
            const active = drill?.label === `Genre: ${g}`;
            return (
              <div
                key={g}
                style={rowStyle(active)}
                onClick={() => clickRow(`Genre: ${g}`, s.genre_titles?.[g])}
                onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = PALETTE.surfaceLight; }}
                onMouseLeave={(e) => { e.currentTarget.style.background = active ? PALETTE.accent + "22" : "transparent"; }}
              >
                <span style={{ color: PALETTE.textMuted }}>{g}</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", color: PALETTE.textMuted }}>{c}</span>
              </div>
            );
          })}
        </div>

        {/* Top Collections */}
        <div style={{ flex: "1 1 260px", background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16 }}>
          <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 8 }}>Top Collections</div>
          {topCollections.map(([c, n]) => {
            const active = drill?.label === `Collection: ${c}`;
            return (
              <div
                key={c}
                style={rowStyle(active)}
                onClick={() => clickRow(`Collection: ${c}`, s.collection_titles?.[c])}
                onMouseEnter={(e) => { if (!active) e.currentTarget.style.background = PALETTE.surfaceLight; }}
                onMouseLeave={(e) => { e.currentTarget.style.background = active ? PALETTE.accent + "22" : "transparent"; }}
              >
                <span style={{ color: PALETTE.textMuted }}>{c}</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", color: PALETTE.textMuted }}>{n}</span>
              </div>
            );
          })}
        </div>
      </div>

      {drill && (
        <PlexDrillDown
          label={drill.label}
          titles={drill.titles}
          onClose={() => setDrill(null)}
        />
      )}
    </div>
  );
}

function PlexMetadata() {
  const [audit, setAudit] = useState(null);
  useEffect(() => {
    api.getPlexAudit().then(setAudit).catch(() => {});
  }, []);

  if (!audit || !audit.sections?.length) return null;

  const movieSections = audit.sections.filter((s) => s.section_type === "movie" || !s.section_type);
  const showSections = audit.sections.filter((s) => s.section_type === "show");

  return (
    <>
      {movieSections.length > 0 && (
        <>
          <SectionTitle>Plex Metadata — Movies</SectionTitle>
          {movieSections.map((s, idx) => <PlexMetadataPanel key={idx} s={s} />)}
        </>
      )}
      {showSections.length > 0 && (
        <>
          <SectionTitle>Plex Metadata — Series</SectionTitle>
          {showSections.map((s, idx) => <PlexMetadataPanel key={idx} s={s} />)}
        </>
      )}
    </>
  );
}

// Expected bitrate ranges per resolution/codec (Mbps)
const EXPECTED_BITRATE = {
  "H.264": { "4K": [15, 50], "1080p": [3, 20], "720p": [1.5, 10], "480p": [0.5, 5], "SD": [0.3, 3] },
  "HEVC (H.265)": { "4K": [8, 35], "1080p": [2, 15], "720p": [1, 8], "480p": [0.3, 4], "SD": [0.2, 2] },
  "AV1": { "4K": [5, 25], "1080p": [1.5, 10], "720p": [0.5, 5], "480p": [0.2, 3], "SD": [0.1, 2] },
};

function BitrateEfficiency({ files, onFileClick }) {
  const bloated = [];
  for (const f of files) {
    const codec = f.video?.codec;
    const res = f.video?.resolution_class;
    const bitrate = f.overall_bitrate_kbps;
    if (!codec || !res || !bitrate) continue;
    const expected = EXPECTED_BITRATE[codec]?.[res];
    if (!expected) continue;
    const mbps = bitrate / 1000;
    if (mbps > expected[1]) {
      bloated.push({ ...f, mbps: mbps.toFixed(1), expected_max: expected[1] });
    }
  }

  if (bloated.length === 0) return null;

  bloated.sort((a, b) => b.mbps - a.mbps);

  return (
    <>
      <SectionTitle>Bloated Files ({bloated.length})</SectionTitle>
      <div style={{ color: PALETTE.textMuted, fontSize: 12, marginBottom: 12 }}>
        Files with bitrate above expected range for their codec and resolution — high-value encode targets.
      </div>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16, marginBottom: 24, maxHeight: 400, overflow: "auto" }}>
        {bloated.slice(0, 40).map((f) => (
          <div key={f.filepath} onClick={() => onFileClick?.(f.filepath)} style={{
            display: "flex", justifyContent: "space-between", alignItems: "center",
            padding: "6px 0", fontSize: 12, borderBottom: `1px solid ${PALETTE.border}22`,
            cursor: onFileClick ? "pointer" : "default",
          }}>
            <span style={{ color: PALETTE.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginRight: 12 }}>
              {f.filename}
            </span>
            <span style={{ color: PALETTE.textMuted, whiteSpace: "nowrap", fontFamily: "'JetBrains Mono', monospace" }}>
              <span style={{ color: PALETTE.red }}>{f.mbps} Mbps</span>
              <span style={{ margin: "0 6px", color: PALETTE.border }}>|</span>
              {f.video?.codec} {f.video?.resolution_class}
              <span style={{ margin: "0 6px", color: PALETTE.border }}>|</span>
              {fmt(f.file_size_gb)}
            </span>
          </div>
        ))}
        {bloated.length > 40 && <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 8 }}>...and {bloated.length - 40} more</div>}
      </div>
    </>
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

function AudioSavingsEstimate({ files }) {
  // Gather all audio streams with their file-level estimated size contribution
  const streamEntries = [];
  files.forEach((f) => {
    const streams = f.audio_streams || [];
    const totalBitrate = streams.reduce((s, a) => s + (a.bitrate_kbps || 0), 0);
    streams.forEach((a) => {
      const share = totalBitrate > 0 ? (a.bitrate_kbps || 0) / totalBitrate : 1 / streams.length;
      streamEntries.push({ ...a, size_gb: (f.audio_estimated_size_gb || 0) * share });
    });
  });

  const totalAudioGB = streamEntries.reduce((s, a) => s + a.size_gb, 0);

  // Categorise each stream and estimate savings
  const categories = {
    "Lossless → EAC-3": { codecs: new Set(), size: 0, saving: 0, count: 0, fill: PALETTE.purple },
    "DTS → EAC-3": { codecs: new Set(), size: 0, saving: 0, count: 0, fill: PALETTE.accent },
    "AC-3 (high)": { codecs: new Set(), size: 0, saving: 0, count: 0, fill: PALETTE.accentWarm },
    "Already efficient": { codecs: new Set(), size: 0, saving: 0, count: 0, fill: PALETTE.green },
  };

  for (const a of streamEntries) {
    const codec = (a.codec || "").toUpperCase();
    const br = a.bitrate_kbps || 0;

    if (a.lossless) {
      // Lossless (TrueHD, FLAC, PCM, DTS-HD MA) → EAC-3: ~80% reduction
      const cat = categories["Lossless → EAC-3"];
      cat.codecs.add(a.codec);
      cat.size += a.size_gb;
      cat.saving += a.size_gb * 0.80;
      cat.count++;
    } else if (codec.includes("DTS") && br > 700) {
      // DTS core (1536kbps typical) → EAC-3 640kbps: ~58% reduction
      const cat = categories["DTS → EAC-3"];
      cat.codecs.add(a.codec);
      cat.size += a.size_gb;
      cat.saving += a.size_gb * 0.58;
      cat.count++;
    } else if ((codec.includes("AC-3") || codec.includes("AC3")) && br > 400) {
      // High-bitrate AC-3 (640kbps) → EAC-3 at lower rate: ~35% reduction
      const cat = categories["AC-3 (high)"];
      cat.codecs.add(a.codec);
      cat.size += a.size_gb;
      cat.saving += a.size_gb * 0.35;
      cat.count++;
    } else {
      // AAC, Opus, low-bitrate AC-3, EAC-3, MP3 — already efficient
      const cat = categories["Already efficient"];
      cat.codecs.add(a.codec);
      cat.size += a.size_gb;
      cat.count++;
    }
  }

  const totalSaving = Object.values(categories).reduce((s, c) => s + c.saving, 0);
  const efficientSize = categories["Already efficient"].size;

  const chartData = Object.entries(categories)
    .filter(([, c]) => c.count > 0)
    .map(([name, c]) => ({
      name: `${name}`,
      current: c.size,
      saving: c.saving,
      count: c.count,
      fill: c.fill,
      detail: [...c.codecs].join(", "),
    }));

  // Non-English audio stats
  const KEEP_LANGS = new Set(["eng", "und", ""]);
  let foreignStreams = 0;
  let foreignEstGB = 0;
  files.forEach((f) => {
    const streams = f.audio_streams || [];
    if (streams.length <= 1) return; // single-track files: always keep
    const totalBr = streams.reduce((s, a) => s + (a.bitrate_kbps || 0), 0);
    streams.forEach((a) => {
      const lang = (a.language || "").toLowerCase().trim();
      if (!KEEP_LANGS.has(lang)) {
        foreignStreams++;
        const share = totalBr > 0 ? (a.bitrate_kbps || 0) / totalBr : 1 / streams.length;
        foreignEstGB += (f.audio_estimated_size_gb || 0) * share;
      }
    });
  });

  return (
    <div>
      <SectionTitle>Estimated Audio Re-encoding Savings</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <StatCard label="Current Audio" value={fmt(totalAudioGB)} />
        <StatCard label="Est. After Re-encode" value={fmt(totalAudioGB - totalSaving)} colour={PALETTE.green} />
        <StatCard label="Est. Savings" value={fmt(totalSaving)} sub={totalAudioGB > 0 ? `~${((totalSaving / totalAudioGB) * 100).toFixed(0)}% reduction` : ""} colour={PALETTE.accentWarm} />
        <StatCard label="Already Efficient" value={fmt(efficientSize)} sub={`${categories["Already efficient"].count} streams`} colour={PALETTE.purple} />
        <StatCard label="Foreign Audio" value={fmtNum(foreignStreams)} sub={foreignEstGB > 0.01 ? `~${fmt(foreignEstGB)} stripped on encode` : "stripped on encode"} colour={PALETTE.textMuted} />
      </div>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20 }}>
        <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>Savings Breakdown</div>
        <ResponsiveContainer width="100%" height={Math.max(160, chartData.length * 50)}>
          <BarChart data={chartData} layout="vertical" margin={{ left: 130, right: 20, top: 5, bottom: 5 }}>
            <XAxis type="number" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={(v) => fmt(v)} />
            <YAxis dataKey="name" type="category" tick={{ fill: PALETTE.text, fontSize: 12 }} axisLine={false} tickLine={false} width={120} />
            <Tooltip
              contentStyle={{ background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`, borderRadius: 8, color: PALETTE.text, fontSize: 13 }}
              formatter={(v, name, props) => {
                const label = name === "current" ? "Current Size" : "Est. Saving";
                return [fmt(v), label];
              }}
              labelFormatter={(label) => {
                const item = chartData.find((d) => d.name === label);
                return item ? `${label} (${item.detail})` : label;
              }}
            />
            <Bar dataKey="current" fill={PALETTE.border} radius={[0, 4, 4, 0]} name="Current Size" />
            <Bar dataKey="saving" radius={[0, 4, 4, 0]} name="Est. Saving">
              {chartData.map((d, i) => <Cell key={i} fill={d.fill} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
        <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 12, fontStyle: "italic" }}>
          Estimates only. Lossless→EAC-3 ~80%, DTS→EAC-3 ~58%, high-bitrate AC-3 ~35%. Already efficient streams (AAC, Opus, EAC-3, low-bitrate) are not re-encoded.
        </div>
      </div>
    </div>
  );
}

function SubtitleAnalysis({ files }) {
  // Subtitle bitrate isn't reported by ffprobe, so estimate from codec type + duration
  const CODEC_KBPS = {
    hdmv_pgs_subtitle: 40,   // image-based PGS, ~40 kbps avg
    dvd_subtitle:      30,   // image-based VOBSUB
    subrip:            0.3,  // SRT text — negligible
    ass:               0.3,
    mov_text:          0.3,
    unknown:           5,
  };
  const KEEP_LANGS = new Set(["eng", "und", ""]);

  let imageSubs = 0, imageEstGB = 0;
  let textSubs = 0, textEstGB = 0;
  let foreignSubs = 0, foreignEstGB = 0;
  const codecCounts = {};

  files.forEach((f) => {
    const dur = f.duration_seconds || 0;
    (f.subtitle_streams || []).forEach((s) => {
      const codec = s.codec || "unknown";
      const lang = (s.language || "").toLowerCase().trim();
      const kbps = CODEC_KBPS[codec] ?? 5;
      const estGB = (kbps * 1000 * dur) / 8 / 1e9;
      codecCounts[codec] = (codecCounts[codec] || 0) + 1;

      const isImage = codec === "hdmv_pgs_subtitle" || codec === "dvd_subtitle";
      if (isImage) { imageSubs++; imageEstGB += estGB; }
      else { textSubs++; textEstGB += estGB; }

      if (!KEEP_LANGS.has(lang)) { foreignSubs++; foreignEstGB += estGB; }
    });
  });

  const totalSubs = imageSubs + textSubs;
  if (totalSubs === 0) return null;

  const codecRows = Object.entries(codecCounts).sort((a, b) => b[1] - a[1]);

  return (
    <>
      <SectionTitle>Subtitle Tracks</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 16 }}>
        <StatCard label="Total Streams" value={fmtNum(totalSubs)} />
        <StatCard label="Image-based (PGS/DVD)" value={fmtNum(imageSubs)} sub={`~${fmt(imageEstGB)} est.`} colour={PALETTE.accentWarm} />
        <StatCard label="Text-based (SRT/ASS)" value={fmtNum(textSubs)} sub="~negligible size" colour={PALETTE.green} />
        <StatCard label="Foreign Language" value={fmtNum(foreignSubs)} sub={`~${fmt(foreignEstGB)} stripped on encode`} colour={PALETTE.textMuted} />
      </div>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 16 }}>
        <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 10 }}>By Codec</div>
        {codecRows.map(([codec, count]) => {
          const kbps = CODEC_KBPS[codec] ?? 5;
          const isImage = codec === "hdmv_pgs_subtitle" || codec === "dvd_subtitle";
          return (
            <div key={codec} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 12, padding: "3px 0", borderBottom: `1px solid ${PALETTE.border}22` }}>
              <span style={{ color: PALETTE.text, fontFamily: "'JetBrains Mono', monospace" }}>{codec}</span>
              <span style={{ display: "flex", gap: 16, color: PALETTE.textMuted }}>
                <span>{fmtNum(count)} streams</span>
                <span style={{ color: isImage ? PALETTE.accentWarm : PALETTE.textMuted }}>{isImage ? `~${kbps} kbps/stream` : "text (~0)"}</span>
              </span>
            </div>
          );
        })}
        <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 10, fontStyle: "italic" }}>
          PGS/DVD subtitle sizes are estimates (~40/30 kbps avg). Text-based (SRT, ASS) are negligible. Pipeline strips non-English subs on encode.
        </div>
      </div>
    </>
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

const FCHK = { width: 13, height: 13, cursor: "pointer", margin: 0 };

function TopFolders({ files, limit = 15 }) {
  const [dismissed, setDismissed] = useState([]);
  const [priority, setPriority] = useState(new Set());
  const [gentle, setGentle] = useState(new Set());
  const [reencode, setReencode] = useState({}); // folderPath -> cq
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api.getDismissed("folders").then(setDismissed).catch(() => {});
  }, []);

  // Build folder map with file lists for path resolution
  const folderMap = {};
  for (const f of files) {
    const folder = getFolder(f.filepath);
    if (!folderMap[folder]) folderMap[folder] = { path: folder, size_gb: 0, saving_gb: 0, count: 0, duration_hrs: 0, filepaths: [] };
    folderMap[folder].size_gb += f.file_size_gb;
    folderMap[folder].saving_gb += estimateSaving(f);
    folderMap[folder].count += 1;
    folderMap[folder].duration_hrs += (f.duration_seconds || 0) / 3600;
    folderMap[folder].filepaths.push(f.filepath);
  }
  const sorted = Object.values(folderMap)
    .filter((f) => !dismissed.includes(f.path))
    .sort((a, b) => b.size_gb - a.size_gb).slice(0, limit);
  const maxSize = sorted[0]?.size_gb || 1;
  const folderLabel = (p) => p.split("\\").pop() || p;
  const mono = { fontFamily: "'JetBrains Mono', monospace" };
  const hasActions = priority.size > 0 || gentle.size > 0 || Object.keys(reencode).length > 0;

  const togglePri = (path) => { const n = new Set(priority); n.has(path) ? n.delete(path) : n.add(path); setPriority(n); };
  const toggleGen = (path) => { const n = new Set(gentle); n.has(path) ? n.delete(path) : n.add(path); setGentle(n); };
  const toggleRe = (f) => {
    const n = { ...reencode };
    if (n[f.path]) { delete n[f.path]; } else { n[f.path] = 30; }
    setReencode(n);
  };
  const setReCQ = (path, val) => setReencode((prev) => ({ ...prev, [path]: val }));

  const getFilePaths = (folderPaths) => {
    const paths = [];
    for (const fp of folderPaths) { if (folderMap[fp]) paths.push(...folderMap[fp].filepaths); }
    return paths;
  };

  const submit = async () => {
    setBusy(true);
    const toDismiss = new Set();
    try {
      if (priority.size > 0) {
        const filePaths = getFilePaths([...priority]);
        const current = await api.getPriority().catch(() => ({ paths: [] }));
        await api.setPriority([...new Set([...(current.paths || []), ...filePaths])]);
        priority.forEach((p) => toDismiss.add(p));
      }
      if (gentle.size > 0) {
        const filePaths = getFilePaths([...gentle]);
        const current = await api.getGentle().catch(() => ({ paths: {}, patterns: {}, default_offset: 0 }));
        const updated = { ...current, paths: { ...current.paths } };
        for (const p of filePaths) updated.paths[p] = { cq_offset: 2 };
        await api.setGentle(updated);
        gentle.forEach((p) => toDismiss.add(p));
      }
      if (Object.keys(reencode).length > 0) {
        const current = await api.getReencode().catch(() => ({ files: {}, patterns: {} }));
        const updatedFiles = { ...(current.files || {}) };
        for (const [folderPath, cq] of Object.entries(reencode)) {
          const filePaths = getFilePaths([folderPath]);
          for (const fp of filePaths) updatedFiles[fp] = { cq };
          toDismiss.add(folderPath);
        }
        await api.setReencode(updatedFiles, current.patterns || {});
      }
      if (toDismiss.size > 0) {
        const next = [...dismissed, ...toDismiss];
        setDismissed(next);
        try { await api.setDismissed("folders", next); } catch {}
      }
    } catch { /* ignore */ }
    setPriority(new Set()); setGentle(new Set()); setReencode({});
    setBusy(false);
  };

  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 8 }}>
      {dismissed.length > 0 && (
        <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 8 }}>
          <button
            onClick={async () => { setDismissed([]); try { await api.setDismissed("folders", []); } catch {} }}
            style={{ background: "none", border: "none", color: PALETTE.textMuted, fontSize: 11, cursor: "pointer", textDecoration: "underline" }}
          >
            Reset {dismissed.length} dismissed
          </button>
        </div>
      )}
      <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, color: PALETTE.textMuted, marginBottom: 2 }}>
          <span style={{ width: 20 }} />
          <span style={{ flex: "0 1 40%" }}>Folder</span>
          <span style={{ ...mono, width: 55, textAlign: "right" }}>Size</span>
          <span style={{ ...mono, width: 50, textAlign: "right" }}>Saving</span>
          <span style={{ ...mono, width: 40, textAlign: "right" }}>Dur</span>
          <span style={{ ...mono, width: 32, textAlign: "right" }}>Files</span>
          <span style={{ width: 16, textAlign: "center", color: PALETTE.green }} title="Priority">⚡</span>
          <span style={{ width: 16, textAlign: "center", color: "#8b8bef" }} title="Gentle +2 CQ">G</span>
          <span style={{ width: 16, textAlign: "center", color: PALETTE.accentWarm }} title="Re-encode">R</span>
          <span style={{ width: 36 }} />
        </div>
        {sorted.map((f, i) => {
          const afterSize = f.size_gb - f.saving_gb;
          const isPri = priority.has(f.path);
          const isGen = gentle.has(f.path);
          const isRe = f.path in reencode;
          const anyAction = isPri || isGen || isRe;
          return (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: 6, fontSize: 12,
              background: anyAction ? `${PALETTE.accent}08` : "transparent",
              borderRadius: 4, padding: "1px 0",
            }}>
              <span style={{ ...mono, color: PALETTE.textMuted, width: 20, textAlign: "right", fontSize: 11 }}>{i + 1}</span>
              <div style={{ flex: "0 1 40%", position: "relative", height: 22, background: PALETTE.surfaceLight, borderRadius: 4, overflow: "hidden" }}>
                <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: `${(f.size_gb / maxSize) * 100}%`, background: PALETTE.accent, opacity: 0.2, borderRadius: 4 }} />
                <div style={{ position: "absolute", left: 0, top: 0, bottom: 0, width: `${(afterSize / maxSize) * 100}%`, background: PALETTE.green, opacity: 0.35, borderRadius: 4 }} />
                <div style={{ position: "relative", padding: "2px 8px", color: PALETTE.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", lineHeight: "18px" }}>
                  {folderLabel(f.path)}
                </div>
              </div>
              <span style={{ ...mono, color: PALETTE.textMuted, width: 55, textAlign: "right", fontSize: 11 }}>{fmt(f.size_gb)}</span>
              {f.saving_gb > 0.01 ? (
                <span style={{ ...mono, color: PALETTE.green, fontSize: 11, width: 50, textAlign: "right" }}>-{fmt(f.saving_gb)}</span>
              ) : ( <span style={{ width: 50 }} /> )}
              <span style={{ ...mono, color: PALETTE.accent, fontSize: 11, width: 40, textAlign: "right" }}>{fmtHrs(f.duration_hrs)}</span>
              <span style={{ ...mono, color: PALETTE.textMuted, fontSize: 11, width: 32, textAlign: "right" }}>{f.count}</span>
              <input type="checkbox" checked={isPri} onChange={() => togglePri(f.path)} title="Priority" style={{ ...FCHK, accentColor: PALETTE.green }} />
              <input type="checkbox" checked={isGen} onChange={() => toggleGen(f.path)} title="Gentle +2 CQ" style={{ ...FCHK, accentColor: "#8b8bef" }} />
              <input type="checkbox" checked={isRe} onChange={() => toggleRe(f)} title="Re-encode" style={{ ...FCHK, accentColor: PALETTE.accentWarm }} />
              {isRe ? (
                <input type="number" value={reencode[f.path]} min={1} max={63}
                  onChange={(e) => setReCQ(f.path, parseInt(e.target.value, 10) || 30)}
                  style={{ ...mono, width: 36, fontSize: 11, padding: "2px 3px", textAlign: "center",
                    background: PALETTE.surfaceLight, color: PALETTE.text,
                    border: `1px solid ${PALETTE.border}`, borderRadius: 3 }}
                  title="CQ value" />
              ) : ( <span style={{ width: 36 }} /> )}
            </div>
          );
        })}
      </div>
      <div style={{ display: "flex", gap: 16, marginTop: 10, fontSize: 11, color: PALETTE.textMuted }}>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.accent, opacity: 0.3, marginRight: 4, verticalAlign: "middle" }} />Current</span>
        <span><span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: PALETTE.green, opacity: 0.5, marginRight: 4, verticalAlign: "middle" }} />Est. after AV1</span>
      </div>

      {hasActions && (
        <div style={{
          marginTop: 12, padding: "8px 14px",
          background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`,
          borderRadius: 8, display: "flex", alignItems: "center", gap: 12,
        }}>
          <span style={{ color: PALETTE.textMuted, fontSize: 12, flex: 1 }}>
            {priority.size > 0 && <span style={{ color: PALETTE.green }}>⚡{priority.size} priority </span>}
            {gentle.size > 0 && <span style={{ color: "#8b8bef" }}>G {gentle.size} gentle </span>}
            {Object.keys(reencode).length > 0 && <span style={{ color: PALETTE.accentWarm }}>R {Object.keys(reencode).length} re-encode </span>}
          </span>
          <button onClick={submit} disabled={busy} style={{
            border: "none", borderRadius: 6, padding: "7px 20px", fontSize: 13, fontWeight: 700,
            cursor: busy ? "default" : "pointer",
            background: busy ? PALETTE.surfaceLight : PALETTE.accent,
            color: busy ? PALETTE.textMuted : "#fff",
          }}>{busy ? "Applying..." : "Apply"}</button>
          <button onClick={() => { setPriority(new Set()); setGentle(new Set()); setReencode({}); }}
            style={{ background: "none", border: "none", color: PALETTE.textMuted, fontSize: 11, cursor: "pointer" }}>Clear</button>
        </div>
      )}
    </div>
  );
}

function StatusBadge({ status }) {
  if (!status) return <span style={{ color: PALETTE.border, fontSize: 10 }}>—</span>;
  const s = status.toLowerCase();
  const map = {
    replaced: { label: "done", color: PALETTE.green },
    verified: { label: "done", color: PALETTE.green },
    encoded: { label: "encoded", color: PALETTE.accent },
    uploaded: { label: "uploaded", color: PALETTE.accent },
    encoding: { label: "encoding", color: PALETTE.accent },
    fetching: { label: "fetching", color: PALETTE.cyan },
    fetched: { label: "fetched", color: PALETTE.cyan },
    uploading: { label: "uploading", color: PALETTE.accent },
    error: { label: "error", color: PALETTE.red },
    failed: { label: "error", color: PALETTE.red },
    pending: { label: "queued", color: PALETTE.textMuted },
    skipped: { label: "skipped", color: PALETTE.textMuted },
  };
  const { label, color } = map[s] || { label: s, color: PALETTE.textMuted };
  return (
    <span style={{
      fontSize: 10, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.05em",
      padding: "2px 6px", borderRadius: 4, background: color + "22", color,
    }}>{label}</span>
  );
}

function FilteredFileList({ files, title, onFileClick, onClose, statusMap }) {
  if (!files || files.length === 0) return null;
  const sorted = [...files].sort((a, b) => b.file_size_bytes - a.file_size_bytes);
  const shown = sorted.slice(0, 100);
  return (
    <div style={{
      background: PALETTE.surface, border: `1px solid ${PALETTE.accent}44`,
      borderRadius: 12, padding: 16, marginTop: 12, marginBottom: 24,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={{ color: PALETTE.text, fontSize: 14, fontWeight: 600 }}>
          {title} — {fmtNum(files.length)} files
        </div>
        <button onClick={onClose} style={{
          background: "transparent", border: `1px solid ${PALETTE.border}`, borderRadius: 6,
          color: PALETTE.textMuted, padding: "4px 10px", fontSize: 11, cursor: "pointer",
        }}>✕ Close</button>
      </div>
      <div style={{ maxHeight: 400, overflow: "auto" }}>
        {shown.map((f) => (
          <div
            key={f.filepath}
            onClick={() => onFileClick?.(f.filepath)}
            style={{
              display: "flex", justifyContent: "space-between", alignItems: "center",
              padding: "6px 4px", borderBottom: `1px solid ${PALETTE.border}22`,
              cursor: "pointer", fontSize: 12, transition: "background 0.1s",
            }}
            onMouseEnter={(e) => e.currentTarget.style.background = PALETTE.surfaceLight}
            onMouseLeave={(e) => e.currentTarget.style.background = "transparent"}
          >
            <span style={{ color: PALETTE.text, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", marginRight: 12 }}>
              {f.filename}
            </span>
            <span style={{ display: "flex", alignItems: "center", gap: 8, whiteSpace: "nowrap", fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>
              <span style={{ color: getCodecColour(f.video?.codec) }}>{f.video?.codec || "?"}</span>
              <span style={{ color: PALETTE.textMuted }}>{f.video?.resolution_class || "?"}</span>
              <span style={{ color: PALETTE.textMuted }}>{fmt(f.file_size_gb)}</span>
              <StatusBadge status={statusMap?.[f.filepath]} />
            </span>
          </div>
        ))}
      </div>
      {files.length > 100 && (
        <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 8, textAlign: "center" }}>
          Showing 100 of {fmtNum(files.length)} — sorted by size
        </div>
      )}
    </div>
  );
}

export function LibraryPage({ onFileClick }) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [tab, setTab] = useState("all");
  const [scanning, setScanning] = useState(false);
  const [search, setSearch] = useState("");
  const [activeFilter, setActiveFilter] = useState(null); // { type, value, label }
  const [statusMap, setStatusMap] = useState({});

  const loadReport = useCallback(() => {
    api.getMediaReport()
      .then(setData)
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => { loadReport(); }, [loadReport]);

  // Fetch pipeline status for file status badges
  useEffect(() => {
    api.getPipeline().then((d) => {
      if (d?.files) {
        const map = {};
        for (const [path, info] of Object.entries(d.files)) {
          map[path] = info.status;
        }
        setStatusMap(map);
      }
    }).catch(() => {});
  }, []);

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

      {/* Search */}
      <div style={{ marginBottom: 20 }}>
        <input
          type="text"
          placeholder={`Search ${fmtNum(files.length)} files...`}
          value={search}
          onChange={(e) => { setSearch(e.target.value); setActiveFilter(null); }}
          style={{
            width: "100%", background: PALETTE.surface, color: PALETTE.text,
            border: `1px solid ${PALETTE.border}`, borderRadius: 8,
            padding: "10px 14px", fontSize: 13,
            fontFamily: "'JetBrains Mono', monospace",
          }}
        />
        {search.length >= 2 && (() => {
          const q = search.toLowerCase();
          const matches = files.filter((f) =>
            f.filename.toLowerCase().includes(q) || f.filepath.toLowerCase().includes(q)
          );
          return (
            <FilteredFileList
              files={matches}
              title={`Search: "${search}"`}
              onFileClick={onFileClick}
              onClose={() => setSearch("")}
              statusMap={statusMap}
            />
          );
        })()}
      </div>

      {/* Summary Cards */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 24 }}>
        <StatCard label="Total Files" value={fmtNum(files.length)} />
        <StatCard label="Total Size" value={fmt(files.reduce((s, f) => s + f.file_size_gb, 0))} />
        <StatCard label="Content Duration" value={(() => {
          const totalSecs = files.reduce((s, f) => s + (f.duration_seconds || 0), 0);
          const days = Math.floor(totalSecs / 86400);
          const hrs = Math.floor((totalSecs % 86400) / 3600);
          if (days > 0) return `${fmtNum(days)}d ${hrs}h`;
          return `${hrs}h`;
        })()} sub={`${fmtNum(Math.round(files.reduce((s, f) => s + (f.duration_seconds || 0), 0) / 3600))} hours`} colour={PALETTE.accent} />
        <StatCard label="HDR Content" value={fmtNum(hdrFiles.length)} sub={files.length > 0 ? `${((hdrFiles.length / files.length) * 100).toFixed(1)}%` : ""} colour={PALETTE.purple} />
        <StatCard label="Avg File Size" value={files.length > 0 ? fmt(files.reduce((s, f) => s + f.file_size_gb, 0) / files.length) : "—"} />
      </div>

      {/* Codec & Resolution pies */}
      <SectionTitle>Video Codecs &amp; Resolution</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 16, marginBottom: 8 }}>
        <PieSection
          data={codecData} colourFn={getCodecColour} title="Video Codec (by storage)"
          onSegmentClick={(name) => setActiveFilter(
            activeFilter?.type === "codec" && activeFilter?.value === name
              ? null : { type: "codec", value: name, label: name }
          )}
          activeSegment={activeFilter?.type === "codec" ? activeFilter.value : null}
        />
        <PieSection
          data={resData} colourFn={getResColour} title="Resolution (by storage)"
          onSegmentClick={(name) => setActiveFilter(
            activeFilter?.type === "resolution" && activeFilter?.value === name
              ? null : { type: "resolution", value: name, label: name }
          )}
          activeSegment={activeFilter?.type === "resolution" ? activeFilter.value : null}
        />
      </div>

      {/* Filtered file list from codec/resolution pie clicks */}
      {activeFilter && ["codec", "resolution"].includes(activeFilter.type) && (() => {
        const filtered = activeFilter.type === "codec"
          ? files.filter((f) => f.video?.codec === activeFilter.value)
          : files.filter((f) => f.video?.resolution_class === activeFilter.value);
        return (
          <FilteredFileList
            files={filtered}
            title={activeFilter.label}
            onFileClick={onFileClick}
            onClose={() => setActiveFilter(null)}
            statusMap={statusMap}
          />
        );
      })()}

      {/* Bitrate */}
      <SectionTitle>Bitrate Distribution</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 16 }}>
        <BitrateDistribution files={files} title="Overall Bitrate (Mbps)"
          onBarClick={(bucket) => setActiveFilter(
            activeFilter?.type === "bitrate" && activeFilter?.value === bucket.label
              ? null : { type: "bitrate", value: bucket.label, min: bucket.min, max: bucket.max, label: `Bitrate: ${bucket.label} Mbps` }
          )}
          activeBar={activeFilter?.type === "bitrate" ? activeFilter.value : null}
        />
      </div>

      {/* Filtered file list from bitrate bar clicks */}
      {activeFilter?.type === "bitrate" && (() => {
        const filtered = files.filter((f) => {
          const br = f.overall_bitrate_kbps;
          return br != null && br >= activeFilter.min && br < activeFilter.max;
        });
        return (
          <FilteredFileList
            files={filtered}
            title={activeFilter.label}
            onFileClick={onFileClick}
            onClose={() => setActiveFilter(null)}
            statusMap={statusMap}
          />
        );
      })()}

      {/* Audio */}
      <AudioAnalysis files={files}
        onAudioClick={(name) => setActiveFilter(
          activeFilter?.type === "audio" && activeFilter?.value === name
            ? null : { type: "audio", value: name, label: `Audio: ${name}` }
        )}
        activeAudio={activeFilter?.type === "audio" ? activeFilter.value : null}
      />

      {/* Filtered file list from audio pie clicks */}
      {activeFilter?.type === "audio" && (() => {
        const filtered = files.filter((f) => f.audio_streams?.[0]?.codec === activeFilter.value);
        return (
          <FilteredFileList
            files={filtered}
            title={activeFilter.label}
            onFileClick={onFileClick}
            onClose={() => setActiveFilter(null)}
            statusMap={statusMap}
          />
        );
      })()}
      <AudioSavingsEstimate files={files} />
      <SubtitleAnalysis files={files} />

      {/* Filename Health */}
      <FilenameHealth files={files} onReload={loadReport} />

      {/* Duplicates */}
      <DuplicateGroups onReload={loadReport} onFileClick={onFileClick} />

      {/* Subtitle health */}
      <SubtitleHealth files={files} onFileClick={onFileClick} />

      {/* Bloated files */}
      <BitrateEfficiency files={files} onFileClick={onFileClick} />

      {/* Plex metadata */}
      <PlexMetadata />

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
