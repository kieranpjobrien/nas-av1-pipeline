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

      {/* Savings */}
      <SavingsEstimate files={files} />

      {/* Largest files */}
      <SectionTitle>Largest Files</SectionTitle>
      <TopFiles files={files} title="Top 15 by file size" />
    </div>
  );
}
