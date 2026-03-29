import { useState, useEffect } from "react";
import { PALETTE } from "../theme";
import { api } from "../lib/api";

function fmt(bytes) {
  if (!bytes) return "—";
  if (bytes >= 1024 ** 4) return `${(bytes / 1024 ** 4).toFixed(2)} TB`;
  if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(0)} MB`;
  return `${(bytes / 1024).toFixed(0)} KB`;
}

function fmtDur(secs) {
  if (!secs || secs <= 0) return "—";
  if (secs < 60) return `${Math.round(secs)}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function Row({ label, value, colour }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", padding: "6px 0", borderBottom: `1px solid ${PALETTE.border}22` }}>
      <span style={{ color: PALETTE.textMuted, fontSize: 12 }}>{label}</span>
      <span style={{ color: colour || PALETTE.text, fontSize: 12, fontFamily: "'JetBrains Mono', monospace" }}>{value}</span>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 20 }}>
      <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 8, borderBottom: `1px solid ${PALETTE.border}`, paddingBottom: 4 }}>{title}</div>
      {children}
    </div>
  );
}

function vmafColour(score) {
  if (score >= 93) return PALETTE.green;
  if (score >= 85) return PALETTE.accentWarm;
  return PALETTE.red;
}

export function FileDrawer({ path, onClose }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [vmaf, setVmaf] = useState(null);
  const [vmafLoading, setVmafLoading] = useState(false);
  const [forced, setForced] = useState(null); // null = loading, true/false = known

  useEffect(() => {
    if (!path) return;
    setLoading(true);
    setForced(null);
    api.getFileDetail(path).then(setData).catch(() => setData(null)).finally(() => setLoading(false));
    api.getPriority().then((p) => {
      const norm = path.replace(/\//g, "\\").toLowerCase();
      setForced((p?.force || []).some((f) => f.replace(/\//g, "\\").toLowerCase() === norm));
    }).catch(() => setForced(false));
  }, [path]);

  if (!path) return null;

  const media = data?.media;
  const pipeline = data?.pipeline;
  const video = media?.video || {};

  return (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        onKeyDown={(e) => e.key === "Escape" && onClose()}
        style={{
          position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)",
          zIndex: 200, cursor: "pointer",
        }}
      />

      {/* Drawer */}
      <div style={{
        position: "fixed", top: 0, right: 0, bottom: 0, width: 420,
        background: PALETTE.surface, borderLeft: `1px solid ${PALETTE.border}`,
        zIndex: 201, overflowY: "auto", padding: 24,
        boxShadow: "-4px 0 20px rgba(0,0,0,0.3)",
      }}>
        {/* Header */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
          <div style={{ fontSize: 15, fontWeight: 600, color: PALETTE.text, wordBreak: "break-word", flex: 1 }}>
            {media?.filename || path.split(/[\\/]/).pop()}
          </div>
          <button onClick={onClose} style={{
            background: "none", border: "none", color: PALETTE.textMuted, fontSize: 20,
            cursor: "pointer", padding: "0 0 0 12px", lineHeight: 1,
          }}>×</button>
        </div>

        {loading && <div style={{ color: PALETTE.textMuted }}>Loading...</div>}

        {!loading && !data && <div style={{ color: PALETTE.textMuted }}>File not found in report or state.</div>}

        {!loading && data && (
          <>
            {/* File info */}
            {media && (
              <Section title="File Info">
                <Row label="Size" value={fmt(media.file_size_bytes)} />
                <Row label="Duration" value={media.duration_display || fmtDur(media.duration_seconds)} />
                <Row label="Bitrate" value={media.overall_bitrate_kbps ? `${Math.round(media.overall_bitrate_kbps)} kbps` : "—"} />
                <Row label="Library" value={media.library_type} />
              </Section>
            )}

            {/* Video */}
            {video.codec && (
              <Section title="Video">
                <Row label="Codec" value={video.codec} />
                <Row label="Resolution" value={`${video.width}×${video.height} (${video.resolution_class})`} />
                <Row label="HDR" value={video.hdr ? "Yes" : "No"} colour={video.hdr ? PALETTE.accentWarm : PALETTE.textMuted} />
                <Row label="Bit depth" value={`${video.bit_depth}-bit`} />
                <Row label="Pixel format" value={video.pixel_format} />
                {video.bitrate_kbps && <Row label="Video bitrate" value={`${Math.round(video.bitrate_kbps)} kbps`} />}
              </Section>
            )}

            {/* Audio */}
            {media?.audio_streams?.length > 0 && (
              <Section title={`Audio (${media.audio_streams.length} streams)`}>
                {media.audio_streams.map((a, i) => (
                  <div key={i} style={{ marginBottom: 8, padding: "6px 8px", background: PALETTE.bg, borderRadius: 6, fontSize: 12 }}>
                    <div style={{ color: PALETTE.text, fontWeight: 500 }}>{a.codec} · {a.channels}ch · {a.language}</div>
                    {a.bitrate_kbps && <div style={{ color: PALETTE.textMuted }}>{Math.round(a.bitrate_kbps)} kbps{a.lossless ? " (lossless)" : ""}</div>}
                    {a.title && <div style={{ color: PALETTE.textMuted, fontSize: 11 }}>{a.title}</div>}
                  </div>
                ))}
              </Section>
            )}

            {/* Subtitles */}
            {media?.subtitle_streams?.length > 0 && (
              <Section title={`Subtitles (${media.subtitle_streams.length})`}>
                {media.subtitle_streams.map((s, i) => (
                  <Row key={i} label={s.language} value={`${s.codec}${s.title ? ` — ${s.title}` : ""}`} />
                ))}
              </Section>
            )}

            {/* Pipeline state */}
            {pipeline && (
              <Section title="Encode Status">
                <Row label="Status" value={pipeline.status} colour={
                  pipeline.status === "replaced" ? PALETTE.green :
                  pipeline.status === "error" ? PALETTE.red : PALETTE.accent
                } />
                {pipeline.encode_time_secs > 0 && <Row label="Encode time" value={fmtDur(pipeline.encode_time_secs)} />}
                {pipeline.fetch_time_secs > 0 && <Row label="Fetch time" value={fmtDur(pipeline.fetch_time_secs)} />}
                {pipeline.upload_time_secs > 0 && <Row label="Upload time" value={fmtDur(pipeline.upload_time_secs)} />}
                {pipeline.compression_ratio > 0 && <Row label="Compression" value={`${pipeline.compression_ratio}%`} colour={PALETTE.green} />}
                {pipeline.bytes_saved > 0 && <Row label="Saved" value={fmt(pipeline.bytes_saved)} colour={PALETTE.green} />}
                {pipeline.output_size_bytes > 0 && <Row label="Output size" value={fmt(pipeline.output_size_bytes)} />}
                {pipeline.input_size_bytes > 0 && <Row label="Input size" value={fmt(pipeline.input_size_bytes)} />}
                {pipeline.tier && <Row label="Tier" value={pipeline.tier} />}
                {pipeline.error && <Row label="Error" value={pipeline.error} colour={PALETTE.red} />}
                {pipeline.last_updated && <Row label="Last updated" value={new Date(pipeline.last_updated).toLocaleString()} />}
              </Section>
            )}

            {/* VMAF Quality Check */}
            {pipeline && ["verified", "replaced"].includes(pipeline.status) && (
              <Section title="Quality Check (VMAF)">
                {vmaf ? (
                  <>
                    <Row label="VMAF Mean" value={vmaf.vmaf_mean} colour={vmafColour(vmaf.vmaf_mean)} />
                    <Row label="VMAF Min" value={vmaf.vmaf_min} colour={vmafColour(vmaf.vmaf_min)} />
                    <Row label="VMAF Max" value={vmaf.vmaf_max} />
                    <Row label="Segment" value={`${vmaf.duration_tested}s at ${vmaf.offset_secs}s`} />
                  </>
                ) : (
                  <button
                    onClick={async () => {
                      setVmafLoading(true);
                      try {
                        const result = await api.vmafCheck(path);
                        setVmaf(result);
                      } catch { setVmaf({ error: "VMAF check failed" }); }
                      setVmafLoading(false);
                    }}
                    disabled={vmafLoading}
                    style={{
                      background: PALETTE.purple, color: "#fff", border: "none", borderRadius: 8,
                      padding: "8px 16px", fontSize: 12, fontWeight: 600, cursor: "pointer",
                      opacity: vmafLoading ? 0.6 : 1, width: "100%",
                    }}
                  >
                    {vmafLoading ? "Running VMAF..." : "Check Quality (30s sample)"}
                  </button>
                )}
                {vmaf?.error && <div style={{ color: PALETTE.red, fontSize: 12, marginTop: 4 }}>{vmaf.error}</div>}
              </Section>
            )}

            {/* Force Next */}
            {forced !== null && (!pipeline || !["verified", "replaced"].includes(pipeline.status)) && (
              <div style={{ marginBottom: 16 }}>
                <button
                  onClick={async () => {
                    const fn = forced ? api.removeForce : api.addForce;
                    await fn(path);
                    setForced(!forced);
                  }}
                  style={{
                    background: forced ? PALETTE.surface : PALETTE.orange,
                    color: forced ? PALETTE.orange : "#fff",
                    border: forced ? `1px solid ${PALETTE.orange}` : "none",
                    borderRadius: 8, padding: "8px 16px", fontSize: 12,
                    fontWeight: 600, cursor: "pointer", width: "100%",
                  }}
                >
                  {forced ? "Remove Force" : "Force Next"}
                </button>
              </div>
            )}

            {/* Path */}
            <div style={{ fontSize: 11, color: PALETTE.textMuted, wordBreak: "break-all", marginTop: 12, padding: "8px 0", borderTop: `1px solid ${PALETTE.border}` }}>
              {path}
            </div>
          </>
        )}
      </div>
    </>
  );
}
