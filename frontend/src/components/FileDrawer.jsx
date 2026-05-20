import { useState, useEffect } from "react";
import { api } from "../lib/api";

// File details drawer. Styled to match the dashboard's `.nc-app` scope
// (olive/yellow palette, Inter Tight + JetBrains Mono via .mono). Wraps
// itself in `<div className="nc-app">` so the CSS variables and base
// typography from pages/dashboard/dashboard.css apply.

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
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "baseline",
        gap: 12,
        padding: "8px 0",
        borderBottom: "1px solid var(--line)",
      }}
    >
      <span style={{ color: "var(--ink-3)", fontSize: 12 }}>{label}</span>
      <span
        className="mono"
        style={{
          color: colour || "var(--ink)",
          fontSize: 12,
          textAlign: "right",
          wordBreak: "break-word",
        }}
      >
        {value}
      </span>
    </div>
  );
}

function Section({ title, count, children }) {
  return (
    <div style={{ marginBottom: 22 }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          fontSize: 11,
          color: "var(--ink-3)",
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          fontWeight: 500,
          margin: "0 0 10px",
          paddingBottom: 6,
          borderBottom: "1px solid var(--line-strong)",
        }}
      >
        <span>{title}</span>
        {count != null && (
          <span className="mono" style={{ color: "var(--ink-4)", fontSize: 11 }}>
            {count}
          </span>
        )}
      </div>
      {children}
    </div>
  );
}

function vmafColour(score) {
  if (score >= 93) return "var(--good)";
  if (score >= 85) return "var(--warn)";
  return "var(--bad)";
}

function statusColour(status) {
  const s = (status || "").toLowerCase();
  if (s === "done" || s === "replaced" || s === "verified") return "var(--good)";
  if (s === "error" || s.startsWith("flagged")) return "var(--bad)";
  if (s === "processing" || s === "encoding" || s === "fetching" || s === "uploading")
    return "var(--accent)";
  return "var(--ink-2)";
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
    api
      .getFileDetail(path)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
    api
      .getPriority()
      .then((p) => {
        const norm = path.replace(/\//g, "\\").toLowerCase();
        setForced((p?.force || []).some((f) => f.replace(/\//g, "\\").toLowerCase() === norm));
      })
      .catch(() => setForced(false));
  }, [path]);

  // ESC closes — global key handler tied to mount.
  useEffect(() => {
    if (!path) return;
    const onKey = (e) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [path, onClose]);

  if (!path) return null;

  const media = data?.media;
  const pipeline = data?.pipeline;
  const video = media?.video || {};
  const params = pipeline?.encode_params_used;
  const hasCompliance =
    pipeline &&
    (pipeline.compliance_violations?.length > 0 ||
      pipeline.corruption_signatures?.length > 0 ||
      (pipeline.compliance_refuse_count ?? 0) > 0 ||
      (pipeline.integrity_failure_count ?? 0) > 0);

  return (
    <div className="nc-app">
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: "fixed",
          inset: 0,
          background: "rgba(0,0,0,0.45)",
          zIndex: 200,
          cursor: "pointer",
        }}
      />

      {/* Drawer panel */}
      <div
        style={{
          position: "fixed",
          top: 0,
          right: 0,
          bottom: 0,
          width: 440,
          background: "var(--bg-elev)",
          borderLeft: "1px solid var(--line-strong)",
          zIndex: 201,
          overflowY: "auto",
          padding: "22px 24px 28px",
          boxShadow: "-8px 0 24px rgba(0,0,0,0.4)",
          color: "var(--ink)",
          fontFamily: "'Inter Tight', system-ui, sans-serif",
        }}
      >
        {/* Header */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "flex-start",
            gap: 12,
            marginBottom: 18,
          }}
        >
          <div
            style={{
              fontSize: 16,
              fontWeight: 600,
              color: "var(--ink)",
              wordBreak: "break-word",
              flex: 1,
              lineHeight: 1.3,
            }}
          >
            {media?.filename || path.split(/[\\/]/).pop()}
          </div>
          <button
            onClick={onClose}
            title="Close (Esc)"
            style={{
              background: "transparent",
              border: "1px solid var(--line)",
              borderRadius: 6,
              color: "var(--ink-3)",
              fontSize: 16,
              cursor: "pointer",
              padding: "2px 9px",
              lineHeight: 1,
            }}
          >
            ×
          </button>
        </div>

        {loading && (
          <div style={{ color: "var(--ink-3)", fontSize: 12 }}>Loading…</div>
        )}

        {!loading && !data && (
          <div style={{ color: "var(--ink-3)", fontSize: 12 }}>
            File not found in report or state.
          </div>
        )}

        {!loading && data && (
          <>
            {/* File info */}
            {media && (
              <Section title="File">
                <Row label="Size" value={fmt(media.file_size_bytes)} />
                <Row
                  label="Duration"
                  value={media.duration_display || fmtDur(media.duration_seconds)}
                />
                <Row
                  label="Bitrate"
                  value={
                    media.overall_bitrate_kbps
                      ? `${Math.round(media.overall_bitrate_kbps)} kbps`
                      : "—"
                  }
                />
                <Row label="Library" value={media.library_type} />
              </Section>
            )}

            {/* Video */}
            {video.codec && (
              <Section title="Video">
                <Row label="Codec" value={video.codec} />
                <Row
                  label="Resolution"
                  value={`${video.width}×${video.height} (${video.resolution_class})`}
                />
                <Row
                  label="HDR"
                  value={video.hdr ? "Yes" : "No"}
                  colour={video.hdr ? "var(--warn)" : "var(--ink-3)"}
                />
                <Row label="Bit depth" value={`${video.bit_depth}-bit`} />
                <Row label="Pixel format" value={video.pixel_format} />
                {video.bitrate_kbps && (
                  <Row
                    label="Video bitrate"
                    value={`${Math.round(video.bitrate_kbps)} kbps`}
                  />
                )}
              </Section>
            )}

            {/* Audio */}
            {media?.audio_streams?.length > 0 && (
              <Section title="Audio" count={`${media.audio_streams.length} streams`}>
                {media.audio_streams.map((a, i) => (
                  <div
                    key={i}
                    style={{
                      marginBottom: 8,
                      padding: "8px 10px",
                      background: "var(--bg-card)",
                      border: "1px solid var(--line)",
                      borderRadius: 6,
                    }}
                  >
                    <div className="mono" style={{ fontSize: 12, color: "var(--ink)" }}>
                      {a.codec} · {a.channels}ch · {a.language}
                      {a.lossless ? " · lossless" : ""}
                    </div>
                    {a.bitrate_kbps && (
                      <div
                        className="mono"
                        style={{ fontSize: 11, color: "var(--ink-3)", marginTop: 2 }}
                      >
                        {Math.round(a.bitrate_kbps)} kbps
                      </div>
                    )}
                    {a.title && (
                      <div
                        style={{
                          fontSize: 11,
                          color: "var(--ink-3)",
                          marginTop: 2,
                          wordBreak: "break-word",
                        }}
                      >
                        {a.title}
                      </div>
                    )}
                  </div>
                ))}
              </Section>
            )}

            {/* Subtitles */}
            {media?.subtitle_streams?.length > 0 && (
              <Section title="Subtitles" count={media.subtitle_streams.length}>
                {media.subtitle_streams.map((s, i) => (
                  <Row
                    key={i}
                    label={s.language || "—"}
                    value={`${s.codec}${s.title ? ` — ${s.title}` : ""}`}
                  />
                ))}
              </Section>
            )}

            {/* Pipeline state */}
            {pipeline && (
              <Section title="Encode status">
                <Row
                  label="Status"
                  value={pipeline.status}
                  colour={statusColour(pipeline.status)}
                />
                {pipeline.stage && <Row label="Stage" value={pipeline.stage} />}
                {pipeline.mode && <Row label="Mode" value={pipeline.mode} />}
                {pipeline.progress_pct != null && (
                  <Row
                    label="Progress"
                    colour="var(--accent)"
                    value={`${pipeline.progress_pct}%${pipeline.speed ? ` · ${pipeline.speed}` : ""}${pipeline.fps ? ` · ${pipeline.fps} fps` : ""}`}
                  />
                )}
                {pipeline.eta_text && (
                  <Row label="ETA" value={pipeline.eta_text} colour="var(--accent)" />
                )}
                {pipeline.encode_time_secs > 0 && (
                  <Row label="Encode time" value={fmtDur(pipeline.encode_time_secs)} />
                )}
                {pipeline.fetch_time_secs > 0 && (
                  <Row label="Fetch time" value={fmtDur(pipeline.fetch_time_secs)} />
                )}
                {pipeline.upload_time_secs > 0 && (
                  <Row label="Upload time" value={fmtDur(pipeline.upload_time_secs)} />
                )}
                {pipeline.compression_ratio > 0 && (
                  <Row
                    label="Compression"
                    value={`${pipeline.compression_ratio}%`}
                    colour="var(--good)"
                  />
                )}
                {pipeline.bytes_saved > 0 && (
                  <Row
                    label="Saved"
                    value={fmt(pipeline.bytes_saved)}
                    colour="var(--good)"
                  />
                )}
                {pipeline.output_size_bytes > 0 && (
                  <Row label="Output size" value={fmt(pipeline.output_size_bytes)} />
                )}
                {pipeline.input_size_bytes > 0 && (
                  <Row label="Input size" value={fmt(pipeline.input_size_bytes)} />
                )}
                {pipeline.tier && <Row label="Tier" value={pipeline.tier} />}
                {pipeline.reason && <Row label="Reason" value={pipeline.reason} />}
                {pipeline.error && (
                  <Row label="Error" value={pipeline.error} colour="var(--bad)" />
                )}
                {pipeline.last_updated && (
                  <Row
                    label="Last updated"
                    value={new Date(pipeline.last_updated).toLocaleString()}
                  />
                )}
              </Section>
            )}

            {/* Encoder settings */}
            {params && (
              <Section title="Encoder settings">
                {params.cq != null && <Row label="CQ" value={String(params.cq)} />}
                {params.content_grade && (
                  <Row label="Grade" value={params.content_grade} />
                )}
                {params.res_key && <Row label="Tier" value={params.res_key} />}
                {params.preset && <Row label="NVENC preset" value={params.preset} />}
                {params.multipass && <Row label="Multipass" value={params.multipass} />}
                {params.maxrate && <Row label="Max bitrate" value={params.maxrate} />}
                {params.lookahead != null && (
                  <Row label="Lookahead" value={String(params.lookahead)} />
                )}
              </Section>
            )}

            {/* Compliance / corruption */}
            {hasCompliance && (
              <Section title="Compliance / corruption">
                {(pipeline.compliance_refuse_count ?? 0) > 0 && (
                  <Row
                    label="Compliance refuses"
                    value={String(pipeline.compliance_refuse_count)}
                    colour={
                      pipeline.compliance_refuse_count >= 3 ? "var(--bad)" : "var(--warn)"
                    }
                  />
                )}
                {(pipeline.integrity_failure_count ?? 0) > 0 && (
                  <Row
                    label="Integrity failures"
                    value={String(pipeline.integrity_failure_count)}
                    colour={
                      pipeline.integrity_failure_count >= 3 ? "var(--bad)" : "var(--warn)"
                    }
                  />
                )}
                {pipeline.compliance_violations?.length > 0 && (
                  <div style={{ marginTop: 10 }}>
                    <div
                      style={{
                        color: "var(--ink-3)",
                        fontSize: 11,
                        marginBottom: 6,
                        letterSpacing: "0.08em",
                        textTransform: "uppercase",
                      }}
                    >
                      Violations
                    </div>
                    {pipeline.compliance_violations.map((v, i) => (
                      <div
                        key={i}
                        className="mono"
                        style={{
                          fontSize: 11,
                          color: "var(--bad)",
                          padding: "6px 8px",
                          background: "var(--bg-card)",
                          border: "1px solid var(--line)",
                          borderRadius: 5,
                          marginBottom: 4,
                          wordBreak: "break-word",
                        }}
                      >
                        {String(v).slice(0, 280)}
                      </div>
                    ))}
                  </div>
                )}
                {pipeline.corruption_signatures?.length > 0 && (
                  <div style={{ marginTop: 10 }}>
                    <div
                      style={{
                        color: "var(--ink-3)",
                        fontSize: 11,
                        marginBottom: 6,
                        letterSpacing: "0.08em",
                        textTransform: "uppercase",
                      }}
                    >
                      Decoder errors
                    </div>
                    {pipeline.corruption_signatures.map((v, i) => (
                      <div
                        key={i}
                        className="mono"
                        style={{
                          fontSize: 11,
                          color: "var(--bad)",
                          padding: "6px 8px",
                          background: "var(--bg-card)",
                          border: "1px solid var(--line)",
                          borderRadius: 5,
                          marginBottom: 4,
                          wordBreak: "break-word",
                        }}
                      >
                        {String(v).slice(0, 280)}
                      </div>
                    ))}
                  </div>
                )}
              </Section>
            )}

            {/* VMAF Quality Check */}
            {pipeline && ["verified", "replaced", "done"].includes(pipeline.status) && (
              <Section title="Quality check (VMAF)">
                {vmaf && !vmaf.error ? (
                  <>
                    <Row
                      label="VMAF mean"
                      value={vmaf.vmaf_mean}
                      colour={vmafColour(vmaf.vmaf_mean)}
                    />
                    <Row
                      label="VMAF min"
                      value={vmaf.vmaf_min}
                      colour={vmafColour(vmaf.vmaf_min)}
                    />
                    <Row label="VMAF max" value={vmaf.vmaf_max} />
                    <Row
                      label="Segment"
                      value={`${vmaf.duration_tested}s at ${vmaf.offset_secs}s`}
                    />
                  </>
                ) : (
                  <button
                    onClick={async () => {
                      setVmafLoading(true);
                      try {
                        const result = await api.vmafCheck(path);
                        setVmaf(result);
                      } catch {
                        setVmaf({ error: "VMAF check failed" });
                      }
                      setVmafLoading(false);
                    }}
                    disabled={vmafLoading}
                    style={{
                      width: "100%",
                      background: "var(--bg-card)",
                      color: "var(--ink)",
                      border: "1px solid var(--line-strong)",
                      borderRadius: 6,
                      padding: "9px 14px",
                      fontSize: 12,
                      fontWeight: 500,
                      cursor: vmafLoading ? "default" : "pointer",
                      opacity: vmafLoading ? 0.6 : 1,
                      fontFamily: "inherit",
                    }}
                  >
                    {vmafLoading ? "Running VMAF…" : "Check quality (30s sample)"}
                  </button>
                )}
                {vmaf?.error && (
                  <div style={{ color: "var(--bad)", fontSize: 12, marginTop: 6 }}>
                    {vmaf.error}
                  </div>
                )}
              </Section>
            )}

            {/* Force Next */}
            {forced !== null &&
              (!pipeline || !["verified", "replaced", "done"].includes(pipeline.status)) && (
                <div style={{ marginTop: 16, marginBottom: 16 }}>
                  <button
                    onClick={async () => {
                      const fn = forced ? api.removeForce : api.addForce;
                      await fn(path);
                      setForced(!forced);
                    }}
                    style={{
                      width: "100%",
                      background: forced ? "var(--bg-card)" : "var(--accent)",
                      color: forced ? "var(--accent)" : "var(--bg)",
                      border: forced ? "1px solid var(--accent)" : "1px solid var(--accent)",
                      borderRadius: 6,
                      padding: "10px 16px",
                      fontSize: 12,
                      fontWeight: 600,
                      cursor: "pointer",
                      letterSpacing: "0.02em",
                      fontFamily: "inherit",
                    }}
                  >
                    {forced ? "Remove force-next" : "Force next"}
                  </button>
                </div>
              )}

            {/* Delete file from NAS — destructive, confirm-then-act.
                Added 2026-05-20: drawer was view-only before, so the user
                had no way to remove flagged_corrupt sources like
                Miami Vice or Once Upon a Time in America without poking
                the DB directly. Calls /api/file/delete which removes
                the on-disk file AND prunes the media-report cache. */}
            <div style={{ marginTop: 8, marginBottom: 16 }}>
              <button
                onClick={async () => {
                  const name = media?.filename || path.split(/[\\/]/).pop();
                  if (
                    !window.confirm(
                      `Permanently delete this file from NAS?\n\n${name}\n\nThis cannot be undone.`
                    )
                  )
                    return;
                  try {
                    await api.deleteFile(path);
                    onClose();
                  } catch (e) {
                    window.alert(`Delete failed: ${e?.message || e}`);
                  }
                }}
                style={{
                  width: "100%",
                  background: "transparent",
                  color: "var(--bad)",
                  border: "1px solid var(--bad)",
                  borderRadius: 6,
                  padding: "9px 14px",
                  fontSize: 12,
                  fontWeight: 500,
                  cursor: "pointer",
                  fontFamily: "inherit",
                  letterSpacing: "0.02em",
                }}
              >
                Delete file from NAS
              </button>
            </div>

            {/* Path footer */}
            <div
              className="mono"
              style={{
                fontSize: 11,
                color: "var(--ink-4)",
                wordBreak: "break-all",
                marginTop: 14,
                paddingTop: 12,
                borderTop: "1px solid var(--line)",
              }}
            >
              {path}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
