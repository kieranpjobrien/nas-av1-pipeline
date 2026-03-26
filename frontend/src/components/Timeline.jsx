import { PALETTE } from "../theme";

const STAGE_COLOURS = {
  fetch: "#3b82f6",   // blue
  encode: "#8b5cf6",  // purple
  upload: "#06b6d4",  // cyan
  verify: "#10b981",  // green
};

function fmtDur(secs) {
  if (!secs || secs <= 0) return "";
  if (secs < 60) return `${Math.round(secs)}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `${m}m ${Math.round(secs % 60)}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function extractFilename(filepath) {
  if (!filepath) return "";
  const parts = filepath.replace(/\\/g, "/").split("/");
  return parts[parts.length - 1];
}

/**
 * Timeline showing stage durations for recently completed files.
 * Each row is a file, with coloured segments for fetch/encode/upload.
 */
export function Timeline({ files }) {
  if (!files || files.length === 0) return null;

  // Build timeline data from state files that have timing info
  const rows = [];
  for (const [path, info] of Object.entries(files)) {
    if (!info.encode_time_secs) continue;
    // Only show files with at least some timing data
    const fetch = info.fetch_time_secs || 0;
    const encode = info.encode_time_secs || 0;
    const upload = info.upload_time_secs || 0;
    const total = fetch + encode + upload;
    if (total <= 0) continue;

    rows.push({
      path,
      filename: extractFilename(path),
      fetch,
      encode,
      upload,
      total,
      compression: info.compression_ratio,
      saved: info.bytes_saved,
      status: info.status,
    });
  }

  // Only show replaced/verified files, most recent first
  const completed = rows
    .filter((r) => r.status === "replaced" || r.status === "verified")
    .sort((a, b) => b.total - a.total)
    .slice(0, 15);

  if (completed.length === 0) return null;

  const maxTotal = Math.max(...completed.map((r) => r.total));

  return (
    <div>
      <div style={{
        display: "flex", gap: 16, marginBottom: 12,
        fontSize: 11, color: PALETTE.textMuted,
      }}>
        {Object.entries(STAGE_COLOURS).map(([stage, colour]) => (
          <span key={stage} style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <span style={{ width: 10, height: 10, borderRadius: 2, background: colour, display: "inline-block" }} />
            {stage.charAt(0).toUpperCase() + stage.slice(1)}
          </span>
        ))}
      </div>

      {completed.map((row) => (
        <div key={row.path} style={{
          display: "flex", alignItems: "center", gap: 8,
          marginBottom: 4, fontSize: 12,
        }}>
          {/* Filename */}
          <div style={{
            width: 200, minWidth: 200, overflow: "hidden",
            textOverflow: "ellipsis", whiteSpace: "nowrap",
            color: PALETTE.textMuted,
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: 11,
          }} title={row.path}>
            {row.filename}
          </div>

          {/* Bar */}
          <div style={{
            flex: 1, display: "flex", height: 18,
            background: PALETTE.bg, borderRadius: 4, overflow: "hidden",
          }}>
            {row.fetch > 0 && (
              <div
                title={`Fetch: ${fmtDur(row.fetch)}`}
                style={{
                  width: `${(row.fetch / maxTotal) * 100}%`,
                  background: STAGE_COLOURS.fetch,
                  minWidth: row.fetch > 0 ? 2 : 0,
                }}
              />
            )}
            {row.encode > 0 && (
              <div
                title={`Encode: ${fmtDur(row.encode)}`}
                style={{
                  width: `${(row.encode / maxTotal) * 100}%`,
                  background: STAGE_COLOURS.encode,
                  minWidth: row.encode > 0 ? 2 : 0,
                }}
              />
            )}
            {row.upload > 0 && (
              <div
                title={`Upload: ${fmtDur(row.upload)}`}
                style={{
                  width: `${(row.upload / maxTotal) * 100}%`,
                  background: STAGE_COLOURS.upload,
                  minWidth: row.upload > 0 ? 2 : 0,
                }}
              />
            )}
          </div>

          {/* Total time */}
          <div style={{
            width: 70, textAlign: "right",
            fontFamily: "'JetBrains Mono', monospace",
            color: PALETTE.textMuted, fontSize: 11,
          }}>
            {fmtDur(row.total)}
          </div>
        </div>
      ))}
    </div>
  );
}
