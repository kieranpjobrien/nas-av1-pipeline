import { useEffect, useMemo, useState } from "react";
import { api } from "../../lib/api";
import {
  codecCount,
  codecKey,
  codecLabel,
  detectIssues,
  fmtBitrate,
  fmtDur,
  fmtNum,
  fmtSize,
  libraryOf,
  prettyTitle,
  resCount,
  resKey,
} from "./helpers";

const statusLabel = (s) => {
  const k = (s || "").toLowerCase();
  if (["error", "errored", "failed"].includes(k)) return { led: "var(--bad)", label: "error" };
  if (["encoding", "analyzing", "fetching", "uploading", "processing"].includes(k))
    return { led: "var(--accent)", label: k };
  if (["queued", "pending"].includes(k)) return { led: "var(--ink-2)", label: k };
  if (["completed", "done", "encoded"].includes(k)) return { led: "var(--good)", label: "done" };
  return { led: "var(--ink-4)", label: "—" };
};

export function Library({ data, pipelineData, onFileOpen }) {
  const all = data.topTargets;
  const [selIdx, setSelIdx] = useState(0);
  const [query, setQuery] = useState("");
  const [filters, setFilters] = useState({ codec: null, res: null, hdr: false });
  const [panel, setPanel] = useState(null);
  const pipelineFiles = pipelineData?.files || {};

  const rows = useMemo(() => {
    return all.filter((f) => {
      if (query && !`${f.filename} ${f.filepath}`.toLowerCase().includes(query.toLowerCase())) return false;
      if (filters.codec && codecKey(f.codec) !== filters.codec) return false;
      if (filters.res && resKey(f.res) !== filters.res) return false;
      if (filters.hdr && !f.hdr) return false;
      return true;
    });
  }, [all, query, filters]);

  const sel = rows[selIdx] || rows[0] || all[0];

  const toggle = (k, v) => setFilters((f) => ({ ...f, [k]: f[k] === v ? null : v }));

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">
            Library <em>browser</em>
          </div>
          <div className="page-sub">
            Unified view across all {fmtNum(data.summary.total_files)} files. Search by title, filter by codec /
            resolution / HDR / subtitle language. Click any row for full media info and per-file actions.
          </div>
        </div>
        <div className="stamp">
          <div>
            <b>Indexed</b>: {fmtNum(data.summary.total_files)} files
          </div>
          <div>
            <b>Size</b>: {(data.summary.total_size_gb / 1024).toFixed(1)} TB
          </div>
          <div>
            <b>Showing</b>: {fmtNum(rows.length)} of {fmtNum(all.length)}
          </div>
        </div>
      </div>

      <div className="lib-toolbar">
        <div className="search">
          <svg
            width="14"
            height="14"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            style={{ color: "var(--ink-3)" }}
          >
            <circle cx="11" cy="11" r="7" />
            <path d="m20 20-3-3" />
          </svg>
          <input
            placeholder="Search library — filename and path only (metadata not indexed)"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setSelIdx(0);
            }}
          />
          <span className="kbd">⌘K</span>
        </div>
        <button className="chip">Sort: Size ↓</button>
        <button className="chip">Group: —</button>
      </div>

      <div className="facets">
        <div className="facet-group">
          <span className="lbl">Codec</span>
          {["av1", "hevc", "h264"].map((c) => (
            <button
              key={c}
              className={`chip ${filters.codec === c ? "on" : ""}`}
              onClick={() => toggle("codec", c)}
            >
              {c.toUpperCase()} <span className="c">{fmtNum(codecCount(data.codecs, c))}</span>
            </button>
          ))}
        </div>
        <div className="facet-group">
          <span className="lbl">Resolution</span>
          {["4k", "1080p", "720p"].map((r) => (
            <button
              key={r}
              className={`chip ${filters.res === r ? "on" : ""}`}
              onClick={() => toggle("res", r)}
            >
              {r.toUpperCase()} <span className="c">{fmtNum(resCount(data.resolutions, r))}</span>
            </button>
          ))}
        </div>
        <div className="facet-group">
          <span className="lbl">Flags</span>
          <button
            className={`chip ${filters.hdr ? "on" : ""}`}
            onClick={() => setFilters((f) => ({ ...f, hdr: !f.hdr }))}
          >
            HDR
          </button>
          <button className="chip">Atmos</button>
          <button className="chip">Foreign subs only</button>
        </div>
        <div className="facet-group">
          <span className="lbl">Status</span>
          <button className="chip">
            Needs encode <span className="c">{fmtNum(codecCount(data.codecs, "hevc") + codecCount(data.codecs, "h264"))}</span>
          </button>
          <button className="chip">Errored <span className="c">{data.errorCount || 0}</span></button>
        </div>
      </div>

      <div className="lib-layout">
        <div className="file-table">
          <div className="ft-head">
            <span>File</span>
            <span style={{ textAlign: "center" }}>Codec</span>
            <span style={{ textAlign: "center" }}>Res</span>
            <span style={{ textAlign: "right" }}>Size</span>
            <span style={{ textAlign: "right" }}>Bitrate</span>
            <span>Status</span>
          </div>
          {rows.slice(0, 25).map((f, i) => (
            <div
              key={f.filepath || i}
              className={`ft-row ${sel === f ? "sel" : ""}`}
              onClick={() => setSelIdx(i)}
              onDoubleClick={() => onFileOpen?.(f.filepath)}
            >
              <div className="ft-name">
                <div className="n">
                  {prettyTitle(f.filename)} {f.hdr && <span className="tag hdr">HDR</span>}
                </div>
                <div className="p">{f.filepath}</div>
              </div>
              <div style={{ textAlign: "center" }}>
                <span className={`tag ${codecKey(f.codec)}`}>{codecLabel(f.codec)}</span>
              </div>
              <div style={{ textAlign: "center" }}>
                <span className="tag res">{f.res || "—"}</span>
              </div>
              <div className="num">{fmtSize(f.size_gb)}</div>
              <div className="num">{fmtBitrate(f.bitrate)}</div>
              {(() => {
                const info = pipelineFiles[f.filepath];
                const st = statusLabel(info?.status);
                return (
                  <div className="status-cell">
                    <span className="led" style={{ background: st.led }} />
                    {st.label}
                  </div>
                );
              })()}
            </div>
          ))}
          {rows.length === 0 && (
            <div style={{ padding: "40px 20px", textAlign: "center", color: "var(--ink-3)", fontSize: 12 }}>
              No files match the current filters.
            </div>
          )}
        </div>

        {sel && (
          <Inspector
            sel={sel}
            panel={panel}
            setPanel={setPanel}
            onFileOpen={onFileOpen}
          />
        )}
      </div>
    </div>
  );
}

function IssueFlag({ issue }) {
  return (
    <span className={`issue-flag ${issue.level || ""}`} title={issue.why}>
      ⚑ {issue.short}
    </span>
  );
}

function FfprobePanel({ sel, onClose }) {
  const [detail, setDetail] = useState(null);
  const [err, setErr] = useState(null);
  useEffect(() => {
    let cancelled = false;
    setDetail(null);
    setErr(null);
    api
      .getFileDetail(sel.filepath)
      .then((r) => {
        if (!cancelled) setDetail(r);
      })
      .catch((e) => {
        if (!cancelled) setErr(e.message || String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [sel.filepath]);

  const media = detail?.media;
  const pipeline = detail?.pipeline;
  const video = media?.video || {};
  const audios = media?.audio_streams || [];
  const subs = media?.subtitle_streams || [];

  return (
    <div className="ins-panel">
      <div className="ins-panel-head">
        <span className="dollar">$</span>
        <span>/media-report · {sel.filename}</span>
        <span className="x" onClick={onClose}>
          ×
        </span>
      </div>
      <div className="ffprobe-out">
        {err && <div className="warn-line">failed to load: {err}</div>}
        {!detail && !err && <div className="k">loading…</div>}
        {detail && !media && <div className="k">no media-report entry for this file</div>}
        {media && (
          <>
            <div className="section">stream #0 · video</div>
            <div>
              <span className="k">codec_name</span>{" "}
              <span className="v">{video.codec || "?"}</span>
            </div>
            <div>
              <span className="k">profile</span>{" "}
              <span className="v">{video.profile || "—"}</span>
            </div>
            <div>
              <span className="k">width x height</span>{" "}
              <span className="v">{video.width && video.height ? `${video.width}x${video.height}` : "—"}</span>
            </div>
            <div>
              <span className="k">pix_fmt</span> <span className="v">{video.pix_fmt || "—"}</span>
            </div>
            <div>
              <span className="k">color_primaries</span>{" "}
              <span className="v">{video.color_primaries || "—"}</span>
            </div>
            <div>
              <span className="k">color_transfer</span>{" "}
              <span className="v">{video.color_transfer || "—"}</span>
            </div>
            <div>
              <span className="k">bit_rate</span>{" "}
              <span className="v">
                {media.overall_bitrate_kbps ? `${media.overall_bitrate_kbps} kb/s` : "—"}
              </span>
            </div>
            <div>
              <span className="k">hdr</span>{" "}
              <span className="v">{video.hdr ? "yes" : "no"}</span>
            </div>
            {audios.map((a, i) => (
              <div key={`a${i}`}>
                <div className="section">stream #{i + 1} · audio</div>
                <div>
                  <span className="k">codec_name</span> <span className="v">{a.codec || "?"}</span>
                </div>
                <div>
                  <span className="k">channel_layout</span>{" "}
                  <span className="v">{a.channels ? `${a.channels} ch` : "—"}</span>
                </div>
                <div>
                  <span className="k">language</span>{" "}
                  <span className="v">{(a.language || "und").toLowerCase()}</span>
                </div>
                <div>
                  <span className="k">bit_rate</span>{" "}
                  <span className="v">{a.bitrate_kbps ? `${a.bitrate_kbps} kb/s` : "—"}</span>
                </div>
              </div>
            ))}
            {subs.length > 0 && (
              <div>
                <div className="section">subtitle streams</div>
                {subs.map((s, i) => (
                  <div key={`s${i}`}>
                    <span className="k">#{audios.length + i + 1}</span>{" "}
                    <span className="v">
                      {(s.language || "und").toLowerCase()} · {s.codec || "?"}
                    </span>
                  </div>
                ))}
              </div>
            )}
            {pipeline && (
              <div>
                <div className="section">pipeline state</div>
                <div>
                  <span className="k">status</span> <span className="v">{pipeline.status || "—"}</span>
                </div>
                {pipeline.stage && (
                  <div>
                    <span className="k">stage</span> <span className="v">{pipeline.stage}</span>
                  </div>
                )}
                {pipeline.error && (
                  <div className="warn-line">
                    error · {pipeline.error}
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

function fmtBytes(b) {
  if (b == null) return "—";
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  if (b < 1024 * 1024 * 1024) return `${(b / (1024 * 1024)).toFixed(1)} MB`;
  return `${(b / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function FileManagerPanel({ sel, onClose }) {
  const [listing, setListing] = useState(null);
  const [err, setErr] = useState(null);
  useEffect(() => {
    let cancelled = false;
    setListing(null);
    setErr(null);
    api
      .getFileSiblings(sel.filepath)
      .then((r) => {
        if (!cancelled) setListing(r);
      })
      .catch((e) => {
        if (!cancelled) setErr(e.message || String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [sel.filepath]);

  const notify = (msg) => window.notify?.({ kind: "info", title: msg, body: sel.filepath });

  return (
    <div className="ins-panel">
      <div className="ins-panel-head">
        <span className="dollar">\\</span>
        <span>{listing?.parent || sel.filepath.replace(/[^\\/]+$/, "") || "."}</span>
        <span className="x" onClick={onClose}>
          ×
        </span>
      </div>
      <div className="fm-list">
        {err && (
          <div className="fm-row" style={{ color: "var(--bad)" }}>
            <span />
            <span className="name">{err}</span>
            <span className="kind" />
            <span className="sz" />
          </div>
        )}
        {!listing && !err && (
          <div className="fm-row">
            <span />
            <span className="name" style={{ color: "var(--ink-3)" }}>loading…</span>
            <span className="kind" />
            <span className="sz" />
          </div>
        )}
        {listing?.items?.map((s, i) => (
          <div key={i} className={`fm-row ${s.current ? "current" : ""}`}>
            <span className={s.is_dir ? "dir" : ""}>{s.is_dir ? "▸" : "·"}</span>
            <span className="name">{s.name}</span>
            <span className="kind">{s.kind}</span>
            <span className="sz">{s.is_dir ? "—" : fmtBytes(s.size_bytes)}</span>
          </div>
        ))}
      </div>
      <div className="fm-actions">
        <button
          onClick={() => {
            const newName = prompt("New filename (keeps original directory):", sel.filename);
            if (!newName || newName === sel.filename) return;
            api
              .renameFile(sel.filepath, newName)
              .then(() => {
                window.notify?.({ kind: "good", title: "Renamed", body: `${sel.filename} → ${newName}` });
                api.getFileSiblings(sel.filepath).then(setListing).catch(() => {});
              })
              .catch((e) =>
                window.notify?.({ kind: "bad", title: "Rename failed", body: String(e.message || e) })
              );
          }}
        >
          Rename
        </button>
        <button
          onClick={() => {
            try {
              navigator.clipboard?.writeText(sel.filepath);
              notify("SMB path copied to clipboard");
            } catch {
              notify("Clipboard unavailable — path: " + sel.filepath);
            }
          }}
        >
          Copy path
        </button>
        <button
          onClick={() => {
            if (!confirm(`Delete ${sel.filename}? This cannot be undone.`)) return;
            api
              .deleteFile(sel.filepath)
              .then(() =>
                window.notify?.({ kind: "good", title: "Deleted", body: sel.filename })
              )
              .catch((e) =>
                window.notify?.({ kind: "bad", title: "Delete failed", body: String(e.message || e) })
              );
          }}
        >
          Delete…
        </button>
      </div>
    </div>
  );
}

function Inspector({ sel, panel, setPanel, onFileOpen }) {
  const issues = detectIssues(sel);
  const hasIssues = issues.length > 0;
  const videoIssue = issues.find((i) => i.scope === "video");
  const audioIssues = issues.filter((i) => i.scope === "audio");
  const subIssues = issues.filter((i) => i.scope === "sub");
  const togglePanel = (k) => setPanel(panel === k ? null : k);

  return (
    <div className="inspector">
      <div className="ins-head">
        <div className="ins-title">{prettyTitle(sel.filename)}</div>
        <div className="ins-path">{sel.filepath}</div>
      </div>
      {hasIssues && (
        <div style={{ padding: "14px 20px 0" }}>
          <div className="issue-banner">
            <span className="led" />
            <div>
              <b>{issues.length} policy issue{issues.length === 1 ? "" : "s"}</b> — this file doesn't match the library
              target (AV1 video · Opus audio · ENG subs). Re-encoding will fix all of them.
            </div>
          </div>
        </div>
      )}
      <div className="ins-body">
        <div className="ins-section">
          <h4>
            Video{" "}
            <span>
              <span className={`tag ${codecKey(sel.codec)}`}>{codecLabel(sel.codec)}</span>
              {videoIssue && <IssueFlag issue={videoIssue} />}
            </span>
          </h4>
          <dl className="ins-grid">
            <dt>Resolution</dt>
            <dd>
              {sel.res || "—"} {sel.hdr && <span className="tag hdr" style={{ marginLeft: 4 }}>HDR</span>}
            </dd>
            <dt>Bitrate</dt>
            <dd>{fmtBitrate(sel.bitrate)}</dd>
            <dt>Duration</dt>
            <dd>{fmtDur(sel.dur)}</dd>
            <dt>File size</dt>
            <dd>{fmtSize(sel.size_gb)}</dd>
            <dt>Library</dt>
            <dd>{sel.library || libraryOf(sel.filepath)}</dd>
          </dl>
        </div>
        <div className="ins-section">
          <h4>
            Audio{" "}
            <span className="mono" style={{ color: "var(--ink-3)" }}>
              {(sel.audio || []).length} tracks
            </span>
          </h4>
          {(sel.audio || []).slice(0, 4).map((a, i) => {
            const issue = audioIssues.find((x) => x.idx === i);
            return (
              <div key={i} className="stream-line">
                <span className="sl-left">
                  <span className="tag" style={{ textTransform: "uppercase" }}>{a.codec}</span>
                  <span>{(a.lang || "und").toUpperCase()}</span>
                  <span style={{ color: "var(--ink-3)" }}>{a.ch}ch</span>
                  {a.lossless && <span className="tag hdr">lossless</span>}
                  {issue && <IssueFlag issue={issue} />}
                </span>
                <span className="sl-right">{fmtBitrate(a.br)}</span>
              </div>
            );
          })}
        </div>
        <div className="ins-section">
          <h4>
            Subtitles{" "}
            <span className="mono" style={{ color: "var(--ink-3)" }}>
              {(sel.subs || []).length} tracks
            </span>
          </h4>
          {(sel.subs || []).slice(0, 6).map((s, i) => {
            const issue = subIssues.find((x) => x.idx === i);
            return (
              <div key={i} className="stream-line">
                <span className="sl-left">
                  <span className="tag">{s.codec || "—"}</span>
                  <span>{(s.lang || "und").toUpperCase()}</span>
                  {issue && <IssueFlag issue={issue} />}
                </span>
              </div>
            );
          })}
          {(!sel.subs || sel.subs.length === 0) && (
            <div style={{ fontSize: 11, color: "var(--ink-3)" }}>No subtitle tracks found.</div>
          )}
        </div>
        <div className="ins-section">
          <h4>
            Encode plan{" "}
            <span className="mono" style={{ color: "var(--accent)" }}>
              ready
            </span>
          </h4>
          <dl className="ins-grid">
            <dt>Preset</dt>
            <dd>svtav1-psy · crf 24</dd>
            <dt>Est. output</dt>
            <dd>~{sel.size_gb ? (sel.size_gb * 0.58).toFixed(2) : "—"} GB</dd>
            <dt>Est. savings</dt>
            <dd style={{ color: "var(--accent)" }}>
              ~{sel.size_gb ? (sel.size_gb * 0.42).toFixed(2) : "—"} GB
            </dd>
            <dt>Est. runtime</dt>
            <dd>~{sel.dur ? fmtDur(sel.dur / 2) : "—"}</dd>
          </dl>
        </div>
      </div>
      {panel === "ffprobe" && <FfprobePanel sel={sel} onClose={() => setPanel(null)} />}
      {panel === "fm" && <FileManagerPanel sel={sel} onClose={() => setPanel(null)} />}
      <div className="ins-actions">
        <button
          className={`ins-btn primary ${hasIssues ? "wrong" : ""}`}
          onClick={async () => {
            try {
              const r = await api.addForce(sel.filepath);
              window.notify?.({
                kind: hasIssues ? "good" : "good",
                title: hasIssues
                  ? `Force-queued to fix ${issues.length} issue${issues.length === 1 ? "" : "s"}`
                  : "Force-queued for encoding",
                body: `${prettyTitle(sel.filename)} · now first in the force stack (${r.force_count ?? "?"} total)`,
              });
            } catch (e) {
              window.notify?.({ kind: "bad", title: "Queue failed", body: String(e.message || e) });
            }
          }}
        >
          {hasIssues
            ? `Fix ${issues.length} issue${issues.length === 1 ? "" : "s"} · queue encode`
            : "Queue encode now"}{" "}
          <span className="k">↵</span>
        </button>
        <button className="ins-btn" onClick={() => onFileOpen?.(sel.filepath)}>
          Open in drawer <span className="k">D</span>
        </button>
        <button
          className="ins-btn"
          onClick={async () => {
            try {
              const current = await api.getPriority().catch(() => ({ paths: [] }));
              const paths = Array.from(new Set([...(current.paths || []), sel.filepath]));
              const r = await api.setPriority(paths);
              window.notify?.({
                kind: "good",
                title: "Added to batch",
                body: `${prettyTitle(sel.filename)} · priority queue now ${r?.paths ?? paths.length} files`,
              });
            } catch (e) {
              window.notify?.({ kind: "bad", title: "Add failed", body: String(e.message || e) });
            }
          }}
        >
          Add to batch <span className="k">B</span>
        </button>
        <button
          className="ins-btn"
          onClick={() => togglePanel("ffprobe")}
          title="ffprobe -show_streams · codec profile, pix_fmt, HDR primaries, channel layout"
        >
          {panel === "ffprobe" ? "Hide stream analysis" : "Analyse streams"} <span className="k">A</span>
        </button>
        <button
          className="ins-btn"
          onClick={() => togglePanel("fm")}
          title="Reveal in the NAS file browser · rename, move, check sidecar subs/artwork"
        >
          {panel === "fm" ? "Hide file manager" : "Open in file manager"} <span className="k">O</span>
        </button>
      </div>
    </div>
  );
}
