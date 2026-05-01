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

const SORT_OPTIONS = [
  { k: "size_desc", label: "Size ↓" },
  { k: "size_asc", label: "Size ↑" },
  { k: "name_asc", label: "Name A-Z" },
  { k: "bitrate_desc", label: "Bitrate ↓" },
  { k: "duration_desc", label: "Duration ↓" },
];

const GROUP_OPTIONS = [
  { k: "none", label: "—" },
  { k: "library", label: "Library (Movies / Series)" },
  { k: "codec", label: "Codec" },
  { k: "resolution", label: "Resolution" },
];

const hasAtmos = (f) =>
  (f.audio || []).some((a) => {
    const c = (a.codec || "").toLowerCase();
    const t = (a.title || "").toLowerCase();
    return c.includes("atmos") || c.includes("truehd") || t.includes("atmos");
  });

const foreignSubsOnly = (f) => {
  const subs = f.subs || [];
  const ext = f.externalSubs || [];
  if (subs.length + ext.length === 0) return false;
  const hasEng =
    subs.some((s) => (s.lang || "").toLowerCase().startsWith("en")) ||
    ext.some((s) => (s.language || "").toLowerCase().startsWith("en"));
  return !hasEng;
};

const statusIsErrored = (pf) => ["error", "errored", "failed"].includes((pf?.status || "").toLowerCase());

// Drill-in: when Glance's compliance card is clicked it routes here with a
// drillKey identifying which standards-compliance bucket to filter on.
// Each function below returns true when the file FAILS that rule (i.e. it's
// in the "to-go" bucket the dashboard surfaces). Mirrors the server-side
// _compliance_for_entry logic in server/routers/library.py so the frontend
// list matches the count shown on the card the user just clicked.
const KEEP_LANGS_SET = new Set(["en", "eng", "english", "und", ""]);
const isEng = (s) => {
  const l = (s || "").toLowerCase().trim();
  return l === "en" || l === "eng" || l === "english";
};
const drillFailures = {
  // Non-AV1 video
  video: (f) => (f.codec || "").toLowerCase() !== "av1",
  // Any audio stream not EAC-3 OR foreign-language audio (stricter audio_ok rule)
  audio: (f) => {
    const audio = f.audio || [];
    if (audio.length === 0) return true; // zero-audio is non-compliant
    if (audio.some((a) => !["eac3", "e-ac-3"].includes((a.codec_raw || a.codec || "").toLowerCase())))
      return true;
    return false;
  },
  // English subs missing
  subs: (f) => {
    const subs = f.subs || [];
    const ext = f.externalSubs || [];
    return !(subs.some((s) => isEng(s.lang)) || ext.some((s) => isEng(s.language)));
  },
  // Foreign subs present (internal or external)
  foreign_subs: (f) => {
    const subs = f.subs || [];
    const ext = f.externalSubs || [];
    return (
      subs.some((s) => !KEEP_LANGS_SET.has((s.lang || "und").toLowerCase())) ||
      ext.some((s) => !KEEP_LANGS_SET.has((s.language || "und").toLowerCase()))
    );
  },
  // Missing TMDb
  tmdb: (f) => !f.tmdb || !f.tmdb.tmdb_id,
  // Any und audio or sub stream
  langs: (f) => {
    const audUnd = (f.audio || []).some((a) => {
      const l = (a.lang || a.language || "und").toLowerCase();
      return l === "und" || l === "unk" || l === "";
    });
    const subUnd = (f.subs || []).some((s) => {
      const l = (s.lang || s.language || "und").toLowerCase();
      return l === "und" || l === "unk" || l === "";
    });
    return audUnd || subUnd;
  },
  // Filename mismatches title (server flag)
  filename: (f) => f.filename_matches_folder === false,
  // Folder doesn't match (English filename rule, same flag in current API)
  english_filename: (f) => f.filename_matches_folder === false,
};
const drillLabel = {
  video: "Non-AV1 video",
  audio: "Non-EAC-3 audio",
  subs: "Missing English sub",
  foreign_subs: "Has foreign subs",
  tmdb: "No TMDb metadata",
  langs: "Has und tracks",
  filename: "Filename mismatch",
  english_filename: "Folder mismatch",
};

export function Library({ data, pipelineData, onFileOpen, drillKey, onClearDrill }) {
  const all = data.files || data.topTargets;
  const [selIdx, setSelIdx] = useState(0);
  const [query, setQuery] = useState("");
  const [filters, setFilters] = useState({
    codec: null,
    res: null,
    hdr: false,
    atmos: false,
    foreignSubs: false,
    status: null, // null | "needs_encode" | "errored"
  });
  const [sort, setSort] = useState("size_desc");
  const [group, setGroup] = useState("none");
  const [sortOpen, setSortOpen] = useState(false);
  const [groupOpen, setGroupOpen] = useState(false);
  const [visibleCount, setVisibleCount] = useState(50);
  const [panel, setPanel] = useState(null);
  const pipelineFiles = pipelineData?.files || {};

  const drillFn = drillKey && drillFailures[drillKey];
  const rows = useMemo(() => {
    const filtered = all.filter((f) => {
      // Drill-in filter takes precedence and stacks with the rest.
      if (drillFn && !drillFn(f)) return false;
      if (query && !`${f.filename} ${f.filepath}`.toLowerCase().includes(query.toLowerCase())) return false;
      if (filters.codec && codecKey(f.codec) !== filters.codec) return false;
      if (filters.res && resKey(f.res) !== filters.res) return false;
      if (filters.hdr && !f.hdr) return false;
      if (filters.atmos && !hasAtmos(f)) return false;
      if (filters.foreignSubs && !foreignSubsOnly(f)) return false;
      if (filters.status === "needs_encode") {
        const k = codecKey(f.codec);
        if (k !== "hevc" && k !== "h264") return false;
      }
      if (filters.status === "errored") {
        if (!statusIsErrored(pipelineFiles[f.filepath])) return false;
      }
      return true;
    });
    const cmp = {
      size_desc: (a, b) => (b.size_gb || 0) - (a.size_gb || 0),
      size_asc: (a, b) => (a.size_gb || 0) - (b.size_gb || 0),
      name_asc: (a, b) => (a.filename || "").localeCompare(b.filename || ""),
      bitrate_desc: (a, b) => (b.bitrate || 0) - (a.bitrate || 0),
      duration_desc: (a, b) => (b.dur || 0) - (a.dur || 0),
    }[sort];
    return [...filtered].sort(cmp);
  }, [all, query, filters, sort, pipelineFiles, drillFn]);

  // Reset selection + pagination when the drill key changes so the user
  // lands on the first matching file rather than wherever they last were.
  useEffect(() => {
    setSelIdx(0);
    setVisibleCount(50);
  }, [drillKey]);

  // reset pagination when filters change
  useEffect(() => setVisibleCount(50), [query, filters, sort]);

  const sel = rows[selIdx] || rows[0] || all[0];

  const toggle = (k, v) => setFilters((f) => ({ ...f, [k]: f[k] === v ? null : v }));
  const toggleBool = (k) => setFilters((f) => ({ ...f, [k]: !f[k] }));
  const setStatus = (v) => setFilters((f) => ({ ...f, status: f.status === v ? null : v }));

  const sortLabel = SORT_OPTIONS.find((o) => o.k === sort)?.label || sort;
  const groupLabel = GROUP_OPTIONS.find((o) => o.k === group)?.label || group;

  return (
    <div className="view">
      {drillKey && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            padding: "8px 14px",
            marginBottom: 12,
            background: "var(--surface)",
            border: "1px solid var(--accent)",
            borderRadius: 6,
          }}
        >
          <span style={{ color: "var(--accent)", fontWeight: 600, fontSize: 12 }}>
            DRILL-IN
          </span>
          <span style={{ fontSize: 13 }}>
            Showing only files that fail: <b>{drillLabel[drillKey] || drillKey}</b> ({fmtNum(rows.length)} of{" "}
            {fmtNum(all.length)})
          </span>
          <button
            onClick={onClearDrill}
            style={{
              marginLeft: "auto",
              padding: "4px 10px",
              background: "transparent",
              border: "1px solid var(--line)",
              borderRadius: 4,
              color: "var(--ink-2)",
              cursor: "pointer",
              fontSize: 11,
            }}
            title="Clear the drill-in filter and show the full library"
          >
            Clear filter ✕
          </button>
        </div>
      )}
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
            <b>Showing</b>: {fmtNum(Math.min(visibleCount, rows.length))} /{" "}
            {fmtNum(rows.length)} match · {fmtNum(all.length)} total
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
        <div style={{ position: "relative" }}>
          <button
            className={`chip ${sortOpen ? "on" : ""}`}
            onClick={() => {
              setSortOpen((v) => !v);
              setGroupOpen(false);
            }}
          >
            Sort: {sortLabel}
          </button>
          {sortOpen && (
            <div className="chip-menu">
              {SORT_OPTIONS.map((o) => (
                <div
                  key={o.k}
                  className={`chip-menu-item ${sort === o.k ? "on" : ""}`}
                  onClick={() => {
                    setSort(o.k);
                    setSortOpen(false);
                  }}
                >
                  {o.label}
                </div>
              ))}
            </div>
          )}
        </div>
        <div style={{ position: "relative" }}>
          <button
            className={`chip ${groupOpen ? "on" : ""}`}
            onClick={() => {
              setGroupOpen((v) => !v);
              setSortOpen(false);
            }}
          >
            Group: {groupLabel}
          </button>
          {groupOpen && (
            <div className="chip-menu">
              {GROUP_OPTIONS.map((o) => (
                <div
                  key={o.k}
                  className={`chip-menu-item ${group === o.k ? "on" : ""}`}
                  onClick={() => {
                    setGroup(o.k);
                    setGroupOpen(false);
                  }}
                >
                  {o.label}
                </div>
              ))}
            </div>
          )}
        </div>
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
            onClick={() => toggleBool("hdr")}
          >
            HDR
          </button>
          <button
            className={`chip ${filters.atmos ? "on" : ""}`}
            onClick={() => toggleBool("atmos")}
            title="Audio track tagged Atmos or encoded as TrueHD"
          >
            Atmos
          </button>
          <button
            className={`chip ${filters.foreignSubs ? "on" : ""}`}
            onClick={() => toggleBool("foreignSubs")}
            title="File has subs, none of them English"
          >
            Foreign subs only
          </button>
        </div>
        <div className="facet-group">
          <span className="lbl">Status</span>
          <button
            className={`chip ${filters.status === "needs_encode" ? "on" : ""}`}
            onClick={() => setStatus("needs_encode")}
          >
            Needs encode{" "}
            <span className="c">
              {fmtNum(codecCount(data.codecs, "hevc") + codecCount(data.codecs, "h264"))}
            </span>
          </button>
          <button
            className={`chip ${filters.status === "errored" ? "on" : ""}`}
            onClick={() => setStatus("errored")}
          >
            Errored <span className="c">{data.errorCount || 0}</span>
          </button>
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
          {(() => {
            const slice = rows.slice(0, visibleCount);
            if (group === "none") {
              return slice.map((f, i) => (
                <FileRow
                  key={f.filepath || i}
                  f={f}
                  selected={sel === f}
                  onClick={() => setSelIdx(i)}
                  onDoubleClick={() => onFileOpen?.(f.filepath)}
                  pipelineInfo={pipelineFiles[f.filepath]}
                />
              ));
            }
            // Grouped view
            const getBucket = (f) => {
              if (group === "library") return libraryOf(f.filepath) || "other";
              if (group === "codec") return codecLabel(f.codec) || "Unknown";
              if (group === "resolution") return f.res || "—";
              return "all";
            };
            const buckets = new Map();
            slice.forEach((f) => {
              const b = getBucket(f);
              if (!buckets.has(b)) buckets.set(b, []);
              buckets.get(b).push(f);
            });
            let runningIdx = 0;
            return Array.from(buckets.entries()).map(([bucket, files]) => (
              <div key={bucket}>
                <div
                  style={{
                    padding: "10px 16px 6px",
                    fontSize: 10,
                    color: "var(--ink-3)",
                    textTransform: "uppercase",
                    letterSpacing: "0.1em",
                    borderBottom: "1px solid var(--line)",
                  }}
                >
                  {bucket} <span style={{ color: "var(--ink-4)" }}>· {files.length}</span>
                </div>
                {files.map((f) => {
                  const absIdx = runningIdx++;
                  return (
                    <FileRow
                      key={f.filepath || absIdx}
                      f={f}
                      selected={sel === f}
                      onClick={() => setSelIdx(absIdx)}
                      onDoubleClick={() => onFileOpen?.(f.filepath)}
                      pipelineInfo={pipelineFiles[f.filepath]}
                    />
                  );
                })}
              </div>
            ));
          })()}
          {rows.length > visibleCount && (
            <div
              style={{
                padding: "14px 20px",
                textAlign: "center",
                borderTop: "1px solid var(--line)",
              }}
            >
              <button
                className="chip"
                onClick={() => setVisibleCount((n) => n + 100)}
              >
                Show {Math.min(100, rows.length - visibleCount)} more ·{" "}
                <span className="c">{fmtNum(rows.length - visibleCount)} hidden</span>
              </button>
            </div>
          )}
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

function FileRow({ f, selected, onClick, onDoubleClick, pipelineInfo }) {
  const st = statusLabel(pipelineInfo?.status);
  return (
    <div
      className={`ft-row ${selected ? "sel" : ""}`}
      onClick={onClick}
      onDoubleClick={onDoubleClick}
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
      <div className="status-cell">
        <span className="led" style={{ background: st.led }} />
        {st.label}
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
