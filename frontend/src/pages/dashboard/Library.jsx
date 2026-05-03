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
  // Grade-optimal: any file that's NOT in the optimal bucket. The audit
  // bucket (optimal/too_low/too_high/unknown) is read from the audit
  // sidecar JSON written by tools.audit_encode_cq. Without the sidecar we
  // have no bucket info at all → show nothing rather than guess.
  grade_optimal: (f) => {
    const b = f.cq_audit_bucket;
    return b && b !== "optimal";
  },
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
  grade_optimal: "CQ ≠ grade target",
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
    library: null, // null | "movie" | "series"
  });
  // CQ audit results — only fetched when the user drills into Grade-Optimised.
  // Map from filepath -> bucket; we annotate `f.cq_audit_bucket` in-place so
  // the drillFailures.grade_optimal predicate can filter on it.
  const [cqAudit, setCqAudit] = useState(null);
  // Sub-filter within the grade_optimal drill. null = all non-optimal,
  // otherwise narrows to one bucket. "too_high" is the deletion-review bucket.
  const [cqBucket, setCqBucket] = useState(null);
  useEffect(() => {
    if (drillKey !== "grade_optimal") return;
    let cancelled = false;
    api.getCqAudit().then((aud) => {
      if (cancelled) return;
      const map = new Map();
      for (const r of aud.results || []) map.set(r.filepath, r);
      setCqAudit({ ready: aud.ready, map, audited_at: aud.audited_at });
    }).catch(() => setCqAudit({ ready: false, map: new Map() }));
    return () => { cancelled = true; };
  }, [drillKey]);
  // Annotate `all` with bucket data so the drill predicate can read it.
  // Cheap — single pass, mutates an in-memory shadow not the input.
  const annotatedAll = useMemo(() => {
    if (drillKey !== "grade_optimal" || !cqAudit?.map) return all;
    return all.map((f) => {
      const r = cqAudit.map.get(f.filepath);
      if (!r) return f;
      return {
        ...f,
        cq_audit_bucket: r.bucket,
        cq_audit_target: r.target_cq,
        cq_audit_current: r.current_cq,
        cq_audit_review_status: r.review_status || null,
      };
    });
  }, [all, cqAudit, drillKey]);
  const [sort, setSort] = useState("size_desc");
  const [group, setGroup] = useState("none");
  const [sortOpen, setSortOpen] = useState(false);
  const [groupOpen, setGroupOpen] = useState(false);
  const [visibleCount, setVisibleCount] = useState(50);
  const [panel, setPanel] = useState(null);
  const pipelineFiles = pipelineData?.files || {};

  const drillFn = drillKey && drillFailures[drillKey];
  // Bucket counts for the sub-filter chip row. Only meaningful inside the
  // grade_optimal drill — outside it we don't need them and the audit map
  // hasn't been fetched anyway.
  const bucketCounts = useMemo(() => {
    if (drillKey !== "grade_optimal" || !annotatedAll) return null;
    const c = { too_low: 0, too_high: 0, unknown: 0, optimal: 0 };
    for (const f of annotatedAll) {
      const b = f.cq_audit_bucket;
      if (b && c[b] !== undefined) c[b] += 1;
    }
    return c;
  }, [annotatedAll, drillKey]);

  const rows = useMemo(() => {
    const source = drillKey === "grade_optimal" ? annotatedAll : all;
    const filtered = source.filter((f) => {
      // Drill-in filter takes precedence and stacks with the rest.
      if (drillFn && !drillFn(f)) return false;
      // Sub-bucket filter inside the grade_optimal drill.
      if (drillKey === "grade_optimal" && cqBucket && f.cq_audit_bucket !== cqBucket) return false;
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
      if (filters.library) {
        const lib = (f.library || libraryOf(f.filepath) || "").toLowerCase();
        if (filters.library === "movie" && lib !== "movies" && lib !== "movie") return false;
        if (filters.library === "series" && lib !== "series" && lib !== "tv") return false;
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
  }, [all, annotatedAll, drillKey, cqBucket, query, filters, sort, pipelineFiles, drillFn]);

  // Reset selection + pagination when the drill key changes so the user
  // lands on the first matching file rather than wherever they last were.
  useEffect(() => {
    setSelIdx(0);
    setVisibleCount(50);
    setCqBucket(null);
  }, [drillKey]);

  // reset pagination when filters change
  useEffect(() => setVisibleCount(50), [query, filters, sort]);

  const sel = rows[selIdx] || rows[0] || all[0];

  // Optimistic local update for grade-review actions: flip the audit map's
  // bucket entry so the row immediately moves to the new bucket without
  // waiting for a full /api/cq-audit refetch. The server has already
  // patched audit_cq.json in place so the next load is consistent.
  const updateAuditRow = (filepath, patch) => {
    setCqAudit((prev) => {
      if (!prev?.map) return prev;
      const next = new Map(prev.map);
      const cur = next.get(filepath) || { filepath };
      next.set(filepath, { ...cur, ...patch });
      return { ...prev, map: next };
    });
  };

  const toggle = (k, v) => setFilters((f) => ({ ...f, [k]: f[k] === v ? null : v }));
  const toggleBool = (k) => setFilters((f) => ({ ...f, [k]: !f[k] }));
  const setStatus = (v) => setFilters((f) => ({ ...f, status: f.status === v ? null : v }));

  const sortLabel = SORT_OPTIONS.find((o) => o.k === sort)?.label || sort;
  const groupLabel = GROUP_OPTIONS.find((o) => o.k === group)?.label || group;

  // Library type counts for the Movies/Series facet. Computed off the
  // unfiltered list so the chip number reflects the total in each
  // bucket, not the count after other facets are applied.
  const libraryCounts = useMemo(() => {
    let movies = 0;
    let series = 0;
    for (const f of all) {
      const lib = (f.library || libraryOf(f.filepath) || "").toLowerCase();
      if (lib === "movies" || lib === "movie") movies += 1;
      else if (lib === "series" || lib === "tv") series += 1;
    }
    return { movies, series };
  }, [all]);

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
      {drillKey === "grade_optimal" && bucketCounts && (
        <div className="facets" style={{ marginBottom: 12 }}>
          <div className="facet-group">
            <span className="lbl">Bucket</span>
            <button
              className={`chip ${cqBucket === null ? "on" : ""}`}
              onClick={() => setCqBucket(null)}
              title="Show every file that isn't grade-optimal"
            >
              All non-optimal{" "}
              <span className="c">
                {fmtNum(bucketCounts.too_low + bucketCounts.too_high + bucketCounts.unknown)}
              </span>
            </button>
            <button
              className={`chip ${cqBucket === "too_low" ? "on" : ""}`}
              onClick={() => setCqBucket("too_low")}
              title="Encoded with a CQ below the grade target — re-encode candidates"
            >
              Too low <span className="c">{fmtNum(bucketCounts.too_low)}</span>
            </button>
            <button
              className={`chip ${cqBucket === "too_high" ? "on" : ""}`}
              onClick={() => setCqBucket("too_high")}
              title="Encoded with a CQ above the grade target — over-compressed; review for delete + re-download"
            >
              Too high <span className="c">{fmtNum(bucketCounts.too_high)}</span>
            </button>
            {bucketCounts.unknown > 0 && (
              <button
                className={`chip ${cqBucket === "unknown" ? "on" : ""}`}
                onClick={() => setCqBucket("unknown")}
                title="No CQ tag and no bitrate-inference match"
              >
                Unknown <span className="c">{fmtNum(bucketCounts.unknown)}</span>
              </button>
            )}
          </div>
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
          <span className="lbl">Library</span>
          <button
            className={`chip ${filters.library === "movie" ? "on" : ""}`}
            onClick={() => toggle("library", "movie")}
          >
            Movies <span className="c">{fmtNum(libraryCounts.movies)}</span>
          </button>
          <button
            className={`chip ${filters.library === "series" ? "on" : ""}`}
            onClick={() => toggle("library", "series")}
          >
            Series <span className="c">{fmtNum(libraryCounts.series)}</span>
          </button>
        </div>
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
            onAuditPatch={updateAuditRow}
          />
        )}
      </div>
    </div>
  );
}

function FileRow({ f, selected, onClick, onDoubleClick, pipelineInfo }) {
  const st = statusLabel(pipelineInfo?.status);
  const bucket = f.cq_audit_bucket;
  const bucketColor = {
    too_low: "var(--accent)",
    too_high: "var(--bad)",
    unknown: "var(--ink-3)",
    optimal: "var(--good)",
  }[bucket];
  const bucketBadge = bucket && (
    <span
      className="tag"
      title={
        bucket === "too_high"
          ? "Encoded above the grade target — over-compressed (lower quality than intended). Candidate for delete + re-download."
          : bucket === "too_low"
          ? "Encoded below the grade target — bigger than needed. Candidate for re-encode at the grade target."
          : bucket === "unknown"
          ? "No CQ tag and no bitrate-inference match"
          : "On target"
      }
      style={{
        marginLeft: 6,
        background: "transparent",
        border: `1px solid ${bucketColor}`,
        color: bucketColor,
        textTransform: "none",
        fontVariantNumeric: "tabular-nums",
      }}
    >
      CQ {f.cq_audit_current ?? "?"}→{f.cq_audit_target ?? "?"}
    </span>
  );
  return (
    <div
      className={`ft-row ${selected ? "sel" : ""}`}
      onClick={onClick}
      onDoubleClick={onDoubleClick}
    >
      <div className="ft-name">
        <div className="n">
          <span className="n-title">{prettyTitle(f.filename)}</span>
          {f.hdr && <span className="tag hdr">HDR</span>}
          {bucketBadge}
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

function Inspector({ sel, panel, setPanel, onFileOpen, onAuditPatch }) {
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
        {sel.cq_audit_bucket && (
          <div className="ins-section">
            <h4>
              Grade audit{" "}
              <span
                className="tag"
                style={{
                  background: "transparent",
                  border: `1px solid ${
                    sel.cq_audit_bucket === "too_high"
                      ? "var(--bad)"
                      : sel.cq_audit_bucket === "too_low"
                      ? "var(--accent)"
                      : "var(--ink-3)"
                  }`,
                  color:
                    sel.cq_audit_bucket === "too_high"
                      ? "var(--bad)"
                      : sel.cq_audit_bucket === "too_low"
                      ? "var(--accent)"
                      : "var(--ink-3)",
                  textTransform: "none",
                }}
              >
                {sel.cq_audit_bucket.replace("_", " ")}
              </span>
            </h4>
            <dl className="ins-grid">
              <dt>Current CQ</dt>
              <dd>{sel.cq_audit_current ?? "—"}</dd>
              <dt>Target CQ</dt>
              <dd>{sel.cq_audit_target ?? "—"}</dd>
              <dt>Delta</dt>
              <dd
                style={{
                  color:
                    sel.cq_audit_current != null && sel.cq_audit_target != null
                      ? sel.cq_audit_current > sel.cq_audit_target
                        ? "var(--bad)"
                        : "var(--accent)"
                      : "var(--ink-3)",
                }}
              >
                {sel.cq_audit_current != null && sel.cq_audit_target != null
                  ? `${sel.cq_audit_current - sel.cq_audit_target > 0 ? "+" : ""}${
                      sel.cq_audit_current - sel.cq_audit_target
                    }`
                  : "—"}
              </dd>
              <dt>Verdict</dt>
              <dd style={{ fontSize: 11, color: "var(--ink-2)" }}>
                {sel.cq_audit_review_status === "accepted"
                  ? "Manually accepted — counted as Grade Optimal."
                  : sel.cq_audit_bucket === "too_high"
                  ? "Over-compressed vs grade target — review for delete + re-download, or accept if quality is fine."
                  : sel.cq_audit_bucket === "too_low"
                  ? "Under-compressed vs grade target — re-encode candidate."
                  : sel.cq_audit_bucket === "unknown"
                  ? "No CQ tag and no bitrate match — needs manual probe."
                  : "On target."}
              </dd>
            </dl>
            <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
              {sel.cq_audit_review_status === "accepted" ? (
                <button
                  className="ins-btn"
                  title="Remove the accepted override — file goes back through the normal CQ-vs-target comparison"
                  onClick={async () => {
                    try {
                      const r = await api.gradeClear(sel.filepath);
                      onAuditPatch?.(sel.filepath, {
                        review_status: null,
                        bucket: r.bucket || "too_high",
                      });
                      window.notify?.({
                        kind: "good",
                        title: "Override cleared",
                        body: `${prettyTitle(sel.filename)} → ${r.bucket || "rebucketed"}`,
                      });
                    } catch (e) {
                      window.notify?.({
                        kind: "bad",
                        title: "Clear failed",
                        body: String(e.message || e),
                      });
                    }
                  }}
                >
                  Clear override
                </button>
              ) : (
                <button
                  className="ins-btn primary"
                  title="Stamp GRADE_REVIEW=accepted into the MKV — file counts as Grade Optimal in audits"
                  onClick={async () => {
                    try {
                      await api.gradeAccept(sel.filepath);
                      onAuditPatch?.(sel.filepath, {
                        review_status: "accepted",
                        bucket: "optimal",
                      });
                      window.notify?.({
                        kind: "good",
                        title: "Marked Grade Optimal",
                        body: prettyTitle(sel.filename),
                      });
                    } catch (e) {
                      window.notify?.({
                        kind: "bad",
                        title: "Accept failed",
                        body: String(e.message || e),
                      });
                    }
                  }}
                >
                  Mark Grade Optimal ✓
                </button>
              )}
            </div>
          </div>
        )}
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
        <button
          className="ins-btn"
          style={{
            marginLeft: "auto",
            color: "var(--bad)",
            borderColor: "var(--bad)",
          }}
          title="Permanently delete this file from the NAS. Use for too-high CQ files you want to re-download at higher quality."
          onClick={async () => {
            const ok = window.confirm(
              `Delete this file from the NAS?\n\n${sel.filename}\n\nThis cannot be undone. The file is removed from disk and dropped from the pipeline state.`
            );
            if (!ok) return;
            try {
              await api.deleteFile(sel.filepath);
              window.notify?.({
                kind: "good",
                title: "Deleted",
                body: prettyTitle(sel.filename),
              });
            } catch (e) {
              window.notify?.({
                kind: "bad",
                title: "Delete failed",
                body: String(e.message || e),
              });
            }
          }}
        >
          Delete file <span className="k">⌫</span>
        </button>
      </div>
    </div>
  );
}
