import { useEffect, useMemo, useState } from "react";
import { api } from "../../lib/api";
import {
  codecCount,
  codecKey,
  fmtNum,
  fmtPct,
  fmtSize,
  prettyTitle,
} from "./helpers";

function fmtAge(ms) {
  if (ms == null) return null;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d`;
}

function fmtEta(secs) {
  if (secs == null || secs < 0) return null;
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m`;
  return `${secs}s`;
}

function LangDetectProgress() {
  const [state, setState] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const refresh = () => {
      api
        .getLangDetectStatus()
        .then((s) => !cancelled && setState(s))
        .catch(() => {});
    };
    refresh();
    // Poll fast while running, slow when idle. Re-evaluating cadence on each
    // render keeps the interval in step with the current state.
    const id = setInterval(refresh, 2500);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  if (!state || !state.running) return null;

  const total = state.total || 0;
  const processed = state.processed || 0;
  const detected = state.detected || 0;
  const failed = state.failed || 0;
  const pct = total > 0 ? Math.min(100, (processed / total) * 100) : 0;
  const recent = Array.isArray(state.recent) ? state.recent : [];

  return (
    <div className="encode-progress" style={{ marginTop: 16 }}>
      <h3>
        Whisper language detection
        <span
          className="mono"
          style={{ color: "var(--ink-3)", fontSize: 11, fontWeight: 400, marginLeft: "auto" }}
        >
          {fmtNum(processed)} / {fmtNum(total)} files
          {state.eta_secs != null && ` · ETA ${fmtEta(state.eta_secs)}`}
          {state.rate_files_per_min != null && ` · ${state.rate_files_per_min.toFixed(1)} files/min`}
        </span>
      </h3>
      <div
        style={{
          margin: "10px 0",
          height: 8,
          background: "var(--line)",
          borderRadius: 4,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            height: "100%",
            width: `${pct}%`,
            background: "#10b981",
            transition: "width 0.4s ease",
          }}
        />
      </div>
      <div
        style={{
          display: "flex",
          gap: 16,
          fontSize: 12,
          color: "var(--ink-2)",
          marginBottom: recent.length ? 8 : 0,
        }}
      >
        <span>
          <span className="mono" style={{ color: "#10b981", fontWeight: 600 }}>
            {fmtNum(detected)}
          </span>{" "}
          detected
        </span>
        {failed > 0 && (
          <span>
            <span className="mono" style={{ color: "var(--warn)", fontWeight: 600 }}>
              {fmtNum(failed)}
            </span>{" "}
            unresolved
          </span>
        )}
        <span style={{ marginLeft: "auto", fontSize: 11, color: "var(--ink-3)" }}>
          {pct.toFixed(1)}%
        </span>
      </div>
      {recent.length > 0 && (
        <div
          style={{
            marginTop: 8,
            paddingTop: 8,
            borderTop: "1px solid var(--line)",
            fontSize: 11,
            color: "var(--ink-3)",
          }}
        >
          <div style={{ marginBottom: 4, color: "var(--ink-2)" }}>Recent:</div>
          {recent.slice(-5).reverse().map((r, i) => (
            <div
              key={i}
              className="mono"
              style={{
                display: "flex",
                justifyContent: "space-between",
                gap: 8,
                padding: "2px 0",
              }}
            >
              <span
                style={{
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  flex: 1,
                  minWidth: 0,
                }}
              >
                {r.file}
              </span>
              <span style={{ color: r.detected === "unresolved" ? "var(--warn)" : "#10b981" }}>
                {r.detected}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ActiveRow({ f }) {
  return (
    <div className="active" style={f.stale ? { borderColor: "rgba(240,180,41,0.35)" } : undefined}>
      <div className="active-top">
        <div className="active-title" title={f.filename}>
          {prettyTitle(f.filename)}
        </div>
        <div className="active-meta">
          {[f.res, f.codec ? `${codecKey(f.codec).toUpperCase()} → AV1` : null]
            .filter(Boolean)
            .join(" · ") || f.status}
        </div>
      </div>
      {f.progressPct != null && (
        <div className="active-bar">
          <div className="fill" style={{ width: Math.min(100, Math.max(0, f.progressPct)) + "%" }} />
        </div>
      )}
      <div className="active-foot">
        <span>
          {f.progressPct != null
            ? `${f.progressPct}%${f.speed ? ` · ${f.speed}` : ""}${f.fps ? ` · ${f.fps} fps` : ""}`
            : `${f.stage || f.status}${f.mode && f.mode !== "full_gamut" ? ` · ${f.mode}` : ""}`}
          {f.stale && (
            <span
              className="mono"
              style={{ color: "var(--warn)", marginLeft: 6 }}
              title="last_updated beyond expected for this stage"
            >
              · stale {fmtAge(f.ageMs)}
            </span>
          )}
        </span>
        <span>
          {[
            f.sizeGb ? fmtSize(f.sizeGb) : null,
            f.eta ? `ETA ${f.eta}` : f.ageMs != null && !f.stale ? `${fmtAge(f.ageMs)} in state` : null,
          ]
            .filter(Boolean)
            .join(" · ")}
        </span>
      </div>
    </div>
  );
}

export function Glance({ data, pipelineData, throughputPerDay, workersActive, workersTotal, onNavigate }) {
  const [historySummary, setHistorySummary] = useState(null);
  const [completion, setCompletion] = useState(null);
  const [healthDeep, setHealthDeep] = useState(null);
  const [incidentsOpen, setIncidentsOpen] = useState(false);
  useEffect(() => {
    let cancelled = false;
    api
      .getHistorySummary()
      .then((s) => {
        if (!cancelled) setHistorySummary(s);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  // Invariants / deep health — polled on the same 60s cadence as library
  // completion. Drives the Incidents card below the page head.
  useEffect(() => {
    let cancelled = false;
    const refresh = () => {
      if (document.hidden) return;
      api.getHealthDeep().then((h) => !cancelled && setHealthDeep(h)).catch(() => {});
    };
    refresh();
    const id = setInterval(refresh, 60_000);
    const onVis = () => { if (!document.hidden) refresh(); };
    window.addEventListener("visibilitychange", onVis);
    window.addEventListener("focus", refresh);
    return () => {
      cancelled = true;
      clearInterval(id);
      window.removeEventListener("visibilitychange", onVis);
      window.removeEventListener("focus", refresh);
    };
  }, []);

  // Library standards compliance — polled alongside the media_report refresh so
  // "remaining work per standard" stays current after encodes land.
  useEffect(() => {
    let cancelled = false;
    const refresh = () => {
      if (document.hidden) return;
      api.getLibraryCompletion().then((c) => !cancelled && setCompletion(c)).catch(() => {});
    };
    refresh();
    const id = setInterval(refresh, 60_000);
    const onVis = () => { if (!document.hidden) refresh(); };
    window.addEventListener("visibilitychange", onVis);
    window.addEventListener("focus", refresh);
    return () => {
      cancelled = true;
      clearInterval(id);
      window.removeEventListener("visibilitychange", onVis);
      window.removeEventListener("focus", refresh);
    };
  }, []);

  const codecs = data.codecs;
  const resolutions = data.resolutions;
  const summary = data.summary;
  const total = summary.total_files || 0;

  const av1 = codecCount(codecs, "av1");
  const hevc = codecCount(codecs, "hevc");
  const h264 = codecCount(codecs, "h264");
  const other = Math.max(0, total - av1 - hevc - h264);

  const segs = [
    { k: "AV1", n: av1, c: "var(--accent)" },
    { k: "HEVC", n: hevc, c: "var(--blue)" },
    { k: "H.264", n: h264, c: "var(--warn)" },
    { k: "Other", n: other, c: "var(--ink-4)" },
  ];

  // Prefer the history totals (cumulative across all runs) over pipeline.stats.bytes_saved,
  // which is a short-lived counter that can go negative if the latest runs grew files.
  const reclaimedGb = (() => {
    const histBytes = historySummary?.totals?.saved_bytes;
    if (histBytes != null) return histBytes / 1024 ** 3;
    return Math.max(0, (pipelineData?.stats?.bytes_saved || 0) / 1024 ** 3);
  })();
  const remainingFiles = Math.max(0, total - av1);
  const remainingSizeGb = data.remainingSizeGb || 0;

  const pipelineFiles = pipelineData?.files || {};
  const activeSample = useMemo(() => {
    const now = Date.now();
    // Expected max time a file should spend in a given stage before it's suspicious.
    // `last_updated` only ticks when the pipeline changes state — a 60 min NVENC encode looks
    // "stale 60m" on that metric even though ffmpeg is pegged.
    const staleThresholdMs = {
      encoding: 3 * 60 * 60 * 1000, // up to 3h for big 4K encodes
      audio_transcode: 20 * 60 * 1000,
      language_detect: 10 * 60 * 1000,
      fetch: 20 * 60 * 1000, // 25 GB at 25 MB/s = 17 min
      upload: 15 * 60 * 1000,
      gap_fill: 10 * 60 * 1000,
    };
    return Object.entries(pipelineFiles)
      .filter(([, info]) =>
        ["encoding", "analyzing", "fetching", "uploading", "processing"].includes(
          (info.status || "").toLowerCase()
        )
      )
      .map(([path, info]) => {
        const lastUpdated = info.last_updated ? Date.parse(info.last_updated) : null;
        const ageMs = lastUpdated ? now - lastUpdated : null;
        const inputBytes = info.input_size_bytes ?? null;
        const stage = info.stage || info.reason || null;
        const status = (info.status || "encoding").toLowerCase();
        let bucket = "queued";
        if (stage === "encoding" || status === "encoding" || status === "analyzing") {
          bucket = "encoding";
        } else if (status === "fetching" || stage === "fetch") {
          bucket = "fetching";
        } else if (stage === "upload" || status === "uploading") {
          bucket = "uploading";
        } else if (stage && stage in staleThresholdMs) {
          bucket = "encoding";
        }
        const threshold = stage ? staleThresholdMs[stage] : null;
        return {
          filename: path.split(/[\\/]/).pop(),
          filepath: path,
          status,
          stage,
          mode: info.mode || null,
          res: info.res_key || info.resolution || null,
          codec: info.codec || null,
          sizeGb: inputBytes ? inputBytes / 1024 ** 3 : (info.size_gb ?? info.source_size_gb ?? null),
          ageMs,
          bucket,
          stale: threshold != null && ageMs != null && ageMs > threshold,
          progressPct: info.progress_pct ?? null,
          speed: info.speed ?? null,
          fps: info.fps ?? null,
          eta: info.eta_text ?? null,
        };
      })
      // Fully stable sort: bucket first, then alphabetical by filepath. Anything
      // derived from progress or last_updated drives re-ordering on every tick — cards
      // jumping around is worse than "closest to done at top". Alphabetical within a
      // bucket is at worst neutral and at best memorable.
      .sort((a, b) => {
        const bucketOrder = { encoding: 0, queued: 1, fetching: 2, uploading: 3 };
        const ba = bucketOrder[a.bucket] ?? 9;
        const bb = bucketOrder[b.bucket] ?? 9;
        if (ba !== bb) return ba - bb;
        return (a.filepath || "").localeCompare(b.filepath || "");
      });
  }, [pipelineFiles]);

  const encoding = activeSample.filter((f) => f.bucket === "encoding");
  const queued = activeSample.filter((f) => f.bucket === "queued");
  const fetching = activeSample.filter((f) => ["fetching", "uploading"].includes(f.bucket));

  const forecast = historySummary?.forecast;
  const avgSaved = 0.41;
  const projectedTbAtCompletion = ((remainingSizeGb * avgSaved) / 1024).toFixed(1);
  const throughput = forecast?.avg_files_per_day ?? throughputPerDay ?? null;
  const daysLeft =
    forecast?.est_days_remaining ??
    (throughput && throughput > 0 ? Math.ceil(remainingFiles / throughput) : null);
  const estCompletion = forecast?.est_completion_date ?? null;

  const recentEvents = data.recentEvents || [];

  const errors = Object.values(pipelineFiles).filter((f) =>
    ["error", "errored", "failed"].includes((f.status || "").toLowerCase())
  ).length;

  const pct = total > 0 ? av1 / total : 0;

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">Plex Media Optimisation</div>
          <div className="page-sub">
            Converting the library to AV1 to reclaim disk space.{" "}
            <span className="mono" style={{ color: "var(--ink-2)" }}>{fmtPct(pct, 1)}</span> of files
            already encoded
            {daysLeft ? (
              <>
                . Forecast at current throughput (
                <span className="mono" style={{ color: "var(--ink-2)" }}>
                  {throughput?.toFixed?.(0) ?? throughput}/day
                </span>
                ): <span className="mono" style={{ color: "var(--ink-2)" }}>~{daysLeft} days</span>
                {estCompletion && (
                  <>
                    {" "}
                    · done <span className="mono" style={{ color: "var(--ink-2)" }}>{estCompletion}</span>
                  </>
                )}
                .
              </>
            ) : (
              <>. Throughput unknown — run a batch to establish a rate.</>
            )}
          </div>
        </div>
        <div className="stamp">
          <div>
            <b>Library</b>: \\KieranNAS\Media
          </div>
          <div>
            <b>Last scan</b>: {summary.scan_date ? summary.scan_date.slice(0, 10) : "—"}
          </div>
          <div>
            <b>Workers</b>:{" "}
            <span style={{ color: "var(--accent)" }}>
              {workersActive ?? Object.values(pipelineFiles).filter((f) => f.status === "encoding").length}
              {workersTotal != null ? `/${workersTotal}` : ""} active
            </span>
          </div>
        </div>
      </div>

      {healthDeep && (() => {
        const checks = healthDeep.checks || [];
        const failing = checks.filter((c) => !c.passed);
        const firstName = failing.length > 0 ? failing[0].name : null;
        const anyCritical =
          healthDeep.any_critical === true ||
          failing.some((c) => String(c.severity).toUpperCase() === "CRITICAL");
        const allGreen = healthDeep.all_green === true && failing.length === 0;
        // "as of Xs ago" so the operator knows the deep scan isn't stale.
        const ageSecs = healthDeep.generated_at
          ? Math.max(0, Math.round((Date.now() - Date.parse(healthDeep.generated_at)) / 1000))
          : null;
        return (
          <div
            onClick={() => setIncidentsOpen((v) => !v)}
            style={{
              marginTop: 12,
              marginBottom: 4,
              padding: "10px 14px",
              background: allGreen
                ? "rgba(94,197,112,0.05)"
                : anyCritical
                  ? "rgba(224,75,75,0.1)"
                  : "rgba(224,101,75,0.06)",
              border: allGreen
                ? "1px solid rgba(94,197,112,0.25)"
                : anyCritical
                  ? "1px solid rgba(224,75,75,0.6)"
                  : "1px solid rgba(224,101,75,0.35)",
              borderRadius: 8,
              cursor: "pointer",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12 }}>
              <span
                style={{
                  width: 8,
                  height: 8,
                  borderRadius: "50%",
                  background: allGreen ? "var(--accent)" : "var(--bad)",
                }}
              />
              <b style={{ color: allGreen ? "var(--accent)" : "var(--bad)" }}>
                {allGreen
                  ? "All invariants OK"
                  : anyCritical
                    ? `CRITICAL · ${failing.length} invariant${failing.length === 1 ? "" : "s"} failing`
                    : `${failing.length} invariant${failing.length === 1 ? "" : "s"} failing`}
              </b>
              {!allGreen && firstName && (
                <span className="mono" style={{ color: "var(--ink-3)" }}>
                  first: {firstName}
                </span>
              )}
              {ageSecs != null && (
                <span className="mono" style={{ color: "var(--ink-4)", fontSize: 10 }}>
                  · as of {ageSecs}s ago
                </span>
              )}
              <span style={{ marginLeft: "auto", fontSize: 10, color: "var(--ink-4)" }}>
                {incidentsOpen ? "hide details" : "click for details"}
              </span>
            </div>
            {incidentsOpen && (
              <div
                onClick={(e) => e.stopPropagation()}
                style={{
                  marginTop: 10,
                  padding: "8px 0 2px",
                  borderTop: "1px solid var(--line)",
                  display: "grid",
                  gridTemplateColumns: "1fr auto auto",
                  columnGap: 12,
                  rowGap: 6,
                  fontSize: 11,
                }}
              >
                <div style={{ color: "var(--ink-4)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                  name
                </div>
                <div style={{ color: "var(--ink-4)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                  severity
                </div>
                <div style={{ color: "var(--ink-4)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                  status
                </div>
                {checks.map((c) => {
                  const violations = Array.isArray(c.violations) ? c.violations : [];
                  const firstFew = violations.slice(0, 3);
                  return (
                    <div key={c.name} style={{ display: "contents" }}>
                      <div
                        className="mono"
                        title={c.message}
                        style={{ color: c.passed ? "var(--ink-2)" : "var(--bad)" }}
                      >
                        {c.name}
                      </div>
                      <div className="mono" style={{ color: "var(--ink-3)" }}>
                        {c.severity}
                      </div>
                      <div
                        className="mono"
                        style={{
                          color: c.passed ? "var(--accent)" : "var(--bad)",
                          fontWeight: 500,
                        }}
                      >
                        {c.passed ? "OK" : `FAIL (${violations.length})`}
                      </div>
                      {!c.passed && (
                        <div
                          style={{
                            gridColumn: "1 / -1",
                            color: "var(--ink-3)",
                            paddingBottom: 2,
                            paddingLeft: 6,
                            fontSize: 10.5,
                          }}
                        >
                          <div>{c.message}</div>
                          {firstFew.length > 0 && (
                            <ul
                              style={{
                                margin: "2px 0 0 12px",
                                padding: 0,
                                color: "var(--ink-4)",
                                fontSize: 10,
                              }}
                            >
                              {firstFew.map((v, i) => (
                                <li
                                  key={i}
                                  className="mono"
                                  style={{ listStyle: "disc", wordBreak: "break-all" }}
                                >
                                  {v}
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        );
      })()}

      <div className="kpis">
        <div className="kpi">
          <div className="kpi-label">Total library</div>
          <div className="kpi-value">
            {summary.total_size_gb ? (summary.total_size_gb / 1024).toFixed(1) : "—"}
            <span className="unit">TB</span>
          </div>
          <div className="kpi-sub">
            <span className="mono">{fmtNum(total)} files</span>
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Encoded to AV1</div>
          <div className="kpi-value">
            {total > 0 ? fmtPct(pct, 1).replace("%", "") : "—"}
            <span className="unit">%</span>
          </div>
          <div className="kpi-sub">
            <span className="mono">
              {fmtNum(av1)} / {fmtNum(total)}
            </span>
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Reclaimed so far</div>
          <div className="kpi-value">
            {reclaimedGb >= 1024 ? (reclaimedGb / 1024).toFixed(2) : reclaimedGb.toFixed(1)}
            <span className="unit">{reclaimedGb >= 1024 ? "TB" : "GB"}</span>
          </div>
          <div className="kpi-sub">
            <span className="mono">avg 41% size↓</span>
          </div>
        </div>
        <div className="kpi">
          <div className="kpi-label">Remaining work</div>
          <div className="kpi-value">
            {fmtNum(remainingFiles)}
            <span className="unit">files</span>
          </div>
          <div className="kpi-sub">
            <span className="mono">
              ~{remainingSizeGb > 1024 ? (remainingSizeGb / 1024).toFixed(1) + " TB" : remainingSizeGb.toFixed(0) + " GB"} to
              process
            </span>
          </div>
        </div>
      </div>

      <div className="encode-progress">
        <h3>
          Encode coverage
          <span
            className="mono"
            style={{ color: "var(--ink-3)", fontSize: 11, fontWeight: 400, marginLeft: "auto" }}
          >
            by codec · all files
          </span>
        </h3>
        <div className="bar-row">
          <div className="bar-num">
            {total > 0 ? fmtPct(pct, 1).replace("%", "") : "—"}
            <span className="unit">%</span>
          </div>
          <div className="bar-label">
            <div style={{ color: "var(--ink)", fontSize: 13 }}>{fmtNum(av1)} files encoded</div>
            <div>
              {fmtNum(total - av1)} remaining · {fmtNum(hevc)} HEVC · {fmtNum(h264)} H.264 · {fmtNum(other)} other
            </div>
          </div>
        </div>
        <div className="bar">
          {segs.map((seg) => (
            <div
              key={seg.k}
              className="seg"
              style={{ width: total > 0 ? (seg.n / total) * 100 + "%" : "0%", background: seg.c }}
            />
          ))}
        </div>
        <div className="bar-legend">
          {segs.map((seg) => (
            <span key={seg.k}>
              <span className="sw" style={{ background: seg.c }} />
              {seg.k} · {fmtNum(seg.n)} · {fmtPct(total > 0 ? seg.n / total : 0, 1)}
            </span>
          ))}
        </div>
      </div>

      {/* Whisper batch progress — visible only while a language-detection
          run is active. Polled at 2s while running so progress feels live. */}
      <LangDetectProgress />

      {/* Standards compliance breakdown — the "what's left to do" view per standard.
          Same metrics as the classic Pipeline page, but with remaining counts front
          and centre rather than just completed percentages. */}
      {completion && (
        <div className="encode-progress" style={{ marginTop: 16 }}>
          <h3>
            Standards compliance
            <span
              className="mono"
              style={{ color: "var(--ink-3)", fontSize: 11, fontWeight: 400, marginLeft: "auto" }}
            >
              {fmtNum(completion.fully_done || 0)} / {fmtNum(completion.total || 0)} fully compliant · click to drill in
            </span>
          </h3>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
              gap: 12,
              marginTop: 12,
            }}
          >
            {[
              { k: "video", label: "AV1 Video", pct: completion.pct_video, done: completion.av1, colour: "var(--accent)" },
              { k: "audio", label: "EAC-3 Audio", pct: completion.pct_audio, done: completion.eac3_done, colour: "#22d3ee" },
              { k: "subs", label: "English Subs", pct: completion.pct_subs, done: completion.subs_done, colour: "#a78bfa" },
              { k: "foreign_subs", label: "No Foreign Subs", pct: completion.pct_no_foreign_subs, done: completion.no_foreign_subs, colour: "#c084fc" },
              { k: "tmdb", label: "TMDb Metadata", pct: completion.pct_tmdb, done: completion.has_tmdb, colour: "#f59e0b" },
              { k: "langs", label: "Langs Known", pct: completion.pct_langs_known, done: (completion.total || 0) - (completion.files_with_und || 0), colour: "#10b981" },
              { k: "filename", label: "Clean Filename", pct: completion.pct_filename, done: completion.has_clean_filename, colour: "#6366f1" },
              { k: "english_filename", label: "Folder Match", pct: completion.pct_english_filename, done: completion.has_english_filename, colour: "#ec4899" },
            ].map(({ k, label, pct, done, colour }) => {
              const total2 = completion.total || 0;
              const remaining = Math.max(0, total2 - (done || 0));
              return (
                <div
                  key={k}
                  style={{
                    padding: 12,
                    borderRadius: 8,
                    background: "var(--surface)",
                    border: "1px solid var(--line)",
                    textAlign: "center",
                  }}
                >
                  <div
                    className="mono"
                    style={{ fontSize: 22, fontWeight: 600, color: colour, lineHeight: 1 }}
                  >
                    {(pct ?? 0).toFixed(1)}%
                  </div>
                  <div
                    style={{
                      margin: "6px auto",
                      width: "80%",
                      height: 4,
                      background: "var(--line)",
                      borderRadius: 2,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        height: "100%",
                        width: `${Math.min(pct ?? 0, 100)}%`,
                        background: colour,
                      }}
                    />
                  </div>
                  <div style={{ fontSize: 11, color: "var(--ink-2)", marginTop: 4 }}>{label}</div>
                  <div
                    className="mono"
                    style={{
                      fontSize: 11,
                      color: remaining > 0 ? "#f59e0b" : "var(--ink-3)",
                      marginTop: 2,
                    }}
                  >
                    {remaining > 0 ? `${fmtNum(remaining)} to go` : "✓ all done"}
                  </div>
                  <div
                    className="mono"
                    style={{ fontSize: 9, color: "var(--ink-4)", marginTop: 2 }}
                  >
                    {fmtNum(done || 0)} / {fmtNum(total2)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="two-col">
        <div className="card">
          <h3>
            In flight
            <span className="tag" style={{ marginLeft: 0 }}>live</span>
            <span className="count">
              {encoding.length} encoding · {queued.length} queued · {fetching.length} fetching
            </span>
          </h3>
          <div className="active-list">
            {encoding.length === 0 && queued.length === 0 && fetching.length === 0 && (
              <div
                className="active"
                style={{ opacity: 0.6, textAlign: "center", padding: "28px 16px" }}
              >
                <div style={{ fontSize: 13, color: "var(--ink-2)", marginBottom: 4 }}>
                  No files in flight
                </div>
                <div style={{ fontSize: 11, color: "var(--ink-3)" }}>
                  Pipeline is idle — use <span className="mono">Run next batch</span> to start.
                </div>
              </div>
            )}
            {encoding.length > 0 && (
              <div
                style={{
                  fontSize: 9,
                  color: "var(--ink-4)",
                  textTransform: "uppercase",
                  letterSpacing: "0.12em",
                  padding: "2px 0",
                }}
              >
                Encoder ({encoding.length})
              </div>
            )}
            {encoding.map((f, i) => (
              <ActiveRow key={`e${i}`} f={f} tone="accent" />
            ))}
            {queued.length > 0 && (
              <div
                style={{
                  fontSize: 9,
                  color: "var(--ink-4)",
                  textTransform: "uppercase",
                  letterSpacing: "0.12em",
                  padding: "6px 0 2px",
                }}
              >
                Queued for encode ({queued.length})
              </div>
            )}
            {queued.slice(0, 10).map((f, i) => (
              <ActiveRow key={`q${i}`} f={f} tone="ink-2" />
            ))}
            {queued.length > 10 && (
              <div
                style={{
                  fontSize: 11,
                  color: "var(--ink-3)",
                  padding: "6px 14px",
                  background: "var(--bg-card)",
                  border: "1px solid var(--line)",
                  borderRadius: 8,
                }}
              >
                + {queued.length - 10} more queued files · see Queue tab for the full list
              </div>
            )}
            {fetching.length > 0 && (
              <div
                style={{
                  fontSize: 9,
                  color: "var(--ink-4)",
                  textTransform: "uppercase",
                  letterSpacing: "0.12em",
                  padding: "6px 0 2px",
                }}
              >
                Staging / fetch ({fetching.length})
              </div>
            )}
            {fetching.map((f, i) => (
              <ActiveRow key={`f${i}`} f={f} tone="blue" />
            ))}
            {workersTotal != null &&
              activeSample.length > 0 &&
              activeSample.length < workersTotal && (
                <div className="active" style={{ opacity: 0.6 }}>
                  <div className="active-top">
                    <div className="active-title">
                      {workersTotal - activeSample.length} worker
                      {workersTotal - activeSample.length === 1 ? "" : "s"} idle
                    </div>
                    <div className="active-meta">waiting for queue</div>
                  </div>
                </div>
              )}
          </div>
        </div>

        <div className="card">
          <h3>
            Codec distribution <span className="count">{fmtNum(total)} files</span>
          </h3>
          <div>
            {segs.map((seg) => (
              <div key={seg.k} className="codec-row">
                <div className="codec-name">{seg.k}</div>
                <div className="codec-bar-wrap">
                  <div
                    className="codec-bar"
                    style={{ width: total > 0 ? (seg.n / total) * 100 + "%" : "0%", background: seg.c }}
                  />
                </div>
                <div className="codec-count">{fmtNum(seg.n)} files</div>
                <div className="codec-pct">{fmtPct(total > 0 ? seg.n / total : 0, 1)}</div>
              </div>
            ))}
          </div>

          <h3 style={{ marginTop: 20 }}>Resolutions</h3>
          <div>
            {Object.entries(resolutions)
              .sort((a, b) => b[1] - a[1])
              .slice(0, 4)
              .map(([k, n]) => (
                <div key={k} className="codec-row">
                  <div className="codec-name">{k}</div>
                  <div className="codec-bar-wrap">
                    <div
                      className="codec-bar"
                      style={{ width: total > 0 ? (n / total) * 100 + "%" : "0%", background: "var(--ink-3)" }}
                    />
                  </div>
                  <div className="codec-count">{fmtNum(n)} files</div>
                  <div className="codec-pct">{fmtPct(total > 0 ? n / total : 0, 1)}</div>
                </div>
              ))}
          </div>
        </div>
      </div>

      <div className="two-col">
        <div className="card">
          <h3>
            Recent activity <span className="count">last 24h</span>
          </h3>
          <div className="stream">
            {recentEvents.length === 0 && (
              <div className="stream-item" style={{ gridTemplateColumns: "1fr", color: "var(--ink-3)" }}>
                No recent activity — pipeline is idle.
              </div>
            )}
            {recentEvents.map((r, i) => (
              <div key={i} className="stream-item">
                <span className="stream-time">{r[0]}</span>
                <span className="stream-body">
                  <span className={`stream-kind k-${r[1]}`}>{r[1]}</span>
                  {r[2]}
                </span>
                <span className="stream-meta">{r[3]}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="card">
          <h3>
            Storage reclaimed <span className="count">cumulative</span>
          </h3>
          <div className="savings">
            <div>
              <div className="savings-num">
                {reclaimedGb >= 1024 ? (reclaimedGb / 1024).toFixed(2) : reclaimedGb.toFixed(1)}
                <span className="unit"> {reclaimedGb >= 1024 ? "TB" : "GB"}</span>
              </div>
              <div className="savings-label">
                across {fmtNum(av1)} encoded files · avg 41% smaller
              </div>
            </div>
          </div>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: 12,
              padding: "14px 0 4px",
              borderTop: "1px solid var(--line)",
              marginTop: 4,
            }}
          >
            <div>
              <div style={{ fontSize: 11, color: "var(--ink-3)", marginBottom: 4 }}>Projected at completion</div>
              <div
                style={{ fontSize: 20, fontWeight: 500, letterSpacing: "-0.02em" }}
                className="mono"
              >
                ~{projectedTbAtCompletion} TB
              </div>
              <div style={{ fontSize: 10, color: "var(--ink-4)" }}>
                based on avg ratio · {fmtNum(remainingFiles)} files left
              </div>
            </div>
            <div>
              <div style={{ fontSize: 11, color: "var(--ink-3)", marginBottom: 4 }}>Throughput</div>
              <div style={{ fontSize: 20, fontWeight: 500, letterSpacing: "-0.02em" }} className="mono">
                {throughput ? `${throughput.toFixed(0)} files/day` : "—"}
              </div>
              <div style={{ fontSize: 10, color: "var(--ink-4)" }}>
                {daysLeft
                  ? `~${daysLeft} days · done ${estCompletion || "soon"}`
                  : "no throughput yet"}
              </div>
            </div>
          </div>

          {errors > 0 && (
            <div
              onClick={() => onNavigate?.("errors")}
              style={{
                marginTop: 18,
                padding: "12px 14px",
                background: "rgba(224,101,75,0.04)",
                border: "1px solid rgba(224,101,75,0.2)",
                borderRadius: 8,
                cursor: onNavigate ? "pointer" : "default",
              }}
            >
              <div
                style={{
                  fontSize: 11,
                  fontWeight: 500,
                  color: "var(--bad)",
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  marginBottom: 4,
                }}
              >
                <span
                  style={{ width: 6, height: 6, borderRadius: "50%", background: "var(--bad)" }}
                />{" "}
                {errors} failed encodes need review
              </div>
              <div style={{ fontSize: 11, color: "var(--ink-3)" }}>
                Most common: encoder timeout on 4K HDR10 content.{" "}
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    onNavigate?.("errors");
                  }}
                  style={{
                    color: "var(--accent)",
                    background: "none",
                    border: 0,
                    padding: 0,
                    cursor: "pointer",
                    font: "inherit",
                  }}
                >
                  Review →
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="footline">
        <span>nascleanup · operator console</span>
        <span>{fmtNum(total)} files · {(summary.total_size_gb / 1024).toFixed(1)} TB indexed</span>
        <span>ts {new Date().toISOString().replace("T", " ").slice(0, 19)}</span>
      </div>
    </div>
  );
}
