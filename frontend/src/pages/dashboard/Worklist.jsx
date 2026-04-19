import { useState } from "react";
import { api } from "../../lib/api";
import { codecCount, codecKey, codecLabel, fmtNum, fmtSize, prettyTitle } from "./helpers";

export function Worklist({ data, pipelineData, onNavigate }) {
  const [tab, setTab] = useState("encode");
  const [expanded, setExpanded] = useState(null);

  // Full library — matching sets here drive both the counts shown AND what gets queued
  // when the user clicks Queue batch. Previews use the same sets, just sliced.
  const allFiles = data.files || data.topTargets;
  const bigHevcAll = allFiles.filter((f) => f.size_gb > 20 && codecKey(f.codec) === "hevc");
  const hevcAll = allFiles.filter((f) => codecKey(f.codec) === "hevc");
  const h264All = allFiles.filter((f) => codecKey(f.codec) === "h264");
  const hevcTotal = codecCount(data.codecs, "hevc");
  const h264Total = codecCount(data.codecs, "h264");
  const bigHevcTotal = bigHevcAll.length;

  const erroredFiles = Object.entries(pipelineData?.files || {})
    .filter(([, f]) => ["error", "errored", "failed"].includes((f.status || "").toLowerCase()))
    .map(([path]) => path);
  const errors = erroredFiles.length;

  const noEngAll = data.noEngAll || data.noEng || [];
  const noSubsAll = data.noSubsAll || data.noSubs || [];
  const noEngCount = noEngAll.length;
  const noSubsCount = noSubsAll.length;

  // `full` is the list that gets queued when the user clicks Queue batch.
  // `detail` is the 5-item preview shown when the card is expanded.
  const encodeProblems = [
    bigHevcTotal > 0
      ? {
          id: "enc-big",
          sev: "hi",
          title: "Large HEVC remuxes (>20 GB)",
          sub: "Highest storage reclaim potential",
          n: bigHevcTotal,
          action: "Queue batch",
          full: bigHevcAll,
          detail: bigHevcAll.slice(0, 5),
        }
      : null,
    hevcTotal > 0
      ? {
          id: "enc-hevc",
          sev: "med",
          title: "HEVC files not yet encoded",
          sub: "Standard priority · mostly 1080p TV episodes",
          n: hevcTotal,
          action: "Queue batch",
          full: hevcAll,
          detail: hevcAll.slice(0, 5),
        }
      : null,
    h264Total > 0
      ? {
          id: "enc-h264",
          sev: "med",
          title: "Legacy H.264 files",
          sub: "Re-encode to AV1 for consistency + space",
          n: h264Total,
          action: "Queue batch",
          full: h264All,
          detail: h264All.slice(0, 5),
        }
      : null,
    errors > 0
      ? {
          id: "enc-err",
          sev: "hi",
          title: "Failed encodes needing review",
          sub: "Reset to pending in bulk, or open Library filtered to errored",
          n: errors,
          action: "Review",
          full: erroredFiles.map((path) => ({ filepath: path })),
          detail: null,
        }
      : null,
  ].filter(Boolean);

  const qualityProblems = [
    noEngCount > 0
      ? {
          id: "q-lang",
          sev: "med",
          title: "Files without English subtitles",
          sub: "Foreign audio detected but no ENG sub track",
          n: noEngCount,
          action: "Fetch subs",
          full: noEngAll,
          detail: noEngAll.slice(0, 5),
        }
      : null,
    noSubsCount > 0
      ? {
          id: "q-nosubs",
          sev: "lo",
          title: "Files with no subtitle tracks at all",
          sub: "Handled by Bazarr · deep link from inspector",
          n: noSubsCount,
          action: "Fetch subs",
          full: noSubsAll,
          detail: noSubsAll.slice(0, 5),
        }
      : null,
  ].filter(Boolean);

  const problems = {
    encode: encodeProblems,
    quality: qualityProblems,
    housekeeping: [],
  };

  const counts = Object.fromEntries(
    Object.entries(problems).map(([k, v]) => [k, v.reduce((s, p) => s + p.n, 0)])
  );

  const remainingSizeGb = data.remainingSizeGb || 0;
  const projectedTb = (remainingSizeGb * 0.42) / 1024;

  return (
    <div className="view">
      <div className="work-hero">
        <div style={{ position: "relative", zIndex: 1 }}>
          <h2>
            What needs <em>attention</em> today
          </h2>
          <p>
            Your library grouped by actionable problems instead of raw files. Tackle the top items first — the{" "}
            {bigHevcTotal} largest HEVC remuxes alone could reclaim ~
            {fmtSize(bigHevcAll.reduce((s, f) => s + f.size_gb * 0.42, 0))}.
          </p>
        </div>
        <div className="work-metric">
          <div className="n">{fmtNum(counts.encode + counts.quality + counts.housekeeping)}</div>
          <div className="l">
            items across {problems.encode.length + problems.quality.length + problems.housekeeping.length}{" "}
            {problems.encode.length + problems.quality.length + problems.housekeeping.length === 1
              ? "issue"
              : "issues"}
          </div>
        </div>
      </div>

      <div className="work-tabs">
        {[
          ["encode", "Encode queue"],
          ["quality", "Quality gaps"],
          ["housekeeping", "Housekeeping"],
        ].map(([k, l]) => (
          <button key={k} className={`work-tab ${tab === k ? "on" : ""}`} onClick={() => setTab(k)}>
            {l} <span className="c">{fmtNum(counts[k])}</span>
          </button>
        ))}
      </div>

      <div className="problem-grid">
        {problems[tab].length === 0 && (
          <div
            className="card"
            style={{ textAlign: "center", padding: "32px 20px", color: "var(--ink-3)", fontSize: 12 }}
          >
            Nothing flagged in this group — clean slate.
          </div>
        )}
        {problems[tab].map((p) => (
          <div
            key={p.id}
            className={`problem ${p.sev}`}
            onClick={() => setExpanded((e) => (e === p.id ? null : p.id))}
          >
            <div className="problem-row">
              <div className="problem-icon">{p.sev === "hi" ? "!!" : p.sev === "med" ? "!" : "·"}</div>
              <div className="problem-body">
                <div className="title">{p.title}</div>
                <div className="sub">{p.sub}</div>
              </div>
              <div className="problem-num">{fmtNum(p.n)}</div>
              <button
                className="problem-action"
                onClick={async (e) => {
                  e.stopPropagation();
                  // Use the FULL matching set — `p.detail` is a 5-item preview.
                  const paths = (p.full || p.detail || [])
                    .map((f) => f.filepath)
                    .filter(Boolean);
                  try {
                    if (p.action === "Queue batch") {
                      if (paths.length === 0) {
                        window.notify?.({
                          kind: "warn",
                          title: "No paths to queue",
                          body: "No matching files in the current library index.",
                        });
                        return;
                      }
                      if (
                        paths.length > 50 &&
                        !confirm(
                          `Queue ${paths.length} files for re-encode? This writes reencode.json and the running pipeline will pick them up.`
                        )
                      ) {
                        return;
                      }
                      const r = await api.setReencode(paths);
                      window.notify?.({
                        kind: "good",
                        title: `Queued ${fmtNum(paths.length)} files for re-encode`,
                        body: `${p.title} · reencode.json written (${r?.files ?? paths.length} total)`,
                      });
                    } else if (p.action === "Fetch subs") {
                      const bazarr =
                        JSON.parse(localStorage.getItem("nc.settings") || "{}").bazarrUrl ||
                        "http://192.168.4.42:6767";
                      window.open(bazarr, "_blank", "noreferrer");
                      window.notify?.({
                        kind: "info",
                        title: "Opened Bazarr",
                        body: `${fmtNum(p.n)} files flagged · use Bazarr to search + download`,
                      });
                    } else if (p.action === "Prune") {
                      const r = await api.compactState();
                      window.notify?.({
                        kind: "good",
                        title: `State compacted`,
                        body: `${r?.removed ?? "?"} terminal entries removed`,
                      });
                    } else if (p.action === "Clean up") {
                      const r = await api.startProcess("strip_tags");
                      window.notify?.({
                        kind: "good",
                        title: "Cleanup started",
                        body: `strip_tags pid ${r?.pid ?? "?"}`,
                      });
                    } else if (p.action === "Review") {
                      // Two-choice: bulk reset-to-pending, or jump to the Errors view
                      const choice = confirm(
                        `${paths.length} errored files.\n\nOK: reset them all to pending (the running pipeline will retry them)\nCancel: open the Errors view to review each one`
                      );
                      if (choice) {
                        const r = await api.resetErrors();
                        window.notify?.({
                          kind: "good",
                          title: `Reset ${r?.reset ?? paths.length} errored files to pending`,
                          body: "The running pipeline will pick them up again.",
                        });
                      } else if (onNavigate) {
                        onNavigate("errors");
                      } else {
                        window.notify?.({
                          kind: "info",
                          title: "Errors view not reachable from here",
                          body: "Use the sidebar to open the Errors view.",
                        });
                      }
                    } else if (p.action === "Flag") {
                      window.notify?.({
                        kind: "info",
                        title: "Flag action not wired",
                        body: "No backend endpoint for 'flag' — expand the card and queue individually.",
                      });
                    }
                  } catch (err) {
                    window.notify?.({
                      kind: "bad",
                      title: `${p.action} failed`,
                      body: err?.detail || String(err.message || err),
                    });
                  }
                }}
              >
                {p.action} →
              </button>
            </div>
            {expanded === p.id && p.detail && p.detail.length > 0 && (
              <div className="problem-detail">
                <div
                  style={{
                    fontSize: 10,
                    color: "var(--ink-3)",
                    textTransform: "uppercase",
                    letterSpacing: "0.1em",
                    marginBottom: 10,
                  }}
                >
                  Preview · top offenders
                </div>
                {p.detail.map((f, i) => (
                  <div key={i} className="row">
                    <div className="t">
                      <div className="n">{prettyTitle(f.filename)}</div>
                      <div className="p">{f.filepath}</div>
                    </div>
                    <span className={`tag ${codecKey(f.codec)}`}>{codecLabel(f.codec)}</span>
                    <span className="tag res">{f.res || "—"}</span>
                    <span className="num" style={{ minWidth: 70 }}>{fmtSize(f.size_gb)}</span>
                  </div>
                ))}
                <div style={{ display: "flex", gap: 8, marginTop: 14 }}>
                  <button
                    className="ins-btn"
                    style={{ padding: "8px 14px" }}
                    onClick={async (e) => {
                      e.stopPropagation();
                      const paths = (p.detail || []).map((f) => f.filepath).filter(Boolean);
                      if (paths.length === 0) {
                        window.notify?.({ kind: "warn", title: "No offenders in preview", body: "" });
                        return;
                      }
                      try {
                        const r = await api.setReencode(paths);
                        window.notify?.({
                          kind: "good",
                          title: `Queued ${fmtNum(paths.length)} preview files`,
                          body: `${p.title} · reencode.json · ${r?.files ?? paths.length} total`,
                        });
                      } catch (err) {
                        window.notify?.({
                          kind: "bad",
                          title: "Queue failed",
                          body: err?.detail || String(err.message || err),
                        });
                      }
                    }}
                  >
                    Queue preview only · {fmtNum((p.detail || []).length)}
                  </button>
                  {(p.full || []).length > (p.detail || []).length && (
                    <button
                      className="ins-btn primary"
                      style={{ padding: "8px 14px" }}
                      onClick={async (e) => {
                        e.stopPropagation();
                        const paths = (p.full || []).map((f) => f.filepath).filter(Boolean);
                        if (paths.length === 0) return;
                        if (
                          paths.length > 50 &&
                          !confirm(`Queue all ${paths.length} ${p.title.toLowerCase()}?`)
                        )
                          return;
                        try {
                          const r = await api.setReencode(paths);
                          window.notify?.({
                            kind: "good",
                            title: `Queued ${fmtNum(paths.length)} files`,
                            body: `${p.title} · reencode.json · ${r?.files ?? paths.length} total`,
                          });
                        } catch (err) {
                          window.notify?.({
                            kind: "bad",
                            title: "Queue failed",
                            body: err?.detail || String(err.message || err),
                          });
                        }
                      }}
                    >
                      Queue all · {fmtNum((p.full || []).length)} →
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>
        ))}
      </div>

      <div style={{ marginTop: 28 }}>
        <div className="card">
          <h3>
            Projected impact <span className="count">if you action everything in Encode queue</span>
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 20, padding: "8px 0" }}>
            <div>
              <div
                style={{
                  fontSize: 10,
                  color: "var(--ink-3)",
                  textTransform: "uppercase",
                  letterSpacing: "0.1em",
                }}
              >
                Space freed
              </div>
              <div
                style={{
                  fontSize: 28,
                  fontWeight: 500,
                  letterSpacing: "-0.02em",
                  color: "var(--accent)",
                }}
                className="mono"
              >
                ~{projectedTb.toFixed(1)} TB
              </div>
            </div>
            <div>
              <div
                style={{
                  fontSize: 10,
                  color: "var(--ink-3)",
                  textTransform: "uppercase",
                  letterSpacing: "0.1em",
                }}
              >
                Runtime
              </div>
              <div
                style={{ fontSize: 28, fontWeight: 500, letterSpacing: "-0.02em" }}
                className="mono"
              >
                —
              </div>
              <div style={{ fontSize: 10, color: "var(--ink-4)", marginTop: 4 }}>
                set after first batch completes
              </div>
            </div>
            <div>
              <div
                style={{
                  fontSize: 10,
                  color: "var(--ink-3)",
                  textTransform: "uppercase",
                  letterSpacing: "0.1em",
                }}
              >
                Files touched
              </div>
              <div
                style={{ fontSize: 28, fontWeight: 500, letterSpacing: "-0.02em" }}
                className="mono"
              >
                {fmtNum(hevcTotal + h264Total + errors)}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
