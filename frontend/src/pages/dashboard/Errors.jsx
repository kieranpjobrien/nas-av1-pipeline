import { useState } from "react";
import { api } from "../../lib/api";
import { fmtSize, prettyTitle } from "./helpers";

function deriveErrors(pipelineData) {
  const files = pipelineData?.files || {};
  return Object.entries(files)
    .filter(([, info]) => ["error", "errored", "failed"].includes((info.status || "").toLowerCase()))
    .slice(0, 50)
    .map(([path, info]) => ({
      file: path,
      kind: info.error_kind || info.error || "encoder failure",
      attempts: info.attempts || info.retries || 1,
      age: info.error_age || "—",
      size: info.size_gb || info.source_size_gb || 0,
    }));
}

export function Errors({ pipelineData }) {
  const rows = deriveErrors(pipelineData);
  const total = rows.length;
  const timeouts = rows.filter((r) => /timeout/i.test(r.kind)).length;
  const [retrying, setRetrying] = useState(false);

  const retryAll = async () => {
    if (retrying) return;
    setRetrying(true);
    try {
      const r = await api.resetErrors();
      const n = r?.reset ?? total;
      window.notify?.({
        kind: "good",
        title: `Requeued ${n} ${n === 1 ? "file" : "files"}`,
        body: "All errored files reset to pending — pipeline will pick them up on the next cycle.",
      });
    } catch (e) {
      window.notify?.({ kind: "bad", title: "Retry-all failed", body: String(e.message || e) });
    } finally {
      setRetrying(false);
    }
  };

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">Errors</div>
          <div className="page-sub">
            {total === 0
              ? "No failed jobs right now."
              : `${total} ${total === 1 ? "file" : "files"} failed encoding${
                  timeouts > 0 ? ` · ${timeouts} timeout${timeouts === 1 ? "" : "s"}` : ""
                }.`}
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 10, alignItems: "flex-end" }}>
          {total > 0 && (
            <button
              className="ins-btn primary"
              onClick={retryAll}
              disabled={retrying}
              style={{ padding: "10px 18px", minWidth: 160, justifyContent: "center" }}
            >
              {retrying ? "Retrying…" : `Retry all ${total}`}
            </button>
          )}
          <div className="stamp">
            <div><b>Total</b>: {total}</div>
            {timeouts > 0 && <div><b>Timeouts</b>: {timeouts}</div>}
          </div>
        </div>
      </div>

      {total === 0 && (
        <div
          className="card"
          style={{ textAlign: "center", padding: "48px 20px", color: "var(--ink-3)" }}
        >
          <div style={{ fontSize: 14, color: "var(--ink-2)", marginBottom: 6 }}>Pipeline is clean</div>
          <div style={{ fontSize: 12 }}>Nothing has failed — there's nothing to retry here.</div>
        </div>
      )}

      <div className="file-table">
        <div
          className="ft-head"
          style={{ gridTemplateColumns: "1fr 180px 70px 70px 90px 120px" }}
        >
          <span>File</span>
          <span>Error</span>
          <span style={{ textAlign: "center" }}>Tries</span>
          <span style={{ textAlign: "right" }}>Age</span>
          <span style={{ textAlign: "right" }}>Size</span>
          <span style={{ textAlign: "right" }}>Action</span>
        </div>
        {rows.map((r, i) => (
          <div
            key={i}
            className="ft-row"
            style={{ gridTemplateColumns: "1fr 180px 70px 70px 90px 120px" }}
          >
            <div className="ft-name">
              <div className="n">{prettyTitle(r.file)}</div>
              <div className="p">{r.file}</div>
            </div>
            <div style={{ fontSize: 11, color: "var(--bad)" }}>{r.kind}</div>
            <div className="num">{r.attempts}</div>
            <div className="num">{r.age}</div>
            <div className="num">{fmtSize(r.size)}</div>
            <div style={{ display: "flex", gap: 6, justifyContent: "flex-end" }}>
              <button
                className="chip"
                onClick={async (e) => {
                  e.stopPropagation();
                  try {
                    await api.forceAccept(r.file);
                    window.notify?.({
                      kind: "good",
                      title: "Force-accepted",
                      body: prettyTitle(r.file),
                    });
                  } catch (err) {
                    window.notify?.({ kind: "bad", title: "Retry failed", body: String(err.message || err) });
                  }
                }}
                title="Force-accept this file (clears error state and requeues)"
              >
                Retry
              </button>
              <button
                className="chip"
                onClick={async (e) => {
                  e.stopPropagation();
                  try {
                    const cur = await api.getSkip().catch(() => ({ paths: [] }));
                    const paths = Array.from(new Set([...(cur.paths || []), r.file]));
                    await api.setSkip(paths);
                    window.notify?.({
                      kind: "good",
                      title: "Added to skip list",
                      body: prettyTitle(r.file),
                    });
                  } catch (err) {
                    window.notify?.({ kind: "bad", title: "Skip failed", body: String(err.message || err) });
                  }
                }}
                title="Add to skip.json so the pipeline stops retrying it"
              >
                Skip
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
