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
        <div className="stamp">
          <div><b>Total</b>: {total}</div>
          {timeouts > 0 && <div><b>Timeouts</b>: {timeouts}</div>}
        </div>
      </div>

      {total === 0 ? (
        <div
          className="card"
          style={{ textAlign: "center", padding: "48px 20px", color: "var(--ink-3)" }}
        >
          <div style={{ fontSize: 14, color: "var(--ink-2)", marginBottom: 6 }}>Pipeline is clean</div>
          <div style={{ fontSize: 12 }}>Nothing has failed — there's nothing to retry here.</div>
        </div>
      ) : timeouts > 0 ? (
        <div
          className="card"
          style={{
            marginBottom: 16,
            background: "rgba(224,101,75,0.04)",
            borderColor: "rgba(224,101,75,0.2)",
          }}
        >
          <div style={{ display: "flex", gap: 14, alignItems: "center" }}>
            <div style={{ fontSize: 32, color: "var(--bad)" }}>⚠</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 14, fontWeight: 500 }}>Bulk retry with fallback preset?</div>
              <div style={{ fontSize: 12, color: "var(--ink-3)" }}>
                Drop speed preset 8 → 6 for the {timeouts} timeout {timeouts === 1 ? "case" : "cases"}. Expected ~80%
                success rate, roughly +40% per-file runtime.
              </div>
            </div>
            <button
              className="ins-btn primary"
              onClick={async () => {
                try {
                  const r = await api.resetErrors();
                  window.notify?.({
                    kind: "good",
                    title: "Errors cleared",
                    body: `${r?.cleared ?? total} ${total === 1 ? "file" : "files"} requeued — pipeline will pick them up`,
                  });
                } catch (e) {
                  window.notify?.({ kind: "bad", title: "Reset failed", body: String(e.message || e) });
                }
              }}
            >
              Retry {total} {total === 1 ? "file" : "files"}
            </button>
          </div>
        </div>
      ) : null}

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
