import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import { prettyTitle } from "./helpers";

export function History() {
  const [rows, setRows] = useState(null);
  const [summary, setSummary] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancelled = false;
    api
      .getHistory(0, 200)
      .then((data) => {
        if (cancelled) return;
        const items = Array.isArray(data?.entries) ? data.entries : Array.isArray(data) ? data : [];
        setRows(
          items
            .slice()
            .reverse()
            .slice(0, 200)
            .map((h) => {
              const inputBytes = h.input_bytes ?? 0;
              const outputBytes = h.output_bytes ?? 0;
              const savedBytes = h.saved_bytes ?? Math.max(0, inputBytes - outputBytes);
              return {
                name:
                  (h.filepath || "").split(/[\\/]/).pop() ||
                  h.filename ||
                  "—",
                before: inputBytes / 1024 ** 3,
                after: outputBytes / 1024 ** 3,
                saved: savedBytes / 1024 ** 3,
                dur: Math.round((h.encode_time_secs ?? 0) / 60),
                date: h.timestamp ? h.timestamp.slice(5, 10) : "—",
                time: h.timestamp ? h.timestamp.slice(11, 16) : "",
                tier: h.res_key || "",
              };
            })
        );
      })
      .catch((e) => {
        if (!cancelled) {
          setErr(e.message || String(e));
          setRows([]);
        }
      });

    api
      .getHistorySummary()
      .then((s) => {
        if (!cancelled) setSummary(s);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  const displayRows = rows || [];
  const totalFiles = summary?.totals?.entries ?? displayRows.length;
  const totalSavedBytes = summary?.totals?.saved_bytes ?? 0;
  const totalSavedTb = totalSavedBytes ? (totalSavedBytes / 1024 ** 4).toFixed(2) : "0.00";
  const totalSavedGb = totalSavedBytes ? (totalSavedBytes / 1024 ** 3).toFixed(0) : null;
  const avgRatio = (() => {
    const input = summary?.totals?.input_bytes;
    const output = summary?.totals?.output_bytes;
    if (input && output) return (output / input).toFixed(2);
    return "—";
  })();
  const forecast = summary?.forecast;

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">History</div>
          <div className="page-sub">
            Every completed encode, newest first. Export this table for accounting how much space the job actually
            reclaimed.
          </div>
        </div>
        <div className="stamp">
          <div><b>Encoded</b>: {totalFiles.toLocaleString()} files</div>
          <div>
            <b>Saved</b>:{" "}
            {totalSavedBytes < 0
              ? `${(totalSavedBytes / 1024 ** 3).toFixed(0)} GB (growth)`
              : `${totalSavedTb} TB`}
          </div>
          <div><b>Avg ratio</b>: {avgRatio}</div>
        </div>
      </div>

      {forecast && (
        <div className="card" style={{ marginBottom: 16 }}>
          <h3>
            Forecast
            <span className="count">
              based on {summary?.days?.length ?? "?"} days of history
            </span>
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 20 }}>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                Remaining
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {forecast.remaining_files?.toLocaleString() ?? "—"}
              </div>
            </div>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                Throughput
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {forecast.avg_files_per_day?.toFixed(0) ?? "—"}/day
              </div>
            </div>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                Days left
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {forecast.est_days_remaining ?? "—"}
              </div>
            </div>
            <div>
              <div style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                Est. completion
              </div>
              <div style={{ fontSize: 22, fontWeight: 500 }} className="mono">
                {forecast.est_completion_date ?? "—"}
              </div>
            </div>
          </div>
        </div>
      )}

      {err && (
        <div className="card" style={{ color: "var(--bad)", fontSize: 12 }}>
          Failed to load history: {err}
        </div>
      )}
      {!rows && !err && (
        <div style={{ padding: 40, fontSize: 12, color: "var(--ink-3)" }}>Loading history…</div>
      )}
      {rows && rows.length === 0 && !err && (
        <div className="card" style={{ textAlign: "center", padding: "48px 20px", color: "var(--ink-3)" }}>
          <div style={{ fontSize: 14, color: "var(--ink-2)", marginBottom: 6 }}>No encode history yet</div>
          <div style={{ fontSize: 12 }}>
            Once the pipeline finishes its first encode, <span className="mono">encode_history.jsonl</span> will start
            populating.
          </div>
        </div>
      )}

      {rows && (
        <div className="file-table">
          <div
            className="ft-head"
            style={{ gridTemplateColumns: "1fr 90px 90px 90px 90px 90px" }}
          >
            <span>File</span>
            <span style={{ textAlign: "right" }}>Before</span>
            <span style={{ textAlign: "right" }}>After</span>
            <span style={{ textAlign: "right" }}>Saved</span>
            <span style={{ textAlign: "right" }}>Runtime</span>
            <span style={{ textAlign: "right" }}>Finished</span>
          </div>
          {displayRows.map((r, i) => (
            <div
              key={i}
              className="ft-row"
              style={{ gridTemplateColumns: "1fr 90px 90px 90px 90px 90px" }}
            >
              <div className="ft-name">
                <div className="n">{prettyTitle(r.name)}</div>
              </div>
              <div className="num">{r.before.toFixed(2)} GB</div>
              <div className="num">{r.after.toFixed(2)} GB</div>
              <div className="num" style={{ color: "var(--accent)" }}>−{r.saved.toFixed(2)} GB</div>
              <div className="num">{r.dur}m</div>
              <div className="num" style={{ color: "var(--ink-3)" }}>
                {r.date} {r.time}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
