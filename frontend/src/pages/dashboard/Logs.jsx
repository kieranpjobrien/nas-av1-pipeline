import { useEffect, useState } from "react";
import { api } from "../../lib/api";

const SOURCES = [
  ["pipeline", "pipeline"],
  ["scanner", "scanner"],
  ["gap_filler", "gap_filler"],
  ["detect_languages", "detect_languages"],
  ["strip_tags", "strip_tags"],
  ["duplicates", "duplicates"],
];

// Tail log lines of the form "LEVEL — message" and guess a level. We don't control log formatting
// from the pipeline, so we fall back to keyword matching.
function guessLevel(line) {
  const l = line.toLowerCase();
  if (/error|fail|exception|traceback|exit\s*1\b|timeout/.test(l)) return "error";
  if (/warn|slow|retry|timeout/.test(l)) return "warn";
  if (/debug/.test(l)) return "debug";
  return "info";
}

export function Logs() {
  const [source, setSource] = useState("pipeline");
  const [filter, setFilter] = useState("all");
  const [lines, setLines] = useState([]);
  const [status, setStatus] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const [logResp, st] = await Promise.all([
          api.getProcessLogs(source, 200).catch((e) => ({ __err: e.message || String(e) })),
          api.getProcessStatus(source).catch(() => null),
        ]);
        if (cancelled) return;
        if (logResp?.__err) {
          setErr(logResp.__err);
          setLines([]);
        } else {
          setErr(null);
          setLines(Array.isArray(logResp?.lines) ? logResp.lines : Array.isArray(logResp) ? logResp : []);
        }
        setStatus(st);
      } catch (e) {
        if (!cancelled) setErr(e.message || String(e));
      }
    };
    tick();
    const id = setInterval(tick, 2500);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [source]);

  const rows = lines.map((raw, i) => {
    const level = guessLevel(raw);
    const ts = raw.match(/^(\d{2}:\d{2}:\d{2})/)?.[1] || "";
    const msg = ts ? raw.slice(ts.length).trimStart() : raw;
    return { i, ts, level, msg, raw };
  });
  const filtered = filter === "all" ? rows : rows.filter((r) => r.level === filter);

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">Logs</div>
          <div className="page-sub">
            Tail of the selected process's stdout. Lines are buffered in-memory (deque, max 500) by the process
            manager — once the process exits the buffer drains on the next start.
          </div>
        </div>
        <div className="stamp">
          <div>
            <b>Source</b>: <span className="mono">{source}</span>
          </div>
          <div>
            <b>Status</b>:{" "}
            <span
              style={{
                color:
                  status?.status === "running"
                    ? "var(--good)"
                    : status?.status === "error"
                      ? "var(--bad)"
                      : "var(--ink-2)",
              }}
            >
              {status?.status ?? "—"}
            </span>
          </div>
          <div>
            <b>Lines</b>: {filtered.length} / {rows.length}
          </div>
        </div>
      </div>

      <div className="lib-toolbar">
        <div
          style={{ display: "flex", gap: 6, alignItems: "center", padding: "4px 8px", border: "1px solid var(--line)", borderRadius: 100 }}
        >
          <span
            style={{ fontSize: 10, color: "var(--ink-3)", textTransform: "uppercase", letterSpacing: "0.1em", marginRight: 4 }}
          >
            Source
          </span>
          {SOURCES.map(([k, l]) => (
            <button key={k} className={`chip ${source === k ? "on" : ""}`} onClick={() => setSource(k)}>
              {l}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          {["all", "info", "warn", "error", "debug"].map((l) => (
            <button key={l} className={`chip ${filter === l ? "on" : ""}`} onClick={() => setFilter(l)}>
              {l}
            </button>
          ))}
        </div>
      </div>

      <div className="logs-pane">
        {err && <div style={{ padding: 16, color: "var(--bad)", fontSize: 12 }}>{err}</div>}
        {!err && rows.length === 0 && (
          <div style={{ padding: 16, color: "var(--ink-3)", fontSize: 12 }}>
            No log lines yet. Start the {source} process to see output.
          </div>
        )}
        {filtered.map((r) => (
          <div key={r.i} className={`log-line lvl-${r.level}`}>
            <span className="log-ts">{r.ts || "—"}</span>
            <span className={`log-lvl lvl-${r.level}`}>{r.level.toUpperCase()}</span>
            <span className="log-src">{source}</span>
            <span className="log-msg">{r.msg}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
