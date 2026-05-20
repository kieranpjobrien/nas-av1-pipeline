import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import { codecKey, codecLabel, fmtNum, fmtSize, prettyTitle } from "./helpers";

const statusLabel = (s) => {
  const k = (s || "").toLowerCase();
  if (["error", "errored", "failed"].includes(k)) return { led: "var(--bad)", label: "error" };
  if (["encoding", "analyzing", "fetching", "uploading", "processing"].includes(k))
    return { led: "var(--accent)", label: k };
  if (["queued", "pending", "fetched"].includes(k)) return { led: "var(--ink-2)", label: k };
  if (["completed", "done", "encoded"].includes(k)) return { led: "var(--good)", label: "done" };
  return { led: "var(--ink-4)", label: s || "—" };
};

export function Queue({ data, pipelineData, onFileOpen }) {
  const [force, setForce] = useState([]);
  const [priorityPaths, setPriorityPaths] = useState(new Set());
  useEffect(() => {
    api.getForceList().then((r) => setForce(r.items || [])).catch(() => setForce([]));
    // Mirror the backend's priority bump (priority.json -> paths).
    // The pipeline sorts the queue priority-first / smallest-first within
    // the priority bucket; this view should reflect the same order so the
    // user sees what's actually about to run, not just biggest pending.
    api
      .getPriority()
      .then((p) => setPriorityPaths(new Set((p?.paths || []).map((s) => s.toLowerCase()))))
      .catch(() => setPriorityPaths(new Set()));
  }, []);

  const pipelineFiles = pipelineData?.files || {};

  // Rich-data lookup map: data.files is the full media-report library, not
  // just the top-200 (topTargets). Pre-fix we looked up via topTargets which
  // meant any pending file ranked below #200 by size showed codec="?", res="—",
  // size=0 — that's what the user noticed at the bottom of the queue.
  const reportByPath = new Map();
  for (const f of data?.files || []) {
    if (f?.filepath) reportByPath.set(f.filepath, f);
  }

  // Build queue from pipeline state: everything not completed/replaced/skipped.
  // Sort: (1) priority paths first, smallest-first within the bucket (matches
  // the backend's priority sort introduced 2026-05-20); (2) the rest by size desc.
  const rows = Object.entries(pipelineFiles)
    .filter(([, info]) => {
      const s = (info.status || "").toLowerCase();
      return !["completed", "done", "encoded", "replaced", "skipped"].includes(s);
    })
    .map(([path, info]) => {
      const reportEntry = reportByPath.get(path);
      return {
        path,
        filename: path.split(/[\\/]/).pop(),
        status: info.status || "pending",
        stage: info.stage,
        tier: info.tier,
        codec: info.codec || reportEntry?.codec || "?",
        res: info.res_key || reportEntry?.res || "",
        size_gb: reportEntry?.size_gb ?? 0,
        is_priority: priorityPaths.has(path.toLowerCase()),
      };
    })
    .sort((a, b) => {
      // Priority items first.
      if (a.is_priority !== b.is_priority) return a.is_priority ? -1 : 1;
      // Within priority: smallest-first (matches backend's quick-wins intent).
      if (a.is_priority && b.is_priority) return (a.size_gb || 0) - (b.size_gb || 0);
      // Outside priority: largest-first.
      return (b.size_gb || 0) - (a.size_gb || 0);
    })
    .slice(0, 50);

  const forcePaths = new Set((force || []).map((f) => f.filepath));
  const active = rows.filter((r) =>
    ["encoding", "analyzing", "fetching", "uploading", "processing"].includes(
      (r.status || "").toLowerCase()
    )
  ).length;

  return (
    <div className="view">
      <div className="page-head">
        <div>
          <div className="page-title">Encode queue</div>
          <div className="page-sub">
            Files currently tracked by the pipeline, in encode order: priority (smallest-first) then
            non-priority (largest-first). Forced items bubble to the top. Double-click any row to open
            its drawer (delete, requeue, override).
          </div>
        </div>
        <div className="stamp">
          <div><b>Tracked</b>: {fmtNum(Object.keys(pipelineFiles).length)}</div>
          <div><b>Active</b>: {active}</div>
          <div><b>Forced</b>: {force.length}</div>
        </div>
      </div>

      {rows.length === 0 && (
        <div className="card" style={{ textAlign: "center", padding: "48px 20px", color: "var(--ink-3)" }}>
          <div style={{ fontSize: 14, color: "var(--ink-2)", marginBottom: 6 }}>Pipeline is clean</div>
          <div style={{ fontSize: 12 }}>
            Nothing pending — run a batch from the topbar or force individual files from the Library inspector.
          </div>
        </div>
      )}

      {rows.length > 0 && (
        <div className="file-table">
          <div className="ft-head">
            <span>File</span>
            <span style={{ textAlign: "center" }}>Codec</span>
            <span style={{ textAlign: "center" }}>Res</span>
            <span style={{ textAlign: "right" }}>Size</span>
            <span style={{ textAlign: "right" }}>Priority</span>
            <span>Status</span>
          </div>
          {rows.map((f) => {
            const st = statusLabel(f.status);
            const forced = forcePaths.has(f.path);
            return (
              <div
                key={f.path}
                className="ft-row"
                onDoubleClick={() => onFileOpen?.(f.path)}
              >
                <div className="ft-name">
                  <div className="n">{prettyTitle(f.filename)}</div>
                  <div className="p">{f.path}</div>
                </div>
                <div style={{ textAlign: "center" }}>
                  <span className={`tag ${codecKey(f.codec)}`}>{codecLabel(f.codec)}</span>
                </div>
                <div style={{ textAlign: "center" }}>
                  <span className="tag res">{f.res || "—"}</span>
                </div>
                <div className="num">{fmtSize(f.size_gb)}</div>
                <div className="num">{forced ? "forced" : f.is_priority ? "priority" : f.tier || "—"}</div>
                <div className="status-cell">
                  <span className="led" style={{ background: st.led }} />
                  {st.label}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
