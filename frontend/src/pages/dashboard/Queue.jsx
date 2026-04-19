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
  useEffect(() => {
    api.getForceList().then((r) => setForce(r.items || [])).catch(() => setForce([]));
  }, []);

  const pipelineFiles = pipelineData?.files || {};

  // Build queue from pipeline state: everything not completed/replaced/skipped, ordered by force first then by size.
  const rows = Object.entries(pipelineFiles)
    .filter(([, info]) => {
      const s = (info.status || "").toLowerCase();
      return !["completed", "done", "encoded", "replaced", "skipped"].includes(s);
    })
    .map(([path, info]) => {
      const reportEntry = (data?.topTargets || []).find((f) => f.filepath === path);
      return {
        path,
        filename: path.split(/[\\/]/).pop(),
        status: info.status || "pending",
        stage: info.stage,
        tier: info.tier,
        codec: info.codec || reportEntry?.codec || "?",
        res: info.res_key || reportEntry?.res || "",
        size_gb: reportEntry?.size_gb ?? 0,
      };
    })
    .sort((a, b) => (b.size_gb || 0) - (a.size_gb || 0))
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
            Files currently tracked by the pipeline, highest-size first. Forced items (from the Library inspector)
            bubble to the top of the force stack.
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
                <div className="num">{forced ? "forced" : f.tier || "—"}</div>
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
