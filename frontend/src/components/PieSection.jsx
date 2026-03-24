import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from "recharts";
import { PALETTE } from "../theme";
import { fmt, fmtNum, fmtHrs } from "../lib/format";

export function PieSection({ data, colourFn, title, valueKey = "size_gb", labelFn, showHours = true }) {
  const total = data.reduce((s, d) => s + d[valueKey], 0);
  const mono = { fontFamily: "'JetBrains Mono', monospace", fontSize: 11 };
  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, flex: "1 1 400px", minWidth: 340 }}>
      <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>{title}</div>
      <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
        <div style={{ width: 200, minWidth: 180, flexShrink: 0 }}>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={data} dataKey={valueKey} cx="50%" cy="50%" outerRadius={90} innerRadius={50} strokeWidth={1} stroke={PALETTE.bg}>
                {data.map((d, i) => <Cell key={i} fill={colourFn(d.name)} />)}
              </Pie>
              <Tooltip
                contentStyle={{ background: PALETTE.surfaceLight, border: `1px solid ${PALETTE.border}`, borderRadius: 8, color: PALETTE.text, fontSize: 13 }}
                formatter={(v, name) => [labelFn ? labelFn(v) : fmt(v), name]}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
        <table style={{ flex: 1, borderCollapse: "collapse", fontSize: 12 }}>
          <tbody>
            {data.map((d, i) => (
              <tr key={i}>
                <td style={{ padding: "3px 0", width: 14 }}>
                  <div style={{ width: 10, height: 10, borderRadius: 3, background: colourFn(d.name) }} />
                </td>
                <td style={{ color: PALETTE.text, padding: "3px 8px 3px 4px", whiteSpace: "nowrap" }}>{d.name}</td>
                <td style={{ ...mono, color: PALETTE.textMuted, textAlign: "right", padding: "3px 6px" }}>{fmtNum(d.count)}</td>
                <td style={{ ...mono, color: PALETTE.textMuted, textAlign: "right", padding: "3px 6px" }}>{fmt(d[valueKey])}</td>
                {showHours && d.duration_hrs != null && (
                  <td style={{ ...mono, color: PALETTE.textMuted, textAlign: "right", padding: "3px 6px" }}>{fmtHrs(d.duration_hrs)}</td>
                )}
                <td style={{ ...mono, color: PALETTE.textMuted, textAlign: "right", padding: "3px 0" }}>
                  {total > 0 ? `${((d[valueKey] / total) * 100).toFixed(1)}%` : ""}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
