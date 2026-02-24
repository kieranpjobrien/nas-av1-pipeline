import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from "recharts";
import { PALETTE } from "../theme";
import { fmt, fmtNum } from "../lib/format";

export function PieSection({ data, colourFn, title, valueKey = "size_gb", labelFn }) {
  const total = data.reduce((s, d) => s + d[valueKey], 0);
  return (
    <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, flex: "1 1 400px", minWidth: 340 }}>
      <div style={{ color: PALETTE.text, fontSize: 15, fontWeight: 600, marginBottom: 16 }}>{title}</div>
      <div style={{ display: "flex", alignItems: "center", gap: 24, flexWrap: "wrap" }}>
        <ResponsiveContainer width="50%" height={220} minWidth={180}>
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
        <div style={{ flex: 1 }}>
          {data.map((d, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6, fontSize: 13 }}>
              <div style={{ width: 10, height: 10, borderRadius: 3, background: colourFn(d.name), flexShrink: 0 }} />
              <span style={{ color: PALETTE.text, flex: 1 }}>{d.name}</span>
              <span style={{ color: PALETTE.textMuted, fontFamily: "'JetBrains Mono', monospace", fontSize: 12 }}>
                {fmtNum(d.count)} · {fmt(d[valueKey])}
                {total > 0 && ` · ${((d[valueKey] / total) * 100).toFixed(1)}%`}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
