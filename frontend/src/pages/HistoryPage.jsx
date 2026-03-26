import { useState, useEffect } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  AreaChart, Area, CartesianGrid, LineChart, Line, Legend,
} from "recharts";
import { PALETTE } from "../theme";
import { api } from "../lib/api";
import { StatCard } from "../components/StatCard";
import { SectionTitle } from "../components/SectionTitle";

function fmtGB(bytes) {
  const gb = bytes / (1024 ** 3);
  if (gb >= 1000) return `${(gb / 1000).toFixed(2)} TB`;
  if (gb >= 1) return `${gb.toFixed(1)} GB`;
  return `${(gb * 1024).toFixed(0)} MB`;
}

function fmtDuration(secs) {
  if (secs >= 86400) {
    const d = Math.floor(secs / 86400);
    const h = Math.round((secs % 86400) / 3600);
    return `${d}d ${h}h`;
  }
  if (secs >= 3600) return `${(secs / 3600).toFixed(1)}h`;
  if (secs >= 60) return `${Math.round(secs / 60)}m`;
  return `${Math.round(secs)}s`;
}

const tooltipStyle = {
  backgroundColor: PALETTE.surface,
  border: `1px solid ${PALETTE.border}`,
  borderRadius: 8,
  fontSize: 12,
  color: PALETTE.text,
};

export function HistoryPage() {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getHistorySummary().then((data) => {
      setSummary(data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) return <div style={{ color: PALETTE.textMuted, padding: 40 }}>Loading history...</div>;
  if (!summary || !summary.days?.length) {
    return <div style={{ color: PALETTE.textMuted, padding: 40 }}>No encode history yet. History is recorded as files complete encoding.</div>;
  }

  const { days, tiers, totals, forecast } = summary;

  // Build cumulative data for area chart
  let cumSaved = 0;
  const cumData = days.map((d) => {
    cumSaved += d.saved_bytes;
    return { date: d.date.slice(5), cumSavedGB: +(cumSaved / (1024 ** 3)).toFixed(2) };
  });

  // Per-day chart data
  const dayData = days.map((d) => ({
    date: d.date.slice(5),
    files: d.count,
    savedGB: +(d.saved_bytes / (1024 ** 3)).toFixed(2),
  }));

  // Tier comparison data
  const tierData = Object.entries(tiers).map(([name, t]) => ({
    name,
    count: t.count,
    avgCompression: +((1 - t.avg_compression_ratio) * 100).toFixed(1),
    avgTimeMins: +(t.avg_encode_time_secs / 60).toFixed(1),
    savedGB: +(t.saved_bytes / (1024 ** 3)).toFixed(1),
  })).sort((a, b) => b.savedGB - a.savedGB);

  // Speed trend (per-day average MB/s)
  const speedData = days.filter((d) => d.encode_time_secs > 0).map((d) => ({
    date: d.date.slice(5),
    mbPerSec: +(d.input_bytes / d.encode_time_secs / (1024 ** 2)).toFixed(1),
    minsPerGB: d.input_bytes > 0 ? +((d.encode_time_secs / 60) / (d.input_bytes / (1024 ** 3))).toFixed(1) : 0,
  }));

  return (
    <div>
      {/* Summary stats */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 24 }}>
        <StatCard label="Total Encodes" value={totals.entries.toLocaleString()} />
        <StatCard label="Space Saved" value={fmtGB(totals.saved_bytes)} colour={PALETTE.green} />
        <StatCard label="Total Input" value={fmtGB(totals.input_bytes)} />
        <StatCard label="Encode Time" value={fmtDuration(totals.encode_time_secs)} />
        {forecast && (
          <StatCard
            label="Est. Completion"
            value={forecast.est_completion_date}
            sub={`${forecast.remaining_files.toLocaleString()} files remaining \u00b7 ~${forecast.avg_files_per_day}/day`}
            colour={PALETTE.accent}
          />
        )}
      </div>

      {/* Encodes per day */}
      <SectionTitle>Encodes Per Day</SectionTitle>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 24 }}>
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={dayData}>
            <CartesianGrid strokeDasharray="3 3" stroke={PALETTE.border} />
            <XAxis dataKey="date" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} />
            <YAxis tick={{ fill: PALETTE.textMuted, fontSize: 11 }} />
            <Tooltip contentStyle={tooltipStyle} />
            <Bar dataKey="files" fill={PALETTE.accent} radius={[4, 4, 0, 0]} name="Files encoded" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Cumulative savings */}
      <SectionTitle>Cumulative Space Saved</SectionTitle>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 24 }}>
        <ResponsiveContainer width="100%" height={220}>
          <AreaChart data={cumData}>
            <CartesianGrid strokeDasharray="3 3" stroke={PALETTE.border} />
            <XAxis dataKey="date" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} />
            <YAxis tick={{ fill: PALETTE.textMuted, fontSize: 11 }} unit=" GB" />
            <Tooltip contentStyle={tooltipStyle} />
            <Area type="monotone" dataKey="cumSavedGB" stroke={PALETTE.green} fill={PALETTE.green} fillOpacity={0.15} name="Total saved (GB)" />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Savings by tier */}
      <SectionTitle>Savings by Resolution Tier</SectionTitle>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 24 }}>
        <ResponsiveContainer width="100%" height={Math.max(200, tierData.length * 40)}>
          <BarChart data={tierData} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke={PALETTE.border} />
            <XAxis type="number" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} unit=" GB" />
            <YAxis type="category" dataKey="name" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} width={100} />
            <Tooltip contentStyle={tooltipStyle} />
            <Bar dataKey="savedGB" fill={PALETTE.green} radius={[0, 4, 4, 0]} name="Saved (GB)" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Compression ratio by tier */}
      <SectionTitle>Compression Ratio by Tier</SectionTitle>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 24 }}>
        <ResponsiveContainer width="100%" height={Math.max(200, tierData.length * 40)}>
          <BarChart data={tierData} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke={PALETTE.border} />
            <XAxis type="number" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} unit="%" domain={[0, 80]} />
            <YAxis type="category" dataKey="name" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} width={100} />
            <Tooltip contentStyle={tooltipStyle} />
            <Bar dataKey="avgCompression" fill={PALETTE.purple} radius={[0, 4, 4, 0]} name="Avg reduction %" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Encode speed trend */}
      {speedData.length > 1 && (
        <>
          <SectionTitle>Encode Speed Trend</SectionTitle>
          <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 24 }}>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={speedData}>
                <CartesianGrid strokeDasharray="3 3" stroke={PALETTE.border} />
                <XAxis dataKey="date" tick={{ fill: PALETTE.textMuted, fontSize: 11 }} />
                <YAxis tick={{ fill: PALETTE.textMuted, fontSize: 11 }} />
                <Tooltip contentStyle={tooltipStyle} />
                <Legend wrapperStyle={{ fontSize: 12, color: PALETTE.textMuted }} />
                <Line type="monotone" dataKey="mbPerSec" stroke={PALETTE.accent} strokeWidth={2} dot={false} name="MB/s (input)" />
                <Line type="monotone" dataKey="minsPerGB" stroke={PALETTE.accentWarm} strokeWidth={2} dot={false} name="min/GB" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </>
      )}

      {/* Tier detail table */}
      <SectionTitle>Tier Details</SectionTitle>
      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, overflow: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13, fontFamily: "'JetBrains Mono', monospace" }}>
          <thead>
            <tr style={{ borderBottom: `1px solid ${PALETTE.border}`, color: PALETTE.textMuted, fontSize: 11, textTransform: "uppercase", letterSpacing: "0.06em" }}>
              <th style={{ textAlign: "left", padding: "8px 12px" }}>Tier</th>
              <th style={{ textAlign: "right", padding: "8px 12px" }}>Files</th>
              <th style={{ textAlign: "right", padding: "8px 12px" }}>Saved</th>
              <th style={{ textAlign: "right", padding: "8px 12px" }}>Reduction</th>
              <th style={{ textAlign: "right", padding: "8px 12px" }}>Avg Time</th>
            </tr>
          </thead>
          <tbody>
            {tierData.map((t) => (
              <tr key={t.name} style={{ borderBottom: `1px solid ${PALETTE.border}22` }}>
                <td style={{ padding: "8px 12px", color: PALETTE.text }}>{t.name}</td>
                <td style={{ padding: "8px 12px", textAlign: "right", color: PALETTE.text }}>{t.count}</td>
                <td style={{ padding: "8px 12px", textAlign: "right", color: PALETTE.green }}>{t.savedGB} GB</td>
                <td style={{ padding: "8px 12px", textAlign: "right", color: PALETTE.purple }}>{t.avgCompression}%</td>
                <td style={{ padding: "8px 12px", textAlign: "right", color: PALETTE.textMuted }}>{t.avgTimeMins} min</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
