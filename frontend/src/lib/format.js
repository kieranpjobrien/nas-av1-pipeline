export function fmt(gb) {
  if (gb >= 1000) return `${(gb / 1000).toFixed(2)} TB`;
  if (gb >= 1) return `${gb.toFixed(1)} GB`;
  return `${(gb * 1024).toFixed(0)} MB`;
}

export function fmtNum(n) {
  return n.toLocaleString();
}

export function aggregate(files, key) {
  const map = {};
  files.forEach((f) => {
    const val = key(f);
    if (val == null) return;
    if (!map[val]) map[val] = { name: val, count: 0, size_gb: 0, duration_hrs: 0 };
    map[val].count += 1;
    map[val].size_gb += f.file_size_gb;
    map[val].duration_hrs += (f.duration_seconds || 0) / 3600;
  });
  return Object.values(map).sort((a, b) => b.size_gb - a.size_gb);
}

export function fmtHrs(hrs) {
  if (hrs >= 24) {
    const d = Math.floor(hrs / 24);
    const h = Math.round(hrs % 24);
    return `${d}d ${h}h`;
  }
  if (hrs >= 1) return `${hrs.toFixed(1)}h`;
  return `${Math.round(hrs * 60)}m`;
}
