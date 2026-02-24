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
    if (!map[val]) map[val] = { name: val, count: 0, size_gb: 0 };
    map[val].count += 1;
    map[val].size_gb += f.file_size_gb;
  });
  return Object.values(map).sort((a, b) => b.size_gb - a.size_gb);
}
