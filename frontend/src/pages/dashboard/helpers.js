export const fmtSize = (gb) => {
  if (gb == null) return "—";
  if (gb >= 1000) return (gb / 1024).toFixed(2) + " TB";
  if (gb >= 10) return gb.toFixed(1) + " GB";
  return gb.toFixed(2) + " GB";
};

export const fmtDur = (s) => {
  if (!s) return "—";
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  return h ? `${h}h ${m.toString().padStart(2, "0")}m` : `${m}m`;
};

export const fmtBitrate = (kbps) => {
  if (!kbps) return "—";
  if (kbps >= 1000) return (kbps / 1000).toFixed(1) + " Mb/s";
  return kbps + " kb/s";
};

export const fmtNum = (n) => (n == null ? "—" : n.toLocaleString());
export const fmtPct = (n, d = 0) => ((n || 0) * 100).toFixed(d) + "%";

export const prettyTitle = (filename) => {
  if (!filename) return "";
  let n = filename.replace(/\.(mkv|mp4|avi|mov|m4v|webm)$/i, "");
  n = n.replace(/\[[^\]]*\]/g, "").replace(/\([^)]*\)/g, "");
  n = n.replace(
    /\.(1080p|2160p|720p|480p|BluRay|WEB-DL|WEBRip|HDTV|REMUX|HDR|DV|x265|x264|HEVC|AV1|AAC|DTS|DDP|DD|TrueHD|Atmos|H\.?264|H\.?265|10bit|8bit)\b.*/i,
    ""
  );
  n = n.replace(/\./g, " ").replace(/_/g, " ").trim();
  return n;
};

export const libraryOf = (path) => {
  if (!path) return "";
  const m = path.match(/[\\/](Movies|Series|TV|Music|Concerts|Documentaries|Anime|Audiobooks|Books)[\\/]/i);
  return m ? m[1] : "";
};

export const codecKey = (raw) => {
  const s = (raw || "").toLowerCase();
  if (s.includes("av1")) return "av1";
  if (s.includes("hevc") || s.includes("h.265") || s.includes("h265") || s.includes("x265")) return "hevc";
  if (s.includes("h.264") || s.includes("h264") || s.includes("avc") || s.includes("x264")) return "h264";
  return "other";
};

export const codecLabel = (raw) => {
  const k = codecKey(raw);
  return (
    { av1: "AV1", hevc: "HEVC", h264: "H.264", other: (raw || "?").replace(/\s*\(.*\)/, "").toUpperCase() }[k]
  );
};

export const codecCount = (dict, want) => {
  let n = 0;
  for (const [k, v] of Object.entries(dict || {})) {
    if (codecKey(k) === want) n += v;
  }
  return n;
};

export const resKey = (raw) => {
  const s = (raw || "").toLowerCase();
  if (s.includes("4k") || s.includes("2160")) return "4k";
  if (s.includes("1080")) return "1080p";
  if (s.includes("720")) return "720p";
  if (s.includes("480")) return "480p";
  return s || "?";
};

export const resCount = (dict, want) => {
  let n = 0;
  for (const [k, v] of Object.entries(dict || {})) {
    if (resKey(k) === want) n += v;
  }
  return n;
};

export const shortName = (name, max = 60) => {
  if (!name || name.length <= max) return name;
  return name.slice(0, max - 3) + "…";
};

// Normalize a file row from the /media-report payload into the shape the design expects.
// Scanner emits `filepath`; older snapshots used `file_path` — read either.
export const normalizeFile = (f) => {
  const path = f.filepath || f.file_path || "";
  return {
    filename: f.filename || (path ? path.split(/[\\/]/).pop() : ""),
    filepath: path,
    codec: f.video?.codec || f.codec || "?",
    res: f.video?.resolution_class || f.res || "",
    size_gb: f.file_size_gb ?? f.size_gb ?? 0,
    bitrate: f.overall_bitrate_kbps ?? f.bitrate ?? 0,
    hdr: !!(f.video?.hdr || f.hdr),
    dur: f.duration_seconds ?? f.dur ?? 0,
    library: f.library_type || libraryOf(path),
    audio: (f.audio_streams || f.audio || []).map((a) => ({
      codec: a.codec,
      lang: a.language || a.lang,
      ch: a.channels || a.ch,
      br: a.bitrate_kbps || a.br,
      lossless: !!a.lossless,
    })),
    subs: (f.subtitle_streams || f.subs || []).map((s) => ({
      codec: s.codec,
      lang: s.language || s.lang,
    })),
    // External sidecar subs recorded by tools/scanner.py. Bazarr writes `.en.srt` etc.
    // next to the media file; the Worklist sub-gap check reads from here.
    externalSubs: (f.external_subtitles || []).map((s) => ({
      filename: s.filename,
      language: s.language,
      flags: s.flags || [],
      ext: s.ext,
    })),
  };
};

// Derive codec/res dicts from raw file array.
export const aggregateBy = (files, fn) => {
  const out = {};
  for (const f of files) {
    const k = fn(f);
    if (!k) continue;
    out[k] = (out[k] || 0) + 1;
  }
  return out;
};

// Detect library-policy issues for a file (AV1 video, Opus audio, ENG subs).
// Returns an array of {scope:'video'|'audio'|'sub', idx?, short, why, level?}.
export const detectIssues = (f) => {
  if (!f) return [];
  const out = [];
  const vk = codecKey(f.codec);
  if (vk === "h264") {
    out.push({ scope: "video", short: "wrong codec", why: "Policy is AV1 for all video. H.264 should be re-encoded." });
  } else if (vk === "hevc") {
    out.push({
      scope: "video",
      short: "wrong codec",
      why: "Policy is AV1 for all video. HEVC is allowed only as a passthrough fallback.",
    });
  }
  (f.audio || []).forEach((a, i) => {
    const c = (a.codec || "").toLowerCase();
    if (/ac.?3|dts|truehd|eac3|ddp|dd\b/.test(c)) {
      out.push({
        scope: "audio",
        idx: i,
        short: "lossy legacy",
        why: `${a.codec} is a legacy surround codec — policy is opus @ 192k or passthrough only if lossless.`,
      });
    } else if (c && !/opus|flac|pcm/.test(c) && !a.lossless) {
      out.push({
        scope: "audio",
        idx: i,
        short: "non-opus",
        why: `Audio codec ${a.codec} is not in the allowed set (opus, flac, pcm).`,
        level: "warn",
      });
    }
  });
  (f.subs || []).forEach((s, i) => {
    const lang = (s.lang || "").toLowerCase();
    const keep = ["eng", "en"];
    if (lang && !keep.includes(lang)) {
      out.push({
        scope: "sub",
        idx: i,
        short: "strip",
        why: `Subtitle track "${(lang || "und").toUpperCase()}" is not in the keep-list (eng). Will be stripped on encode.`,
      });
    }
  });
  return out;
};

// Host capability map + job type definitions for the run-batch routing matrix.
// NAS has no GPU, so encode is locked to local.
export const HOSTS = {
  local: {
    label: "this machine",
    host: "workstation · rtx 4070 · nvenc + svt-av1",
    can: ["encode", "probe", "scan", "cleanup", "move"],
  },
  nas: {
    label: "NAS KieranNAS",
    host: "192.168.4.42 · no gpu · fast disk",
    can: ["scan", "cleanup", "move", "probe"],
  },
};

export const JOB_TYPES = [
  { k: "encode", l: "Re-encode", sub: "svt-av1-psy · cpu/gpu heavy", icon: "\u25B6" },
  { k: "scan", l: "Library scan", sub: "walk + ffprobe · disk heavy", icon: "\u27F2" },
  { k: "probe", l: "Stream analysis", sub: "ffprobe single file · light", icon: "\u24D8" },
  { k: "cleanup", l: "Cleanup / prune", sub: "orphans, stale state, sidecars", icon: "\u2715" },
  { k: "move", l: "Move / archive", sub: "file ops between pools", icon: "\u2192" },
];

export const DEFAULT_ROUTING = {
  encode: "local",
  scan: "nas",
  cleanup: "nas",
  move: "nas",
  probe: "nas",
};
