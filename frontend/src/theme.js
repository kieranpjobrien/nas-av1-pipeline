export const PALETTE = {
  bg: "#0a0e17",
  surface: "#111827",
  surfaceLight: "#1e293b",
  border: "#2d3a4f",
  text: "#e2e8f0",
  textMuted: "#8899aa",
  accent: "#3b82f6",
  accentWarm: "#f59e0b",
  green: "#10b981",
  red: "#ef4444",
  purple: "#8b5cf6",
  pink: "#ec4899",
  cyan: "#06b6d4",
  orange: "#f97316",
};

export const CODEC_COLOURS = {
  "H.264": "#3b82f6",
  "HEVC (H.265)": "#10b981",
  "AV1": "#8b5cf6",
  "MPEG-4": "#f97316",
  "MPEG-2": "#ef4444",
  "VP9": "#06b6d4",
  "VC-1": "#ec4899",
  "WMV": "#f59e0b",
};

export const RES_COLOURS = {
  "4K": "#8b5cf6",
  "1080p": "#3b82f6",
  "720p": "#10b981",
  "480p": "#f59e0b",
  "SD": "#ef4444",
};

export const AUDIO_COLOURS = {
  "TrueHD": "#8b5cf6",
  "DTS": "#3b82f6",
  "FLAC": "#06b6d4",
  "E-AC-3": "#10b981",
  "AC-3": "#f59e0b",
  "AAC": "#f97316",
  "Opus": "#ec4899",
  "MP3": "#ef4444",
  "PCM": "#64748b",
  "PCM 24-bit": "#94a3b8",
};

export function getCodecColour(codec) {
  return CODEC_COLOURS[codec] || "#64748b";
}
export function getResColour(res) {
  return RES_COLOURS[res] || "#64748b";
}
export function getAudioColour(codec) {
  return AUDIO_COLOURS[codec] || "#64748b";
}
