const BASE = "/api";

async function fetchJSON(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function postJSON(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function putJSON(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

export const api = {
  getPipeline: () => fetchJSON("/pipeline"),
  getMediaReport: () => fetchJSON("/media-report"),
  getControlStatus: () => fetchJSON("/control/status"),
  getSkip: () => fetchJSON("/control/skip"),
  getPriority: () => fetchJSON("/control/priority"),
  getGentle: () => fetchJSON("/control/gentle"),
  pause: (type) => postJSON("/control/pause", { type }),
  resume: () => postJSON("/control/resume", {}),
  setSkip: (paths) => putJSON("/control/skip", { paths }),
  setPriority: (paths) => putJSON("/control/priority", { paths }),
  setGentle: (gentle) => putJSON("/control/gentle", gentle),
  getReencode: () => fetchJSON("/control/reencode"),
  setReencode: (files, patterns = {}) => putJSON("/control/reencode", { files, patterns }),
  getCustomTags: () => fetchJSON("/control/custom-tags"),
  setCustomTags: (keywords) => putJSON("/control/custom-tags", { keywords }),
  resetErrors: () => postJSON("/pipeline/reset-errors", {}),
  getProcessStatus: (name) => fetchJSON(`/process/${name}/status`),
  startProcess: (name) => postJSON(`/process/${name}/start`, {}),
  stopProcess: (name) => postJSON(`/process/${name}/stop`, {}),
  killProcess: (name) => postJSON(`/process/${name}/kill`, {}),
  getProcessLogs: (name, lastN = 50) => fetchJSON(`/process/${name}/logs?last_n=${lastN}`),
  getDismissed: (section) => fetchJSON(`/dismissed/${section}`).then((d) => d.paths || []).catch(() => []),
  setDismissed: (section, paths) => putJSON(`/dismissed/${section}`, { paths }),
  getGpu: () => fetchJSON("/gpu"),
  getHistory: (days = 0, limit = 500) => fetchJSON(`/history?days=${days}&limit=${limit}`),
  getHistorySummary: () => fetchJSON("/history/summary"),
  getHealth: () => fetchJSON("/health"),
  getPlexAudit: () => fetchJSON("/plex-audit"),
  compactState: () => postJSON("/pipeline/compact", {}),
  vmafCheck: (path) => postJSON("/vmaf/check", { path }),
  getFileDetail: (path) => fetchJSON(`/file-detail?path=${encodeURIComponent(path)}`),
  getConfig: () => fetchJSON("/config"),
  setConfig: (overrides) => putJSON("/config", overrides),
};
