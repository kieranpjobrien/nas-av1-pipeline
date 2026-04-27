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
  if (!res.ok) {
    // FastAPI HTTPException bodies carry the real reason as {detail: "..."} —
    // parse and rethrow an Error that preserves that text. Callers that care
    // (start/stop/kill etc.) can read e.status / e.detail and render a proper
    // toast instead of the generic "409 Conflict".
    let detail = `${res.status} ${res.statusText}`;
    try {
      const payload = await res.json();
      if (payload?.detail) detail = payload.detail;
      else if (payload?.error) detail = payload.error;
    } catch {
      /* non-JSON error body, keep the status line */
    }
    const err = new Error(detail);
    err.status = res.status;
    err.detail = detail;
    throw err;
  }
  return res.json();
}

async function deleteJSON(path) {
  const res = await fetch(`${BASE}${path}`, { method: "DELETE" });
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const payload = await res.json();
      if (payload?.detail) detail = payload.detail;
    } catch {
      /* non-JSON body */
    }
    const err = new Error(detail);
    err.status = res.status;
    err.detail = detail;
    throw err;
  }
  return res.json();
}

async function putJSON(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const payload = await res.json();
      if (payload?.detail) detail = payload.detail;
      else if (payload?.error) detail = payload.error;
    } catch {
      /* non-JSON error body */
    }
    const err = new Error(detail);
    err.status = res.status;
    err.detail = detail;
    throw err;
  }
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
  addForce: (path) => postJSON("/control/priority/force", { path, action: "add" }),
  removeForce: (path) => postJSON("/control/priority/force", { path, action: "remove" }),
  setGentle: (gentle) => putJSON("/control/gentle", gentle),
  getReencode: () => fetchJSON("/control/reencode"),
  setReencode: (files, patterns = {}) => putJSON("/control/reencode", { files, patterns }),
  getCustomTags: () => fetchJSON("/control/custom-tags"),
  setCustomTags: (keywords) => putJSON("/control/custom-tags", { keywords }),
  resetErrors: () => postJSON("/pipeline/reset-errors", {}),
  forceAccept: (path) => postJSON("/pipeline/force-accept", { path }),
  getProcessStatus: (name) => fetchJSON(`/process/${name}/status`),
  startProcess: (name) => postJSON(`/process/${name}/start`, {}),
  stopProcess: (name) => postJSON(`/process/${name}/stop`, {}),
  killProcess: (name) => postJSON(`/process/${name}/kill`, {}),
  getProcessLogs: (name, lastN = 50) => fetchJSON(`/process/${name}/logs?last_n=${lastN}`),
  getDismissed: (section) => fetchJSON(`/dismissed/${section}`).then((d) => d.paths || []).catch(() => []),
  setDismissed: (section, paths) => putJSON(`/dismissed/${section}`, { paths }),
  getDuplicates: () => fetchJSON("/duplicates"),
  deleteFile: (path) => postJSON("/file/delete", { path }),
  getGpu: () => fetchJSON("/gpu"),
  getHostStats: () => fetchJSON("/host-stats"),
  getFileSiblings: (path) => fetchJSON(`/file/siblings?path=${encodeURIComponent(path)}`),
  getHistory: (days = 0, limit = 500) => fetchJSON(`/history?days=${days}&limit=${limit}`),
  getHistorySummary: () => fetchJSON("/history/summary"),
  getHealth: () => fetchJSON("/health"),
  getHealthDeep: () => fetchJSON("/health-deep"),
  getPlexAudit: () => fetchJSON("/plex-audit"),
  compactState: () => postJSON("/pipeline/compact", {}),
  vmafCheck: (path) => postJSON("/vmaf/check", { path }),
  getFileDetail: (path) => fetchJSON(`/file-detail?path=${encodeURIComponent(path)}`),
  getConfig: () => fetchJSON("/config"),
  setConfig: (overrides) => putJSON("/config", overrides),
  getMkvpropedAvailable: () => fetchJSON("/mkvpropedit-available"),
  getLibraryCompletion: () => fetchJSON("/library-completion"),
  quickWins: () => postJSON("/quick-wins", {}),
  getForceList: () => fetchJSON("/control/force-list"),
  getCompletionMissing: (category) => fetchJSON(`/completion-missing?category=${category}`),
  renameFile: (path, newName) => postJSON("/file/rename", { path, new_name: newName }),
  // Upgrades recommender (Claude-backed taste scorer + bluray.com gap)
  getUpgradesRanked: (limit = 100, libraryType = "all") =>
    fetchJSON(`/upgrades/ranked?limit=${limit}&library_type=${libraryType}`),
  getUpgradeSeeds: () => fetchJSON("/upgrades/seeds"),
  saveUpgradeSeeds: (bundle) => postJSON("/upgrades/seeds", bundle),
  addUpgradeSeed: (tier, seed) => postJSON("/upgrades/seeds/add", { tier, seed }),
  removeUpgradeSeed: (tier, title, year) =>
    deleteJSON(`/upgrades/seeds/${tier}?title=${encodeURIComponent(title)}&year=${year}`),
  rescoreUpgrade: (req) => postJSON("/upgrades/rescore", req),
  // Flagged files (FLAGGED_FOREIGN_AUDIO / FLAGGED_UNDETERMINED / FLAGGED_MANUAL)
  getFlaggedFiles: () => fetchJSON("/flagged"),
  flaggedAction: (filepath, action) =>
    postJSON("/flagged/action", { filepath, action }),
  getRadarrProfiles: () => fetchJSON("/upgrades/radarr/profiles"),
  radarrUpgrade: (req) => postJSON("/upgrades/radarr/upgrade", req),
  getSonarrProfiles: () => fetchJSON("/upgrades/sonarr/profiles"),
  sonarrUpgrade: (req) => postJSON("/upgrades/sonarr/upgrade", req),
  // Diagnostics — size-vs-duration scatter for spotting corrupt/sample files
  getSizeVsDuration: () => fetchJSON("/diagnostics/size-vs-duration"),
};
