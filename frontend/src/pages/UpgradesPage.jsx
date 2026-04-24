import { useState, useEffect, useCallback } from "react";
import { PALETTE } from "../theme";
import { api } from "../lib/api";

/**
 * UpgradesPage — the "which files are worth targeting better sources for?" pane.
 *
 * Pulls /api/upgrades/ranked (bluray.com gap × Claude taste score) and renders:
 *   1. A ranked candidate table, sortable by the combined score.
 *   2. A seed-list editor (tune the LLM's taste calibration).
 *   3. Per-row "rescore" action that hits Claude on demand (~5-15s per call).
 *
 * The page treats unscored films (taste_score == null) as a soft prompt: they
 * appear in the list but with a "score me" cell so the user can bulk-fill via
 * the CLI (`uv run python -m tools.upgrades taste-rescore`) or trigger one at
 * a time from the UI.
 */
export function UpgradesPage({ onFileClick }) {
  const [candidates, setCandidates] = useState([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState(null);
  const [limit, setLimit] = useState(100);
  const [libraryType, setLibraryType] = useState(
    () => localStorage.getItem("nc.upgrades.libraryType") || "all",
  );
  const [rescoringTitle, setRescoringTitle] = useState(null);

  const [seeds, setSeeds] = useState(null);
  const [seedsOpen, setSeedsOpen] = useState(false);

  const setLibraryTypePersist = (t) => {
    setLibraryType(t);
    localStorage.setItem("nc.upgrades.libraryType", t);
  };

  const load = useCallback(async () => {
    try {
      setLoading(true);
      setErr(null);
      const r = await api.getUpgradesRanked(limit, libraryType);
      setCandidates(r.candidates || []);
    } catch (e) {
      setErr(e.message || String(e));
    } finally {
      setLoading(false);
    }
  }, [limit, libraryType]);

  useEffect(() => {
    load();
  }, [load]);

  const loadSeeds = useCallback(async () => {
    try {
      const s = await api.getUpgradeSeeds();
      setSeeds(s);
    } catch (e) {
      setErr(`seeds: ${e.message || e}`);
    }
  }, []);

  useEffect(() => {
    if (seedsOpen && !seeds) loadSeeds();
  }, [seedsOpen, seeds, loadSeeds]);

  const handleRescore = async (row) => {
    if (rescoringTitle) return;
    try {
      setRescoringTitle(`${row.title}|${row.year}`);
      const result = await api.rescoreUpgrade({
        title: row.title,
        year: row.year,
        // We don't have director/genres/overview in the ranked payload; the
        // backend will work from title+year alone. TMDb enrichment happens
        // server-side in a proper batch rescore (CLI path).
      });
      // Merge updated score into the row in place so the user sees immediate feedback.
      setCandidates((prev) =>
        prev.map((c) =>
          c.title === row.title && c.year === row.year
            ? {
                ...c,
                taste_score: result.score,
                taste_rationale: result.rationale,
                combined_score: (c.upgrade_score || 0) * (result.score / 10),
              }
            : c,
        ),
      );
    } catch (e) {
      setErr(`rescore failed: ${e.message || e}`);
    } finally {
      setRescoringTitle(null);
    }
  };

  return (
    <div style={{ color: PALETTE.text }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
        <h2 style={{ margin: 0, fontSize: 22, fontWeight: 600 }}>Upgrade candidates</h2>
        <span style={{ color: PALETTE.textMuted, fontSize: 13 }}>
          ranked by bluray.com gap × Claude taste score
        </span>

        {/* Library-type filter (all / movies / series) */}
        <div
          style={{
            display: "flex",
            background: PALETTE.surfaceLight,
            border: `1px solid ${PALETTE.border}`,
            borderRadius: 8,
            padding: 2,
          }}
        >
          {[
            { k: "all", l: "All" },
            { k: "movie", l: "Movies" },
            { k: "series", l: "Series" },
          ].map(({ k, l }) => (
            <button
              key={k}
              onClick={() => setLibraryTypePersist(k)}
              style={{
                background: libraryType === k ? PALETTE.accent : "transparent",
                color: libraryType === k ? "#fff" : PALETTE.textMuted,
                border: "none",
                borderRadius: 6,
                padding: "5px 12px",
                fontSize: 12,
                fontWeight: 500,
                cursor: "pointer",
                transition: "background 0.1s",
              }}
            >
              {l}
            </button>
          ))}
        </div>

        <div style={{ flex: 1 }} />
        <button onClick={() => setSeedsOpen((v) => !v)} style={ghostBtn}>
          {seedsOpen ? "Hide seeds" : "Edit taste seeds"}
        </button>
        <button onClick={load} style={ghostBtn} disabled={loading}>
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>

      {err && (
        <div
          style={{
            background: "#3a1218",
            border: `1px solid ${PALETTE.red}`,
            color: PALETTE.text,
            padding: "10px 14px",
            borderRadius: 8,
            marginBottom: 16,
            fontSize: 13,
          }}
        >
          {err}
        </div>
      )}

      {seedsOpen && <SeedsEditor seeds={seeds} onChange={setSeeds} onSaved={load} />}

      <div
        style={{
          background: PALETTE.surface,
          border: `1px solid ${PALETTE.border}`,
          borderRadius: 12,
          overflow: "hidden",
        }}
      >
        {loading && (
          <div style={{ padding: 40, textAlign: "center", color: PALETTE.textMuted }}>
            Loading…
          </div>
        )}
        {!loading && candidates.length === 0 && (
          <div style={{ padding: 40, textAlign: "center", color: PALETTE.textMuted, fontSize: 14 }}>
            No upgrade candidates yet. Run:
            <pre style={preStyle}>
{`uv run python -m tools.upgrades refresh
uv run python -m tools.upgrades taste-rescore`}
            </pre>
          </div>
        )}
        {!loading && candidates.length > 0 && (
          <CandidatesTable
            rows={candidates}
            onFileClick={onFileClick}
            onRescore={handleRescore}
            rescoringKey={rescoringTitle}
          />
        )}
      </div>

      <div style={{ marginTop: 12, color: PALETTE.textMuted, fontSize: 12 }}>
        Showing {candidates.length} of max {limit}.{" "}
        <button onClick={() => setLimit((l) => l + 100)} style={linkBtn}>
          Load more
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Candidates table
// ---------------------------------------------------------------------------

function CandidatesTable({ rows, onFileClick, onRescore, rescoringKey }) {
  return (
    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
      <thead>
        <tr style={{ background: PALETTE.surfaceLight, textAlign: "left" }}>
          <Th width="62px" align="right">Combined</Th>
          <Th width="50px" align="right">Gap</Th>
          <Th width="50px" align="right">Taste</Th>
          <Th>Title</Th>
          <Th width="60px">Year</Th>
          <Th width="80px">Current</Th>
          <Th>Available</Th>
          <Th>Rationale</Th>
          <Th width="110px">Actions</Th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <Row
            key={`${r.title}|${r.year}|${i}`}
            row={r}
            onFileClick={onFileClick}
            onRescore={onRescore}
            rescoring={rescoringKey === `${r.title}|${r.year}`}
          />
        ))}
      </tbody>
    </table>
  );
}

function Row({ row, onFileClick, onRescore, rescoring }) {
  const taste = row.taste_score;
  const combinedColour =
    row.combined_score >= 50 ? PALETTE.green : row.combined_score >= 25 ? PALETTE.accentWarm : PALETTE.textMuted;
  const available = [
    row.has_atmos_available && "Atmos",
    row.has_truehd_available && "TrueHD",
    row.has_4k_hdr_available && "4K HDR",
  ].filter(Boolean).join(" · ") || "—";

  return (
    <tr
      style={{
        borderTop: `1px solid ${PALETTE.border}`,
        cursor: row.filepath ? "pointer" : "default",
      }}
      onClick={() => row.filepath && onFileClick && onFileClick(row.filepath)}
    >
      <Td align="right" style={{ color: combinedColour, fontWeight: 600 }}>
        {row.combined_score?.toFixed(1) ?? "—"}
      </Td>
      <Td align="right">{row.upgrade_score ?? "—"}</Td>
      <Td align="right">
        {taste == null ? (
          <span style={{ color: PALETTE.textMuted, fontStyle: "italic" }}>—</span>
        ) : (
          <span style={{ color: taste >= 7 ? PALETTE.green : taste <= 3 ? PALETTE.red : PALETTE.text }}>
            {taste}
          </span>
        )}
      </Td>
      <Td style={{ fontWeight: 500 }}>{row.title}</Td>
      <Td style={{ color: PALETTE.textMuted }}>{row.year ?? "—"}</Td>
      <Td style={{ color: PALETTE.textMuted, whiteSpace: "nowrap" }}>
        {[row.current_video_res, row.current_audio_codec].filter(Boolean).join(" ") || "—"}
      </Td>
      <Td style={{ color: PALETTE.textMuted, whiteSpace: "nowrap" }}>{available}</Td>
      <Td style={{ color: PALETTE.textMuted, fontSize: 12, maxWidth: 380 }}>
        {row.taste_rationale ? (
          <span title={row.taste_rationale}>{truncate(row.taste_rationale, 120)}</span>
        ) : (
          <span style={{ fontStyle: "italic" }}>
            {(row.upgrade_reasons || []).join("; ") || "—"}
          </span>
        )}
      </Td>
      <Td onClick={(e) => e.stopPropagation()}>
        <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
          <button
            onClick={() => onRescore(row)}
            disabled={rescoring}
            style={{ ...ghostBtn, padding: "4px 10px", fontSize: 12 }}
            title="Re-ask Claude to score (5-15s)"
          >
            {rescoring ? "Scoring…" : taste == null ? "Score" : "Rescore"}
          </button>
          <ArrButton
            filepath={row.filepath}
            title={row.title}
            year={row.year}
            libraryType={row.library_type}
          />
          {row.filepath && (
            <DeleteButton filepath={row.filepath} title={row.title} />
          )}
        </div>
      </Td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Delete / Radarr action buttons
// ---------------------------------------------------------------------------

/**
 * Two-click delete with an undo window.
 *
 * First click: arm the button (turns red, says "Confirm?").
 * Second click within 4s: actually delete via /api/file/delete.
 * Timeout without second click: reverts to normal state.
 *
 * The intent is "I can see this file is a low-quality version of a film I
 * care about (per the taste scorer) — delete it so Radarr/Sonarr grabs a
 * better source on the next monitored search". Sonarr/Radarr will auto-
 * re-download because the movie is still monitored.
 */
function DeleteButton({ filepath, title }) {
  const [armed, setArmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [done, setDone] = useState(false);
  const [err, setErr] = useState(null);

  useEffect(() => {
    if (!armed) return;
    const t = setTimeout(() => setArmed(false), 4000);
    return () => clearTimeout(t);
  }, [armed]);

  if (done) {
    return <span style={{ color: PALETTE.green, fontSize: 11 }}>✓ deleted</span>;
  }
  if (err) {
    return (
      <span style={{ color: PALETTE.red, fontSize: 11 }} title={err}>
        ✗ failed
      </span>
    );
  }

  return (
    <button
      onClick={async (e) => {
        e.stopPropagation();
        if (!armed) {
          setArmed(true);
          return;
        }
        try {
          setBusy(true);
          await api.deleteFile(filepath);
          setDone(true);
        } catch (e2) {
          setErr(e2.message || String(e2));
        } finally {
          setBusy(false);
        }
      }}
      disabled={busy}
      title={armed ? `Click again to delete "${title}"` : `Delete "${title}" from NAS`}
      style={{
        ...ghostBtn,
        padding: "4px 10px",
        fontSize: 12,
        color: armed ? "#fff" : PALETTE.red,
        background: armed ? PALETTE.red : "transparent",
        borderColor: PALETTE.red,
      }}
    >
      {busy ? "…" : armed ? "Confirm?" : "Delete"}
    </button>
  );
}

/**
 * Arr integration button — uses Radarr for movies, Sonarr for series.
 *
 * Clicking opens a profile picker; choosing a profile PUTs the target
 * quality profile onto the movie/series and triggers a search.
 *
 * Renders "Radarr (off)" / "Sonarr (off)" if the relevant env vars aren't
 * set, or if the item has an unknown library_type.
 */
function ArrButton({ filepath, title, year, libraryType }) {
  const isSeries = libraryType === "series";
  const app = isSeries ? "Sonarr" : "Radarr";
  const fetchProfiles = isSeries ? api.getSonarrProfiles : api.getRadarrProfiles;
  const triggerUpgrade = isSeries ? api.sonarrUpgrade : api.radarrUpgrade;

  const [profiles, setProfiles] = useState(null); // null = not loaded, [] = disabled, [...] = loaded
  const [picking, setPicking] = useState(false);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState(null);

  const loadProfiles = useCallback(async () => {
    try {
      const r = await fetchProfiles();
      if (r.disabled) {
        setProfiles([]);
      } else {
        setProfiles(r.profiles || []);
      }
    } catch (e) {
      setProfiles([]);
      setMsg(e.detail || e.message || String(e));
    }
  }, [fetchProfiles]);

  const handleClick = async (e) => {
    e.stopPropagation();
    if (profiles === null) await loadProfiles();
    setPicking((v) => !v);
  };

  const choose = async (profileId, profileName) => {
    try {
      setBusy(true);
      setMsg(null);
      await triggerUpgrade({
        filepath,
        title,
        year,
        quality_profile_id: profileId,
        quality_profile_name: profileName,
      });
      setMsg(`queued: ${profileName}`);
      setPicking(false);
    } catch (e) {
      setMsg(`failed: ${e.detail || e.message || e}`);
    } finally {
      setBusy(false);
    }
  };

  if (msg && !picking) {
    return (
      <span style={{ color: msg.startsWith("failed") ? PALETTE.red : PALETTE.green, fontSize: 11 }} title={msg}>
        {msg.startsWith("failed") ? `✗ ${app}` : `✓ ${msg}`}
      </span>
    );
  }

  if (profiles !== null && profiles.length === 0) {
    return (
      <span
        style={{ color: PALETTE.textMuted, fontSize: 11, fontStyle: "italic" }}
        title={msg || `Configure ${app.toUpperCase()}_URL and ${app.toUpperCase()}_API_KEY to enable`}
      >
        {app} (off)
      </span>
    );
  }

  return (
    <div style={{ position: "relative", display: "inline-block" }}>
      <button
        onClick={handleClick}
        disabled={busy}
        title={`Change ${app} quality profile + trigger search`}
        style={{ ...ghostBtn, padding: "4px 10px", fontSize: 12, color: PALETTE.purple, borderColor: PALETTE.purple }}
      >
        {busy ? "…" : `${app} →`}
      </button>
      {picking && profiles && profiles.length > 0 && (
        <div
          style={{
            position: "absolute",
            top: "calc(100% + 4px)",
            right: 0,
            background: PALETTE.surfaceLight,
            border: `1px solid ${PALETTE.border}`,
            borderRadius: 8,
            padding: 6,
            minWidth: 180,
            zIndex: 30,
            boxShadow: "0 6px 20px rgba(0,0,0,0.4)",
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <div style={{ fontSize: 11, color: PALETTE.textMuted, padding: "4px 8px" }}>
            Pick target profile
          </div>
          {profiles.map((p) => (
            <button
              key={p.id}
              onClick={() => choose(p.id, p.name)}
              style={{
                display: "block",
                width: "100%",
                textAlign: "left",
                background: "transparent",
                color: PALETTE.text,
                border: "none",
                padding: "6px 10px",
                borderRadius: 6,
                cursor: "pointer",
                fontSize: 13,
              }}
              onMouseEnter={(e) => (e.currentTarget.style.background = PALETTE.surface)}
              onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
            >
              {p.name}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Seeds editor
// ---------------------------------------------------------------------------

function SeedsEditor({ seeds, onChange, onSaved }) {
  const [addingTier, setAddingTier] = useState(null);
  const [form, setForm] = useState({ title: "", year: "", director: "", score: 8, rationale: "" });
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState(null);

  if (!seeds) {
    return (
      <div style={panelStyle}>
        <div style={{ color: PALETTE.textMuted }}>Loading seeds…</div>
      </div>
    );
  }

  const startAdd = (tier) => {
    setAddingTier(tier);
    setForm({
      title: "",
      year: new Date().getFullYear(),
      director: "",
      score: tier === "high" ? 9 : 2,
      rationale: "",
    });
    setMsg(null);
  };

  const submitAdd = async () => {
    if (!form.title.trim() || !form.rationale.trim()) {
      setMsg("title + rationale required");
      return;
    }
    try {
      setSaving(true);
      await api.addUpgradeSeed(addingTier, {
        title: form.title.trim(),
        year: parseInt(form.year, 10) || 2000,
        director: form.director.trim(),
        score: parseInt(form.score, 10),
        rationale: form.rationale.trim(),
      });
      const refreshed = await api.getUpgradeSeeds();
      onChange(refreshed);
      setAddingTier(null);
      setMsg("Seed added — version bumped. Rescore will apply on next pass.");
      if (onSaved) onSaved();
    } catch (e) {
      setMsg(`failed: ${e.message || e}`);
    } finally {
      setSaving(false);
    }
  };

  const removeSeed = async (tier, seed) => {
    try {
      await api.removeUpgradeSeed(tier, seed.title, seed.year);
      const refreshed = await api.getUpgradeSeeds();
      onChange(refreshed);
    } catch (e) {
      setMsg(`remove failed: ${e.message || e}`);
    }
  };

  return (
    <div style={panelStyle}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
        <h3 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>Taste seeds</h3>
        <span style={{ color: PALETTE.textMuted, fontSize: 12 }}>
          version {seeds.version} · {(seeds.high || []).length} high · {(seeds.low || []).length} low
        </span>
        <div style={{ flex: 1 }} />
        {msg && <span style={{ color: PALETTE.accentWarm, fontSize: 12 }}>{msg}</span>}
      </div>
      <p style={{ color: PALETTE.textMuted, fontSize: 12, marginTop: 0, marginBottom: 12 }}>
        These are the reference points the LLM uses to calibrate scores. Editing bumps the
        seed version; next <code>taste-rescore</code> pass will re-score films whose cached
        scores are on the old version.
      </p>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <SeedColumn
          label="High — worth the upgrade"
          colour={PALETTE.green}
          seeds={seeds.high}
          onAdd={() => startAdd("high")}
          onRemove={(s) => removeSeed("high", s)}
        />
        <SeedColumn
          label="Low — don't bother"
          colour={PALETTE.red}
          seeds={seeds.low}
          onAdd={() => startAdd("low")}
          onRemove={(s) => removeSeed("low", s)}
        />
      </div>

      {addingTier && (
        <div
          style={{
            marginTop: 16,
            padding: 12,
            background: PALETTE.surfaceLight,
            border: `1px solid ${PALETTE.border}`,
            borderRadius: 8,
          }}
        >
          <div style={{ marginBottom: 8, fontSize: 13, fontWeight: 600 }}>
            New {addingTier} seed
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "2fr 80px 2fr 60px", gap: 8, marginBottom: 8 }}>
            <Input placeholder="Title" value={form.title} onChange={(v) => setForm({ ...form, title: v })} />
            <Input placeholder="Year" value={form.year} onChange={(v) => setForm({ ...form, year: v })} />
            <Input placeholder="Director (optional)" value={form.director} onChange={(v) => setForm({ ...form, director: v })} />
            <Input placeholder="0-10" value={form.score} onChange={(v) => setForm({ ...form, score: v })} />
          </div>
          <textarea
            placeholder="Rationale — cite specific craft (DP, sound design, source master)"
            value={form.rationale}
            onChange={(e) => setForm({ ...form, rationale: e.target.value })}
            style={{
              width: "100%",
              minHeight: 60,
              padding: 8,
              background: PALETTE.surface,
              color: PALETTE.text,
              border: `1px solid ${PALETTE.border}`,
              borderRadius: 6,
              fontFamily: "inherit",
              fontSize: 13,
              resize: "vertical",
            }}
          />
          <div style={{ marginTop: 8, display: "flex", gap: 8 }}>
            <button onClick={submitAdd} style={primaryBtn} disabled={saving}>
              {saving ? "Saving…" : "Add seed"}
            </button>
            <button onClick={() => setAddingTier(null)} style={ghostBtn}>
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function SeedColumn({ label, colour, seeds, onAdd, onRemove }) {
  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
        <div style={{ color: colour, fontWeight: 600, fontSize: 13 }}>{label}</div>
        <span style={{ color: PALETTE.textMuted, fontSize: 12 }}>({(seeds || []).length})</span>
        <div style={{ flex: 1 }} />
        <button onClick={onAdd} style={{ ...ghostBtn, padding: "3px 10px", fontSize: 12 }}>
          + Add
        </button>
      </div>
      <div style={{ maxHeight: 280, overflowY: "auto", paddingRight: 4 }}>
        {(seeds || []).map((s, i) => (
          <div
            key={`${s.title}|${s.year}|${i}`}
            style={{
              display: "flex",
              alignItems: "flex-start",
              gap: 8,
              padding: "6px 8px",
              background: PALETTE.bg,
              border: `1px solid ${PALETTE.border}`,
              borderRadius: 6,
              marginBottom: 4,
              fontSize: 12,
            }}
          >
            <div style={{ color: colour, fontWeight: 600, minWidth: 22, textAlign: "right" }}>{s.score}</div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontWeight: 500 }}>
                {s.title} <span style={{ color: PALETTE.textMuted, fontWeight: 400 }}>({s.year})</span>
              </div>
              <div style={{ color: PALETTE.textMuted, fontSize: 11, marginTop: 2 }}>
                {truncate(s.rationale, 120)}
              </div>
            </div>
            <button
              onClick={() => onRemove(s)}
              title="Remove seed"
              style={{
                background: "transparent",
                border: "none",
                color: PALETTE.textMuted,
                cursor: "pointer",
                fontSize: 14,
                padding: "0 4px",
              }}
            >
              ×
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Small presentational helpers
// ---------------------------------------------------------------------------

function Th({ children, width, align }) {
  return (
    <th
      style={{
        padding: "10px 12px",
        color: PALETTE.textMuted,
        fontSize: 11,
        fontWeight: 500,
        textTransform: "uppercase",
        letterSpacing: 0.5,
        width,
        textAlign: align || "left",
      }}
    >
      {children}
    </th>
  );
}

function Td({ children, align, style, onClick }) {
  return (
    <td
      onClick={onClick}
      style={{
        padding: "10px 12px",
        verticalAlign: "top",
        textAlign: align || "left",
        ...(style || {}),
      }}
    >
      {children}
    </td>
  );
}

function Input({ placeholder, value, onChange }) {
  return (
    <input
      placeholder={placeholder}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      style={{
        padding: "6px 8px",
        background: PALETTE.surface,
        color: PALETTE.text,
        border: `1px solid ${PALETTE.border}`,
        borderRadius: 6,
        fontFamily: "inherit",
        fontSize: 13,
        width: "100%",
        boxSizing: "border-box",
      }}
    />
  );
}

function truncate(s, n) {
  if (!s) return "";
  if (s.length <= n) return s;
  return s.slice(0, n - 1).trimEnd() + "…";
}

const ghostBtn = {
  background: "transparent",
  color: PALETTE.text,
  border: `1px solid ${PALETTE.border}`,
  borderRadius: 8,
  padding: "6px 14px",
  fontSize: 13,
  cursor: "pointer",
};

const primaryBtn = {
  background: PALETTE.accent,
  color: "#fff",
  border: "none",
  borderRadius: 8,
  padding: "6px 14px",
  fontSize: 13,
  fontWeight: 600,
  cursor: "pointer",
};

const linkBtn = {
  background: "transparent",
  color: PALETTE.accent,
  border: "none",
  padding: 0,
  cursor: "pointer",
  fontSize: 12,
};

const panelStyle = {
  background: PALETTE.surface,
  border: `1px solid ${PALETTE.border}`,
  borderRadius: 12,
  padding: 16,
  marginBottom: 16,
};

const preStyle = {
  marginTop: 12,
  padding: "10px 14px",
  background: PALETTE.bg,
  border: `1px solid ${PALETTE.border}`,
  borderRadius: 8,
  fontFamily: "JetBrains Mono, monospace",
  fontSize: 12,
  textAlign: "left",
  color: PALETTE.text,
};
