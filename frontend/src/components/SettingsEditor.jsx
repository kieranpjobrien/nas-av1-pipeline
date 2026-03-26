import { useState, useEffect } from "react";
import { PALETTE } from "../theme";
import { api } from "../lib/api";
import { SectionTitle } from "./SectionTitle";

const RES_KEYS = ["4K_HDR", "4K_SDR", "1080p", "720p", "480p", "SD"];
const CONTENT_TYPES = ["movie", "series"];
const PRESETS = ["p1", "p2", "p3", "p4", "p5", "p6", "p7"];
const MULTIPASS = ["disabled", "qres", "fullres"];

function InputRow({ label, value, onChange, type = "number", width = 70, suffix, options }) {
  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "4px 0" }}>
      <span style={{ color: PALETTE.textMuted, fontSize: 12 }}>{label}</span>
      <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
        {options ? (
          <select value={value || ""} onChange={(e) => onChange(e.target.value)} style={{
            background: PALETTE.bg, color: PALETTE.text, border: `1px solid ${PALETTE.border}`,
            borderRadius: 4, padding: "3px 6px", fontSize: 12, fontFamily: "'JetBrains Mono', monospace",
          }}>
            {options.map((o) => <option key={o} value={o}>{o}</option>)}
          </select>
        ) : (
          <input
            type={type} value={value ?? ""} onChange={(e) => onChange(type === "number" ? Number(e.target.value) : e.target.value)}
            style={{
              width, background: PALETTE.bg, color: PALETTE.text, border: `1px solid ${PALETTE.border}`,
              borderRadius: 4, padding: "3px 6px", fontSize: 12, fontFamily: "'JetBrains Mono', monospace",
              textAlign: "right",
            }}
          />
        )}
        {suffix && <span style={{ color: PALETTE.textMuted, fontSize: 11 }}>{suffix}</span>}
      </div>
    </div>
  );
}

function TierGrid({ label, data, onChange, inputType, options }) {
  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 8 }}>{label}</div>
      <div style={{ display: "grid", gridTemplateColumns: "80px repeat(6, 1fr)", gap: 4, fontSize: 11 }}>
        {/* Header */}
        <div style={{ color: PALETTE.textMuted }} />
        {RES_KEYS.map((r) => <div key={r} style={{ color: PALETTE.textMuted, textAlign: "center", fontFamily: "'JetBrains Mono', monospace" }}>{r}</div>)}
        {/* Rows */}
        {CONTENT_TYPES.map((ct) => (
          <div key={ct} style={{ display: "contents" }}>
            <div style={{ color: PALETTE.textMuted, textTransform: "capitalize", paddingTop: 4 }}>{ct}</div>
            {RES_KEYS.map((rk) => (
              <div key={rk} style={{ textAlign: "center" }}>
                {options ? (
                  <select
                    value={data?.[ct]?.[rk] ?? ""}
                    onChange={(e) => onChange(ct, rk, e.target.value)}
                    style={{
                      width: "100%", background: PALETTE.bg, color: PALETTE.text,
                      border: `1px solid ${PALETTE.border}`, borderRadius: 4,
                      padding: "2px 2px", fontSize: 11, fontFamily: "'JetBrains Mono', monospace",
                    }}
                  >
                    {options.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                ) : (
                  <input
                    type={inputType || "number"}
                    value={data?.[ct]?.[rk] ?? ""}
                    onChange={(e) => onChange(ct, rk, inputType === "number" || !inputType ? Number(e.target.value) : e.target.value)}
                    style={{
                      width: "100%", background: PALETTE.bg, color: PALETTE.text,
                      border: `1px solid ${PALETTE.border}`, borderRadius: 4,
                      padding: "2px 4px", fontSize: 12, fontFamily: "'JetBrains Mono', monospace",
                      textAlign: "center",
                    }}
                  />
                )}
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

export function SettingsEditor() {
  const [config, setConfig] = useState(null);
  const [overrides, setOverrides] = useState({});
  const [saving, setSaving] = useState(false);
  const [flash, setFlash] = useState(null);

  useEffect(() => {
    api.getConfig().then((data) => {
      setConfig(data.effective);
      setOverrides(data.overrides || {});
    }).catch(() => {});
  }, []);

  if (!config) return null;

  // Deep-clone working config (defaults + overrides applied)
  const working = JSON.parse(JSON.stringify(config));

  const updateNested = (key, ct, rk, value) => {
    const ov = { ...overrides };
    if (!ov[key]) ov[key] = {};
    if (!ov[key][ct]) ov[key][ct] = {};
    ov[key][ct][rk] = value;
    setOverrides(ov);

    // Also update working config for display
    if (!working[key]) working[key] = {};
    if (!working[key][ct]) working[key][ct] = {};
    working[key][ct][rk] = value;
  };

  const updateScalar = (key, value) => {
    setOverrides({ ...overrides, [key]: value });
  };

  const save = async () => {
    setSaving(true);
    try {
      await api.setConfig(overrides);
      setFlash("Saved — pipeline will use new settings on next file");
      setTimeout(() => setFlash(null), 4000);
    } catch {
      setFlash("Save failed");
      setTimeout(() => setFlash(null), 3000);
    }
    setSaving(false);
  };

  const reset = async () => {
    setSaving(true);
    try {
      await api.setConfig({});
      setOverrides({});
      // Reload defaults
      const data = await api.getConfig();
      setConfig(data.effective);
      setFlash("Reset to defaults");
      setTimeout(() => setFlash(null), 3000);
    } catch {
      setFlash("Reset failed");
    }
    setSaving(false);
  };

  // Merge overrides into working for display
  const merged = JSON.parse(JSON.stringify(config));
  for (const [key, val] of Object.entries(overrides)) {
    if (typeof val === "object" && val !== null && typeof merged[key] === "object") {
      for (const [sub, subval] of Object.entries(val)) {
        if (typeof subval === "object" && subval !== null && typeof merged[key]?.[sub] === "object") {
          merged[key][sub] = { ...merged[key][sub], ...subval };
        } else {
          merged[key][sub] = subval;
        }
      }
    } else {
      merged[key] = val;
    }
  }

  const hasOverrides = Object.keys(overrides).length > 0;

  return (
    <>
      <SectionTitle>Pipeline Settings</SectionTitle>

      {flash && (
        <div style={{
          background: PALETTE.green + "22", border: `1px solid ${PALETTE.green}44`,
          borderRadius: 8, padding: "10px 16px", marginBottom: 16,
          color: PALETTE.green, fontSize: 13, fontWeight: 500,
        }}>{flash}</div>
      )}

      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
        {/* CQ Values */}
        <TierGrid label="CQ (Constant Quality)" data={merged.cq} onChange={(ct, rk, v) => updateNested("cq", ct, rk, v)} />

        {/* Presets */}
        <TierGrid label="NVENC Preset" data={merged.nvenc_preset} onChange={(ct, rk, v) => updateNested("nvenc_preset", ct, rk, v)} options={PRESETS} />

        {/* Multipass */}
        <TierGrid label="Multipass" data={merged.nvenc_multipass} onChange={(ct, rk, v) => updateNested("nvenc_multipass", ct, rk, v)} options={MULTIPASS} />

        {/* Lookahead */}
        <TierGrid label="Lookahead Frames" data={merged.nvenc_lookahead} onChange={(ct, rk, v) => updateNested("nvenc_lookahead", ct, rk, v)} />
      </div>

      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
        <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 12 }}>Staging Limits</div>
        <InputRow label="Max staging" value={Math.round((merged.max_staging_bytes || 0) / (1024 ** 3))} onChange={(v) => updateScalar("max_staging_bytes", v * 1024 ** 3)} suffix="GB" />
        <InputRow label="Max fetch buffer" value={Math.round((merged.max_fetch_buffer_bytes || 0) / (1024 ** 3))} onChange={(v) => updateScalar("max_fetch_buffer_bytes", v * 1024 ** 3)} suffix="GB" />
        <InputRow label="Min free space" value={Math.round((merged.min_free_space_bytes || 0) / (1024 ** 3))} onChange={(v) => updateScalar("min_free_space_bytes", v * 1024 ** 3)} suffix="GB" />
      </div>

      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
        <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 12 }}>Audio</div>
        <InputRow label="Audio mode" value={merged.audio_mode} onChange={(v) => updateScalar("audio_mode", v)} options={["smart", "copy"]} />
        <InputRow label="Surround bitrate" value={merged.audio_eac3_surround_bitrate} onChange={(v) => updateScalar("audio_eac3_surround_bitrate", v)} type="text" width={80} />
        <InputRow label="Stereo bitrate" value={merged.audio_eac3_stereo_bitrate} onChange={(v) => updateScalar("audio_eac3_stereo_bitrate", v)} type="text" width={80} />
        <InputRow label="Loudness normalisation (EBU R128)" value={merged.audio_loudnorm ? "yes" : "no"} onChange={(v) => updateScalar("audio_loudnorm", v === "yes")} options={["no", "yes"]} />
      </div>

      <div style={{ background: PALETTE.surface, border: `1px solid ${PALETTE.border}`, borderRadius: 12, padding: 20, marginBottom: 16 }}>
        <div style={{ color: PALETTE.text, fontSize: 13, fontWeight: 600, marginBottom: 12 }}>Behaviour</div>
        <InputRow label="Replace originals" value={merged.replace_original ? "yes" : "no"} onChange={(v) => updateScalar("replace_original", v === "yes")} options={["yes", "no"]} />
        <InputRow label="Strip non-English subs" value={merged.strip_non_english_subs !== false ? "yes" : "no"} onChange={(v) => updateScalar("strip_non_english_subs", v === "yes")} options={["yes", "no"]} />
        <InputRow label="Duration tolerance" value={merged.verify_duration_tolerance_secs} onChange={(v) => updateScalar("verify_duration_tolerance_secs", v)} suffix="secs" />
      </div>

      {/* Actions */}
      <div style={{ display: "flex", gap: 12, marginBottom: 24 }}>
        <button onClick={save} disabled={saving} style={{
          background: PALETTE.accent, color: "#fff", border: "none", borderRadius: 8,
          padding: "10px 24px", fontSize: 13, fontWeight: 600, cursor: "pointer",
          opacity: saving ? 0.6 : 1,
        }}>
          {saving ? "Saving..." : "Save Settings"}
        </button>
        {hasOverrides && (
          <button onClick={reset} disabled={saving} style={{
            background: "transparent", color: PALETTE.textMuted, border: `1px solid ${PALETTE.border}`,
            borderRadius: 8, padding: "10px 24px", fontSize: 13, fontWeight: 500, cursor: "pointer",
          }}>
            Reset to Defaults
          </button>
        )}
        {hasOverrides && (
          <span style={{ color: PALETTE.accentWarm, fontSize: 11, alignSelf: "center" }}>
            {Object.keys(overrides).length} override(s) active
          </span>
        )}
      </div>
    </>
  );
}
