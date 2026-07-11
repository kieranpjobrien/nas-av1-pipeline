# Animated content — dual-audio (original + English)

**Goal:** keep BOTH the original-language audio AND the English dub on animated
content, so our daughter watches the dub and we can watch in the original with
English subs.

**Rule shipped:**
- `781f661` — original Studio Ghibli director-roster rule (`config.dual_audio_directors`).
- `36e4ee2` — **generalised to ALL animated content** (2026-07-11). Any TMDb
  Animation-genre title now keeps both languages, via `is_animated()` in
  `pipeline/content_grade.py`, shared by the encoder, the gap-filler, AND the
  dashboard compliance check. The director roster stays as a fallback.

The dashboard now flags a foreign-origin animated film that has its original but
no English dub as `audio_animated_missing_english`. Well-targeted: only genuine
anime match — live-action Japanese films (Kurosawa, Godzilla Minus One) are
correctly excluded, so they keep original-only + English subs.

## Already correct (nothing to do)

| Film | Audio | Note |
|---|---|---|
| My Neighbor Totoro (1988) | eng + jpn | ✅ dual audio present (was re-sourced) |
| Princess Mononoke (1997) | jpn + eng | ✅ dual audio present |
| Castle in the Sky (1986) | jpn + eng (+ ita) | ✅ has English |

## Re-source list (8) — deleted 2026-07-11, need a "Dual-Audio (orig+ENG)" release

These were original-language-only (no English dub the daughter watches), so they
were deleted to make room for a dual-audio re-grab. Get a release carrying BOTH
audio tracks + English subs (GKIDS / Disney / Crunchyroll masters have both):

```
\\KieranNAS\Media\Movies\Your Name. (2016)\
\\KieranNAS\Media\Movies\Kiki's Delivery Service (1989)\
\\KieranNAS\Media\Movies\Spirited Away (2001)\
\\KieranNAS\Media\Movies\Ponyo (2008)\
\\KieranNAS\Media\Movies\Demon Slayer - Kimetsu no Yaiba Infinity Castle (2025)\
\\KieranNAS\Media\Movies\Grave of the Fireflies (1988)\
\\KieranNAS\Media\Movies\Little Amélie or the Character of Rain (2025)\
\\KieranNAS\Media\Movies\Howl's Moving Castle (2004)\
```

(Machine-readable copy: `F:\AV1_Staging\animated_resource_needed.txt`.)

Re-download manually via Radarr (interactive search → pick a dual-audio release).
Radarr "allow upgrades" stays OFF — deliberate per-title re-grab, not auto-upgrade.
Once landed, the scanner re-adds them and the pipeline keeps both tracks.

## Plex (the daughter-vs-parents split)

Set audio language **per Plex profile** (Plex remembers per user):
- Daughter's profile → preferred audio **English**, subtitles off.
- You + wife → preferred audio **original (Japanese)**, subtitles **English**.

The file just needs both tracks present; Plex handles who hears what.
