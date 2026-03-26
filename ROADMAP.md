# NASCleanup — Augmentation Roadmap

Focused on: faster/more efficient encoding on this machine, a more informative dashboard, and integrated media management tools (subtitles, tags, audio, Plex metadata).

---

## Wave 1: High Impact, Moderate Effort (DONE)

### 1. Parallel Encode + Upload
Upload file N in a background thread while GPU encodes file N+1. Near-doubles throughput when upload is a bottleneck.

### 2. Concurrent Audio-Only Remux
Audio remux jobs (AV1 with bulky audio) dispatch to CPU background threads — GPU encodes aren't blocked.

### 3. Incremental Scanner (mtime)
Added mtime to the cache key so renamed/re-tagged files are correctly re-probed on rescan.

### 4. Encode History Log + Analytics Page
Append-only JSONL log per completed encode. New "History" tab with charts: encodes/day, cumulative savings, compression ratios by tier, speed trends, storage forecast.

### 5. Live GPU Monitor Widget
Nav bar widget showing RTX 4080 temp, encoder utilisation, GPU utilisation, VRAM, power draw. Polls nvidia-smi every 5s via cached API endpoint.

### 6. WebSocket for Live Updates
Replaced 3-second polling with WebSocket push for pipeline state, GPU stats, and control status. Falls back to polling if WS disconnects.

---

## Wave 2: Dashboard & Efficiency

### 7. Pipeline Timeline / Gantt View
Horizontal timeline for recent files showing fetch/encode/upload/verify/replace durations. Reveals whether you're network-bound or GPU-bound at a glance.
- Extend state to track per-stage start/end timestamps
- New frontend component using the encode history data

### 8. Per-File ETA in Up Next
Show estimated encode time per file based on tier averages. "40 GB 4K HDR → ~2h 15m" in the queue list.
- Already have per-tier averages — surface per-item in PipelinePage

### 9. File Detail Drawer
Click any file → slide-out panel with full metadata: all streams, bitrate, duration, HDR info, encode settings used, compression ratio achieved, stage timestamps.

### 10. Health Dashboard
System health cards: NAS reachability, staging disk free space + trend, GPU driver version, FFmpeg version, pipeline uptime.

### 11. Notifications
Browser push notifications + optional webhook (ntfy.sh/Discord) for: encode errors, pipeline stall, pipeline completion, milestones.

### 12. Settings UI
Edit pipeline config from the dashboard: CQ values per tier, presets, staging limits, fetch buffer size, audio mode. Reads/writes a `config_overrides.json` merged on top of defaults.

### 13. Smarter Prefetch Sizing
Predict encode time from file size/resolution and adjust prefetch buffer — fewer large files (encode slowly), more small files (encode fast).

---

## Wave 3: Media Management Tools

### 14. Subtitle Management
Integrate `tools/subtitles.py` into the dashboard: strip duplicate subtitle tracks, flag files with no English subs, surface subtitle health in Library page. Optional: auto-download from OpenSubtitles API.

### 15. Audio Loudness Normalisation (EBU R128)
Optional per-profile: normalise audio loudness during transcode using FFmpeg `loudnorm` filter. Fixes the whisper-dialogue/explosion-bass problem. Apply to movies selectively.

### 16. HDR → SDR Tone-Mapping Profile
New quality profile "tonemap" for content shot in SDR but wrapped in HDR containers (common with sitcoms). BT.2020→BT.709 with FFmpeg zscale. Saves bitrate and avoids Plex runtime tone-mapping.

### 17. Duplicate Detection in UI
Surface `tools/duplicates.py` results in the dashboard: show duplicate groups with size comparison, suggest keeping the higher quality version, one-click skip/delete.

### 18. Integrity Scanning
Periodic integrity checks via `tools/integrity.py`: detect corrupted files, truncated encodes, audio sync drift. Flag in UI with severity. Option to re-queue for re-encode.

### 19. Plex Integration
Trigger Plex library scan after replacing files (already have PLEX_URL/TOKEN). Show Plex ratings/popularity alongside files in Library view. Auto-create "Recently Encoded" collection. Surface watch history to inform encoding priority.

### 20. Filename & Metadata Cleanup
Extend `strip_tags` integration: auto-detect and fix common issues (dot-separated names, leftover release group tags, missing metadata). Ensure Plex can properly match and tag all content.

### 21. Chapter Preservation & Cleanup
Verify chapters survive encode. Strip garbage auto-chapters ("Chapter 1" every 5 minutes). Import meaningful chapter data from ChapterDB for known films.

### 22. Container Cleanup
Strip unnecessary streams during encode: cover art, data streams, unused fonts (when no ASS subs), encoder metadata bloat.

---

## Wave 4: Quality & Polish

### 23. Encode Quality Spot-Check (VMAF)
For completed encodes, run VMAF on a 30-second sample to validate CQ choices. Quality report card per profile/tier. Expensive — sample 5% of encodes or on-demand.

### 24. Profile A/B Testing
Encode same file with two different profiles side by side. Compare output size, encode time, and optionally VMAF. Evidence-based quality tuning.

### 25. Tagging / Collections
Custom labels on files (e.g., "kids", "documentary", "reference quality"). Bulk-apply profiles by tag. More flexible than path-based pattern matching.

### 26. Bitrate Efficiency Score
Compare each file's bitrate to expected for its resolution/codec. Flag outliers — "This 1080p is 45 Mbps, likely a bloated scene encode." Helps surface high-value encode targets.

### 27. State Compaction
`pipeline_state.json` grows forever. Compact REPLACED entries into summary stats, archive detail to `pipeline_state_archive.jsonl`. Faster dashboard load times.

### 28. Drag-and-Drop Priority Reordering
Manual reorder of priority queue in UI. Drag files up/down in the Up Next list.
