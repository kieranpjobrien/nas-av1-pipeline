# NASCleanup — What Is This?

This is a tool that shrinks your movie and TV show files so they take up less space on your NAS (Network Attached Storage), without any visible loss in quality.

It does this by re-encoding video files from older formats (H.264, HEVC) into **AV1** — a newer, more efficient video codec. A typical library can shrink by **30-50%**, meaning a 20 TB collection might drop to 10-12 TB.

## Who is this for?

You, if:

- You have a NAS full of movies and TV shows (Plex, Jellyfin, etc.)
- You're running low on storage and don't want to buy more drives yet
- You have a PC with an **NVIDIA RTX 4000-series GPU** (RTX 4060 or above) — this is required for the hardware encoding

## What you need before starting

### Hardware

- **NVIDIA RTX 4000+ GPU** — the encoding is done by dedicated hardware on the GPU, so it's fast and doesn't max out your CPU. Older GPUs won't work (no AV1 hardware encoder).
- **Fast local SSD** — the tool copies files to a local staging drive, encodes them, then copies them back. An NVMe SSD with at least 500 GB free works well. The default staging location is `E:\AV1_Staging`.
- **NAS accessible as a network drive** — your movies and series folders need to be mapped as drive letters (e.g. `Z:\Movies`, `Z:\Series`).

### Software to install

1. **Python 3.11 or newer**
   - Download from https://www.python.org/downloads/
   - During install, **check the box that says "Add Python to PATH"** — this is important
   - To verify it worked, open a terminal and type `python --version` — you should see something like `Python 3.12.x`

2. **uv** (Python package manager)
   - Open a terminal (PowerShell or Command Prompt) and run:
     ```
     pip install uv
     ```
   - This is what manages the project's dependencies

3. **FFmpeg** (the video encoding engine)
   - Download from https://www.gyan.dev/ffmpeg/builds/ — grab the "release full" build
   - Extract it somewhere (e.g. `C:\ffmpeg`)
   - Add the `bin` folder to your system PATH:
     - Search Windows for "Environment Variables"
     - Under "System variables", find `Path`, click Edit
     - Add a new entry: `C:\ffmpeg\bin` (or wherever you extracted it)
   - To verify: open a new terminal and type `ffmpeg -version`

4. **Node.js** (only needed if you want to modify the dashboard UI)
   - Download from https://nodejs.org/ — grab the LTS version
   - This is optional — the dashboard comes pre-built

### Nvidia drivers

Make sure your GPU drivers are up to date. The NVENC encoder that this tool uses gets improvements with driver updates.

## First-time setup

1. **Open a terminal** in the project folder (`C:\Projects\NASCleanup` or wherever you put it)

2. **Map your NAS drives** if they aren't already:
   - In Windows Explorer, right-click "This PC" > "Map Network Drive"
   - Map your movies folder to `Z:\Movies` and series to `Z:\Series`
   - Or set environment variables if your paths are different:
     ```
     set NAS_MOVIES=\\yournas\movies
     set NAS_SERIES=\\yournas\series
     ```

3. **Create the staging folder**:
   ```
   mkdir E:\AV1_Staging
   ```

4. **Install project dependencies**:
   ```
   uv sync
   ```

## How to use it

### Step 1: Scan your library

This reads every video file on your NAS and builds a report of what you have (codec, resolution, bitrate, etc.). It doesn't modify anything.

```
uv run python -m tools.scanner
```

This takes a while on a big library (ffprobe checks every file). When done, it saves `media_report.json` to your staging folder.

### Step 2: Start the dashboard (optional but recommended)

```
uv run python -m server
```

Then open http://localhost:8000 in your browser. You get:

- **Library tab** — charts showing your codec mix, resolution breakdown, estimated savings, biggest files
- **Pipeline tab** — live progress, ETA, per-tier savings stats, error tracking
- **Control tab** — pause/resume, skip specific files, adjust quality settings on the fly

The dashboard can also start/stop the scanner and pipeline for you (no separate terminal needed).

### Step 3: Run the encoding pipeline

```
uv run python -m pipeline
```

Or from the dashboard, hit "Start Pipeline" on the Pipeline tab.

This will:

1. **Fetch** — copy a file from NAS to your local SSD
2. **Encode** — re-encode it to AV1 using your GPU
3. **Upload** — copy the new file back to the NAS
4. **Verify** — check the new file plays correctly and has the right duration
5. **Replace** — swap the original file with the smaller AV1 version

It processes files in priority order (biggest savings first) and handles everything automatically. You can leave it running overnight or for days — it saves progress and can resume if interrupted.

To resume after stopping:

```
uv run python -m pipeline --resume
```

### Dry run (preview without encoding)

Want to see what would be processed without actually doing anything?

```
uv run python -m pipeline --dry-run
```

## Extra tools

### Find duplicates

Checks your scan report for files that look like duplicates (same movie in different quality, etc.):

```
uv run python -m tools.duplicates
```

Outputs a CSV you can review before deleting anything manually.

### Check file integrity

Runs a full decode check on files to find corruption:

```
uv run python -m tools.integrity --directory Z:\Movies
```

Or check just the files the pipeline has already re-encoded:

```
uv run python -m tools.integrity --from-state
```

### Find missing subtitles

```
uv run python -m tools.scanner --missing-subs-csv missing_subs.csv
```

### Find files without English audio

```
uv run python -m tools.scanner --non-english-csv non_english.csv
```

## How long does it take?

It depends on your library size and the content. Rough ballpark with an RTX 4080:

- A 1080p movie (~2 hours, ~8 GB) encodes in about 10-15 minutes
- A 4K HDR movie takes longer due to higher quality settings
- Series episodes (45 min, ~2 GB each) take 3-5 minutes each

A 3000-file library might take 1-2 weeks of continuous running. The pipeline handles everything — you just need to leave the PC on.

## Is it safe? Will I lose files?

The pipeline is designed to be crash-safe:

- It keeps the original file until the new one is fully verified
- If power cuts out mid-encode, it resumes from where it left off
- The replace step uses a backup-rename-delete sequence so you never have zero copies
- All progress is saved to a JSON state file after every step

That said, **this is your media library** — consider having a backup of anything irreplaceable before running bulk operations on it.

## Troubleshooting

**"ffmpeg not found"** — FFmpeg isn't on your PATH. Re-check the install step above.

**"Path not found: Z:\Movies"** — Your NAS drive isn't mapped. Check that the network drive is connected.

**Encoding fails on specific files** — Some files have unusual containers or streams. The pipeline will log the error and move on. You can retry failed files from the dashboard.

**Dashboard shows stale data** — Hard refresh your browser with Ctrl+Shift+R.

**Pipeline seems stuck** — Check the dashboard Pipeline tab for the current activity. Large 4K files can take a while. You can also check `E:\AV1_Staging\pipeline.log` for detailed logs.
