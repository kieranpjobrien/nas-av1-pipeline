"""VMAF quality spot-check — compare original vs encoded on a sample segment."""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from paths import STAGING_DIR


VMAF_RESULTS_DIR = STAGING_DIR / "vmaf_results"


def get_duration(filepath: str) -> float:
    """Get video duration in seconds via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(filepath)],
            capture_output=True, text=True, timeout=15,
        )
        data = json.loads(result.stdout)
        return float(data.get("format", {}).get("duration", 0))
    except Exception:
        return 0


def run_vmaf(source: str, encoded: str, duration: int = 30, offset: str = "auto") -> dict:
    """Run VMAF comparison on a segment of two video files.

    Args:
        source: Path to the original file.
        encoded: Path to the encoded file.
        duration: Duration of the sample segment in seconds.
        offset: Start offset in seconds, or "auto" for 25% into the file.

    Returns:
        Dict with vmaf_mean, vmaf_min, vmaf_max, duration_tested, and paths.
    """
    if not os.path.exists(source):
        return {"error": f"Source not found: {source}"}
    if not os.path.exists(encoded):
        return {"error": f"Encoded not found: {encoded}"}

    # Auto offset: 25% into the file to skip intros
    if offset == "auto":
        total_dur = get_duration(source)
        if total_dur > 0:
            offset_secs = max(0, int(total_dur * 0.25))
            # Ensure we don't overshoot
            if offset_secs + duration > total_dur:
                offset_secs = max(0, int(total_dur - duration - 5))
        else:
            offset_secs = 60
    else:
        offset_secs = int(offset)

    # VMAF log output
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        vmaf_log = tmp.name

    try:
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(offset_secs), "-t", str(duration), "-i", source,
            "-ss", str(offset_secs), "-t", str(duration), "-i", encoded,
            "-filter_complex",
            f"[0:v:0]scale=1920:-1:flags=bicubic[ref];"
            f"[1:v:0]scale=1920:-1:flags=bicubic[dist];"
            f"[dist][ref]libvmaf=log_fmt=json:log_path={vmaf_log}:n_threads=4",
            "-f", "null", "-",
        ]

        print(f"Running VMAF check ({duration}s segment at {offset_secs}s offset)...")
        print(f"  Source:  {os.path.basename(source)}")
        print(f"  Encoded: {os.path.basename(encoded)}")

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            stderr = result.stderr[-500:] if result.stderr else ""
            return {"error": f"FFmpeg VMAF failed (exit {result.returncode}): {stderr}"}

        # Parse VMAF JSON output
        with open(vmaf_log, "r", encoding="utf-8") as f:
            vmaf_data = json.load(f)

        frames = vmaf_data.get("frames", [])
        if not frames:
            return {"error": "No VMAF frames computed"}

        scores = [f["metrics"]["vmaf"] for f in frames]
        vmaf_mean = sum(scores) / len(scores)
        vmaf_min = min(scores)
        vmaf_max = max(scores)

        result_data = {
            "vmaf_mean": round(vmaf_mean, 2),
            "vmaf_min": round(vmaf_min, 2),
            "vmaf_max": round(vmaf_max, 2),
            "frames_tested": len(scores),
            "duration_tested": duration,
            "offset_secs": offset_secs,
            "source": source,
            "encoded": encoded,
        }

        print(f"\n  VMAF Mean: {vmaf_mean:.2f}")
        print(f"  VMAF Min:  {vmaf_min:.2f}")
        print(f"  VMAF Max:  {vmaf_max:.2f}")
        print(f"  Frames:    {len(scores)}")

        # Cache result
        VMAF_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = Path(encoded).stem.replace(" ", "_")[:80]
        result_path = VMAF_RESULTS_DIR / f"{safe_name}.json"
        with open(result_path, "w", encoding="utf-8") as f:
            json.dump(result_data, f, indent=2)
        print(f"  Result saved to {result_path}")

        return result_data

    finally:
        try:
            os.unlink(vmaf_log)
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser(description="VMAF quality spot-check")
    parser.add_argument("source", help="Original source file")
    parser.add_argument("encoded", help="Encoded output file")
    parser.add_argument("--duration", type=int, default=30, help="Sample duration in seconds")
    parser.add_argument("--offset", default="auto", help="Start offset (seconds or 'auto')")
    args = parser.parse_args()

    result = run_vmaf(args.source, args.encoded, args.duration, args.offset)
    if "error" in result:
        print(f"\nError: {result['error']}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
