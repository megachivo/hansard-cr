"""Local fallback for `notebooks/02_pull_youtube_audio.py`.

YouTube gates downloads from Databricks serverless egress (datacenter IPs).
Residential ISP IPs aren't gated. This script mirrors the notebook's
discovery + download + trim logic so you can pull mp3s from your laptop,
then `databricks fs cp` them into the UC volume for the transcription
notebook to pick up.

Usage:
  python scripts/pull_audio_local.py \
    --channel https://www.youtube.com/@AsambleaCRC/streams \
    --max 10 \
    --min-duration 3600 \
    --limite-seg 3600 \
    --out ./local_audio

Then upload (replace catalog/schema/volume with your deployed names):
  for f in local_audio/*.mp3; do
    databricks fs cp "$f" \
      "dbfs:/Volumes/josesraspoke/dev_jose_cisneros_bronze/raw_files/audio/$(basename $f)"
  done
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def run_ytdlp(extra_args: list[str], url: str) -> subprocess.CompletedProcess:
    cmd = [
        "yt-dlp",
        "--user-agent", USER_AGENT,
        "--no-warnings",
        *extra_args,
        url,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"yt-dlp failed (exit {result.returncode})\n"
            f"cmd: {' '.join(cmd)}\n"
            f"stderr: {result.stderr.strip()}"
        )
    return result


def list_videos(url: str, max_n: int) -> list[dict]:
    result = run_ytdlp(
        ["--flat-playlist", "--dump-json", "--playlist-end", str(max_n)],
        url,
    )
    out = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        d = json.loads(line)
        out.append({
            "video_id": d.get("id"),
            "titulo": d.get("title", ""),
            "duration_seg": int(d.get("duration") or 0),
        })
    return out


def download_audio(video_id: str, out_dir: Path, limite_seg: int) -> Path:
    url = f"https://www.youtube.com/watch?v={video_id}"
    out_template = str(out_dir / f"{video_id}.%(ext)s")
    audio_path = out_dir / f"{video_id}.mp3"

    run_ytdlp(
        [
            "-x",
            "--audio-format", "mp3",
            "--audio-quality", "5",
            "--no-playlist",
            "-o", out_template,
        ],
        url,
    )

    info = run_ytdlp(
        ["--no-playlist", "--print", "%(duration)s", "--skip-download"],
        url,
    )
    duration = int(float(info.stdout.strip() or 0))
    if duration > limite_seg:
        cut = out_dir / f"{video_id}_cut.mp3"
        subprocess.run(
            ["ffmpeg", "-y", "-i", str(audio_path),
             "-t", str(limite_seg), "-c", "copy", str(cut)],
            check=True, capture_output=True,
        )
        cut.replace(audio_path)
    return audio_path


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--channel",
                   default="https://www.youtube.com/@AsambleaCRC/streams")
    p.add_argument("--max", type=int, default=10,
                   help="how many channel entries to inspect")
    p.add_argument("--min-duration", type=int, default=3600,
                   help="skip videos shorter than this (seconds)")
    p.add_argument("--limite-seg", type=int, default=3600,
                   help="trim downloaded mp3s to this many seconds")
    p.add_argument("--out", default="./local_audio")
    args = p.parse_args()

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    candidatos = list_videos(args.channel, args.max)
    print(f"Candidatos: {len(candidatos)}")
    elegibles = [c for c in candidatos if c["duration_seg"] >= args.min_duration]
    print(f"Tras filtrar duración ≥{args.min_duration}s: {len(elegibles)}")
    for c in elegibles:
        print(f"  {c['video_id']}  {c['duration_seg']:>6}s  {c['titulo'][:80]}")

    manifest_rows = ["video_id,duration_seg,titulo"]
    for c in elegibles:
        skip = out_dir / f"{c['video_id']}.mp3"
        if skip.exists():
            print(f"SKIP {c['video_id']} (already downloaded)")
            manifest_rows.append(
                f"{c['video_id']},{c['duration_seg']},"
                f"\"{c['titulo'].replace('\"', '\"\"')}\""
            )
            continue
        try:
            path = download_audio(c["video_id"], out_dir, args.limite_seg)
            size_mb = path.stat().st_size / 1_048_576
            print(f"OK   {c['video_id']}  {size_mb:.1f} MB  {path.name}")
            manifest_rows.append(
                f"{c['video_id']},{c['duration_seg']},"
                f"\"{c['titulo'].replace('\"', '\"\"')}\""
            )
        except Exception as e:
            print(f"FAIL {c['video_id']}  {type(e).__name__}: {e}")

    (out_dir / "manifest.csv").write_text("\n".join(manifest_rows) + "\n")
    print(f"\nManifest: {out_dir / 'manifest.csv'}")
    print(f"Files in: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
