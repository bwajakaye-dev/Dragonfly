#!/usr/bin/env bash
# Convert any phone/camera clip into a transmitter-friendly MP4:
#   - H.264 (yuv420p) video only: audio, subtitles and data tracks dropped
#   - constant frame rate (phones record variable frame rate, which breaks pacing)
#   - rotation metadata applied to the pixels, so portrait clips stay portrait
#   - keyframe every second and faststart, so looping/seeking is cheap
#
# Usage: scripts/convert_video.sh <input> <output.mp4> [fps]   (default fps: 30)
#
# Set DRAGONFLY_FFMPEG to a specific ffmpeg binary if the one on PATH lacks
# libx264 (e.g. Anaconda's build).
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: $0 <input> <output.mp4> [fps]" >&2
    exit 2
fi

input=$1
output=$2
fps=${3:-30}

if [[ ! -f "$input" ]]; then
    echo "error: input file not found: $input" >&2
    exit 1
fi
ffmpeg=${DRAGONFLY_FFMPEG:-ffmpeg}
command -v "$ffmpeg" >/dev/null || { echo "error: ffmpeg not found: $ffmpeg" >&2; exit 1; }
if ! "$ffmpeg" -hide_banner -encoders 2>/dev/null | grep -q libx264; then
    echo "error: $(command -v "$ffmpeg") has no libx264 encoder;" \
         "install a full build or set DRAGONFLY_FFMPEG (see README)" >&2
    exit 1
fi

"$ffmpeg" -hide_banner -loglevel error -stats -y \
    -i "$input" \
    -map 0:v:0 -an -sn -dn \
    -vf "fps=${fps},format=yuv420p" \
    -c:v libx264 -preset veryfast -crf 20 \
    -g "$fps" -keyint_min "$fps" \
    -movflags +faststart \
    "$output"

echo "wrote $output"
