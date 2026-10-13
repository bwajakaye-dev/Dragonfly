#!/usr/bin/env bash
# Start one transmitter per video file as separate background processes,
# with each line of output prefixed by its camera id. Ctrl+C stops them all
# cleanly (each publishes its "offline" registry record).
#
# Camera id = file name without extension (cam1.mp4 -> cam1).
# Start the cluster, create topics and start the switchboard first.
#
# Usage: scripts/run_demo_local.sh [video ...]     (default: sample_videos/cam*.mp4)
# Env:   MAX_WIDTH (default 1280), DRAGONFLY_BROKERS
#
# On Windows prefer scripts/run_demo_local.ps1: Git Bash cannot deliver a
# clean Ctrl+C to native Windows processes.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if [[ $# -gt 0 ]]; then
    files=("$@")
else
    shopt -s nullglob
    files=(sample_videos/cam*.mp4)
fi
if [[ ${#files[@]} -eq 0 ]]; then
    echo "error: no videos found: put cam1.mp4, cam2.mp4, ... in sample_videos/ or pass paths" >&2
    exit 1
fi

if [[ -x .venv/bin/python ]]; then
    python=.venv/bin/python
elif [[ -x .venv/Scripts/python.exe ]]; then
    python=.venv/Scripts/python.exe
else
    python=python3
fi

pids=()
cleanup() {
    echo "stopping ${#pids[@]} transmitter(s)..."
    kill -INT "${pids[@]}" 2>/dev/null || true
    wait || true
}
trap cleanup INT TERM

for file in "${files[@]}"; do
    camera_id="$(basename "${file%.*}")"
    "$python" -u transmitter.py --camera-id "$camera_id" --file "$file" \
        --max-width "${MAX_WIDTH:-1280}" \
        > >(sed -u "s/^/[$camera_id] /") 2>&1 &
    pids+=($!)
    echo "started $camera_id <- $file (pid $!)"
done

echo "${#pids[@]} transmitter(s) running; Ctrl+C to stop all."
wait
