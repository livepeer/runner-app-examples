#!/bin/bash
# Activate the ComfyStream image's conda env, then run the integration runner.
set -euo pipefail

# shellcheck disable=SC1091
source /workspace/miniconda3/etc/profile.d/conda.sh
conda activate comfystream

export COMFYUI_CWD="${COMFYUI_CWD:-/workspace/ComfyUI}"
exec python /app/runner.py "$@"
