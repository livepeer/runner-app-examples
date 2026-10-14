# ComfyStream app (workflow-driven video + analyze)

A ComfyUI live-video app on the Livepeer network. The image starts from the
published [`livepeer/comfystream`](https://hub.docker.com/r/livepeer/comfystream)
base (ComfyUI workspace and conda env). The Dockerfile then replaces
`/workspace/comfystream` with a pinned checkout of
[livepeer/comfystream](https://github.com/livepeer/comfystream) and installs that
package. This folder is the live-runner app layer: `runner.py` (registration,
trickle, WebSocket) and `client.py`.

|              |                                      |
| ------------ | ------------------------------------ |
| App id       | `livepeer-example/comfystream`       |
| Runner mode  | persistent (held-open session)       |
| Registration | dynamic (self-registers via the SDK) |
| Transport    | trickle + HTTP + WebSocket           |
| Pricing      | hour (metered per second)            |
| Port         | 8991                                 |

Prerequisites (Docker, NVIDIA GPU, `uv`, the
[`livepeer-gateway` SDK](https://pypi.org/project/livepeer-gateway/)) and the
shared setup are in the [repo README](../README.md). The first build pulls
`livepeer/comfystream:latest` and clones the pinned package, so it takes a while.
This image is **built locally only**. It is not published to GHCR: the
CUDA/ComfyUI base is too large for GitHub-hosted runners.

## How it's wired

`COMFYSTREAM_REF` in the [Dockerfile](Dockerfile) pins the comfystream checkout
(currently `687f90b2f6023d8a31b548809abac02989878a2e` on
`feat/comfypeer-liverunner`). Bump that build-arg to move the package forward,
then rebuild. `runner.py` imports `comfystream.Pipeline` from the installed
package and registers with the orchestrator. Batch video tools run in a
separate process pool so they do not share the live stream pipeline.

The app is **dynamically registered**: it self-registers via `register_runner`
([runner.py](runner.py)) and exposes:

- `POST /analyze` — video-in → text-out (trickle `in` + JSONL `text`)
- `POST /start_stream` — live trickle video (optional audio and text)
- `POST /update_stream` — mid-session workflow / resolution change
- `GET /ws_stream` — browser JPEG frames over the session WebSocket
- `GET /text` — buffered text for the active session
- `GET /healthz`
- `POST /tools/{name}` — one-shot video tool. `{name}` is `convert`,
  `compress`, `trim`, `resize`, `gif`, `extract-audio`, `mute`, or `rotate`.
  `GET /tools/{name}/schema` returns the JSON fields.

The stream client calls it with `reserve_session` → `post_json` / `MediaPublish` →
`stop_runner_session` ([client.py](client.py)). Grep `# Livepeer:` in either
file to see the exact calls.

This is the same persistent + trickle shape as [`echo`](../echo), plus an HTTP
analyze surface and a WebSocket JPEG stream, both driven by a ComfyUI
API-format workflow JSON.

## Run offchain (free)

Start the stack and confirm the runner registered.

```sh
docker compose up -d --build
curl -sk https://localhost:8935/discovery | jq '.[].runners[].app'   # confirm livepeer-example/comfystream registered
```

Any video works; this makes a short one if you need it:

```sh
ffmpeg -f lavfi -i testsrc=size=512x512:rate=30 -t 5 -c:v libx264 -preset ultrafast -pix_fmt yuv420p sample.mp4
uv run client.py sample.mp4 --discovery https://localhost:8935/discovery
```

`client.py` defaults to [workflows/analyze-stub-api.json](workflows/analyze-stub-api.json)
(video-in → a fixed text token). For a visible video transform, use the
video-only invert workflow:

```sh
uv run client.py sample.mp4 --mode stream \
  --workflow workflows/invert-tensor-api.json \
  --discovery https://localhost:8935/discovery
```

[workflows/invert-color-av-passthrough-api.json](workflows/invert-color-av-passthrough-api.json)
is the same invert plus an audio passthrough. Pass `--audio` and a file that
contains an audio stream. The graph waits about a second for the first audio
frame, so a live source (the way [`echo`](../echo) is driven) is a better fit
than a short file dumped as fast as possible.

## Batch video tools

Each tool is its own single-shot app, `livepeer-example/comfystream-<tool>`,
billed once per call (`unit=fixed`). Workers are CPU processes
(`CUDA_VISIBLE_DEVICES` is cleared) so the live stream keeps the GPU. One pool,
default 2 workers, is shared by every tool. Input files are capped at 64 MB.

```sh
uv run client.py sample.mp4 --tool convert --format webm \
  --discovery https://localhost:8935/discovery --output sample.webm
uv run client.py sample.mp4 --tool resize --tool-height 720 \
  --discovery https://localhost:8935/discovery --output sample-720.mp4
```

The body is `{"video_base64": "...", ...fields}`. Resize and rotate workflows
call Comfy `ImageScale` and `RotateFlipImage` once per frame. Convert, compress,
trim, gif, extract-audio, and mute run as a single container edit.

Stop the stack with `docker compose down`.

## Attach to an existing orchestrator

Same image and runner, no local go-livepeer. Point at the orch you already run
(env vars in `.env` / `.env.example`):

```sh
docker compose -f compose.existing.yml up -d --build
uv run client.py sample.mp4 --discovery "$LIVEPEER_ORCH_URL/discovery"
```

## Run on-chain (paid)

Layer `compose.onchain.yml` to run the orchestrator on-chain with a remote
signer paying for the session. This example uses **metered pricing**: `PRICE`
is USD per hour, billed per second for as long as the client holds the session.
For the required RPC and wallets see
[On-chain (paid) setup](../README.md#on-chain-paid-setup) in the repo README.

```sh
cp .env.example .env   # fill in RPC, network, keystore paths, accounts, pricing
docker compose -f compose.yml -f compose.onchain.yml up -d --build
uv run client.py sample.mp4 \
  --discovery https://localhost:8935/discovery \
  --signer http://localhost:7936
docker compose -f compose.yml -f compose.onchain.yml down
```

A metered session pays **more than once**. Use a clip of at least a few tens of
seconds if you want to see repeated payments in
`docker compose logs -f orchestrator`.
