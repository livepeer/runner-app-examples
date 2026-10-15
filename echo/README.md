# Echo app (trickle realtime video)

A realtime video app on the Livepeer network: it receives a live video stream over **trickle** channels, optionally transforms each frame (gray / invert / blur) or the audio (robot), and echoes it back. This is the **live/stateful** path — continuous media over trickle, not request/response — so the app embeds the SDK and self-registers (dynamic).

|              |                                      |
| ------------ | ------------------------------------ |
| App id       | `livepeer-example/echo`              |
| Runner mode  | persistent (held-open session)       |
| Registration | dynamic (self-registers via the SDK) |
| Transport    | trickle (realtime video in/out)      |
| Pricing      | hour (metered per second)            |
| Port         | 8989                                 |

Prerequisites (Docker, `uv`, the [SDK](https://pypi.org/project/livepeer-gateway/)) and the shared setup are in the [repo README](../README.md).

## How it's wired

The app is **dynamically registered**: it self-registers with the orchestrator via `register_runner` ([runner.py](runner.py)) and exposes `POST /echo` (start a session, open trickle `in`/`out` channels with `create_trickle_channels`) and `POST /update` (change the transform mid-stream), both reverse-proxied through the orchestrator. The client calls it with `reserve_session` → `MediaPublish`/`MediaOutput` → `stop_runner_session` ([client.py](client.py)) — reserve a session, publish frames into `in`, read the transformed output from `out`, release. Grep `# Livepeer:` in either file to see the exact calls. Frame decode/encode is PyAV; the transforms are OpenCV.

The flow end to end, from startup to release:

```mermaid
sequenceDiagram
    participant R as Runner (runner.py)
    participant O as Orchestrator
    participant C as Client (client.py)
    participant T as Trickle channels (in, out)

    Note over R,O: Startup
    R->>O: register_runner(app, mode=persistent, price in USD per hour)
    O-->>R: registration (runner_id)

    Note over C,O: 1. Reserve a session
    C->>O: reserve_session(discovery_url, app, signer_url)
    opt paid path, signer_url set
        O-->>C: 402 payment challenge
        C->>O: answer the challenge through the signer
    end
    O-->>C: session (session_id, app_url)
    Note over C,O: On the paid path, the client keeps the session funded in the background until the async with block exits

    Note over C,R: 2. Open the channels
    C->>O: POST app_url/echo (mode, radius, audio)
    O->>R: forward POST /echo
    R->>O: create_trickle_channels(in, out) using the request's Session-Control header
    O-->>R: channel urls (public and internal)
    R-->>C: in_url and out_url, through the orchestrator

    Note over C,T: 3. Stream
    par client publishes
        loop for each decoded frame
            opt blur mode, when the next radius step is due
                C->>O: POST app_url/update (mode, radius)
                O->>R: forward POST /update
            end
            C->>T: MediaPublish: frame into in
        end
    and runner transforms
        T-->>R: MediaOutput: frames out of in
        R->>T: MediaPublish: transformed frames into out
    and client reads
        T-->>C: MediaOutput: frames out of out, written to a file or stdout
    end

    Note over C,O: 4. Release
    C->>T: close the publisher and wait for out to drain
    C->>O: leave the async with block, which stops the funding
    C->>O: stop_runner_session(session)
    Note over R,O: On runner shutdown: registration.close()
```

echo registers **dynamically** as the natural fit for a stateful app that already embeds the SDK (heartbeats, capacity, lifecycle). Trickle itself isn't tied to dynamic, though: `create_trickle_channels` rides the orchestrator's per-request `Livepeer-Session-Control` header, so a static runner exposing the same endpoints could open channels too.

## Held-open sessions — what this shows

**A trickle session is a pipe the client holds open, not a call it makes.** `hello-world`, `tiles` and `api-proxy` each answer one request and are finished. echo reserves a session once and then streams through it: frames go into the `in` channel and come back transformed from `out` continuously, with no request boundary in between.

Two things follow from that, and they are what this example exists to show:

- **The runner keeps state.** Each session owns its current transform and blur radius, which is why `POST /update` can change the effect mid-stream while frames keep flowing. A single-shot runner has nowhere to keep that between calls.
- **Billing becomes a lifecycle.** There is no call to bill against, so the session is metered per second for as long as it is held, and payment repeats for the life of the stream instead of settling once. The [on-chain section](#run-on-chain-paid) below exercises exactly that.

> [!NOTE]
> The session ends when the client releases it, not when the input runs out, which is why the client calls `stop_runner_session` on the way out. echo registers the default capacity of 1, so one held session occupies the runner and `/discovery` reports `capacity_available: 0` until it is released. See [`tiles`](../tiles) for what capacity does under fan-out.

## Run offchain (free)

> [!TIP]
> Built locally by the compose file below, or run the published [`ghcr.io/livepeer/runner-example-echo`](https://github.com/livepeer/runner-app-examples/pkgs/container/runner-example-echo) with `docker compose up -d --pull always` — see [Images](../README.md#images).

Start the stack and confirm the runner registered:

```sh
docker compose up -d --build
curl -sk https://localhost:8935/discovery | jq '.[].runners[].app'   # confirm livepeer-example/echo registered
```

The input is a file path, or `-` to read an MPEG-TS stream from stdin (so you can pipe in anything ffmpeg produces); the output is a file, or `-` to write the echoed stream to stdout (pipe it to a player).

**From a file** — writes the result to `echo-out.ts`. Any video works; the first command makes a 30s one (`-t` sets the length):

```sh
ffmpeg -f lavfi -i testsrc=size=1280x720:rate=30 -t 30 -c:v libx264 -preset ultrafast -pix_fmt yuv420p sample.mp4   # skip if you have a video
uv run client.py --mode blur --discovery https://localhost:8935/discovery sample.mp4
```

**Live from ffmpeg's test pattern** — no file needed; watch the test counter echo back in real time:

```sh
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 -c:v libx264 -tune zerolatency -preset ultrafast -pix_fmt yuv420p -f mpegts - \
  | uv run client.py - --mode blur --discovery https://localhost:8935/discovery --output - \
  | ffplay -fflags nobuffer -flags low_delay -framedrop -i -
```

**From a webcam** — same pipe; just point ffmpeg at your camera. Device numbers vary (a bare `/dev/video0` often isn't the camera, and some cameras expose several nodes), so find and confirm yours first:

```sh
v4l2-ctl --list-devices              # list cameras and their /dev/videoN
ffplay -f v4l2 -i /dev/video0        # preview a node to confirm it's your camera (q to quit); try video1, video2, ...

ffmpeg -f v4l2 -input_format mjpeg -framerate 30 -video_size 1280x720 -i /dev/video0 \
  -c:v libx264 -tune zerolatency -preset ultrafast -pix_fmt yuv420p -f mpegts - \
  | uv run client.py - --mode blur --discovery https://localhost:8935/discovery --output - \
  | ffplay -fflags nobuffer -flags low_delay -framedrop -i -
```

Swap `/dev/video0` for your node. If that size/format isn't supported, list the camera's modes with `ffmpeg -f v4l2 -list_formats all -i /dev/videoN`. macOS: `-f avfoundation -framerate 30 -i 0`; Windows: `-f dshow -i video="<name>"`.

The `ffplay` low-delay flags (`-fflags nobuffer -flags low_delay -framedrop`) keep the preview close to realtime; drop them and it buffers.

- `--mode` picks the transform: `echo` (passthrough, the default), `gray`, `invert`, `blur`, or `robot`. Use `--mode blur` on any command above to see the echo visibly transform the stream.
- `robot` ring-modulates the audio and leaves the video alone. It is the only mode that publishes an audio track, since a declared track that never gets a frame stalls the stream.
- `blur` sweeps the radius `0 -> max -> 0` live (driving `/update`); `--blur-period N` sets the seconds per sweep cycle (default 2; larger is slower). `gray`/`invert` are static.
- `--radius N` sets the initial blur strength, `--max-frames N` stops early.

**Hearing `robot`** — every command above is video-only, so `robot` would refuse them. Record yourself with a microphone (`arecord -l` lists capture devices), then play both files:

```sh
ffmpeg -f v4l2 -input_format mjpeg -video_size 1280x720 -framerate 30 -i /dev/video0 \
  -f alsa -i plughw:1,0 -filter_complex "[1:a]aresample=async=1:first_pts=0[a]" \
  -map 0:v -map "[a]" -fps_mode cfr -t 10 \
  -c:v libx264 -preset ultrafast -pix_fmt yuv420p -g 30 -c:a aac -ar 48000 -f mpegts -y me.ts

uv run client.py --mode robot --output me-robot.ts me.ts
ffplay -autoexit me.ts && ffplay -autoexit me-robot.ts   # you, then you ring-modulated
```

To hear it live instead, keep the same capture and swap the tail for `--output - -` piped into `ffplay -fflags nobuffer -i -`. Expect 2 to 4 seconds of lag, since trickle publishes in 2s segments, and wear headphones or the mic re-records the playback.

The camera and the audio device are separate clocks, so `aresample` and `-fps_mode cfr` align them; without both the publisher fails at the first segment boundary. On a multi-input interface add `-channels 6` and pick one input with `pan=mono|c0=c0`.

Stop the stack with `docker compose down`.

## Run on-chain (paid)

Layer `compose.onchain.yml` to run the orchestrator on-chain with a remote signer paying for the session. This example showcases **metered pricing**: `PRICE` is USD per hour, billed per second for as long as the client holds the session, the natural fit for a stream with no fixed length. For the required RPC and wallets see [On-chain (paid) setup](../README.md#on-chain-paid-setup) in the repo README.

```sh
cp .env.example .env   # fill in RPC, network, keystore paths, accounts, pricing
docker compose -f compose.yml -f compose.onchain.yml up -d --build
uv run client.py sample.mp4 --mode blur \
  --discovery https://localhost:8935/discovery \
  --signer http://localhost:7936
docker compose -f compose.yml -f compose.onchain.yml down
```

The ffmpeg and webcam pipes above work the same way on-chain: add `--signer` to the `client.py` in the pipeline. Without it the client stops at the payment challenge (`Live runner paid call requires signer_url`), which closes the pipe and leaves ffmpeg reporting `Broken pipe` — the paid stack refusing an unpaid caller, not a broken camera.

A metered session pays **more than once**. The upfront payment that answers the 402 challenge only buys the signer's preroll (about ten seconds), while the orchestrator keeps debiting every few seconds and releases the session on the first debit it cannot cover. So `reserve_session` keeps the session funded in the background for as long as the client holds it, and leaving the client's `async with session` block stops that before the session is released. A stream that outlives the preroll is the whole point of this example on-chain, so use a clip of at least a few tens of seconds (the 30s `sample.mp4` above is enough): watch `docker compose logs -f orchestrator` and you should see repeated payments, not one.

## Run without Docker

Start an orchestrator built from go-livepeer `v0.9.1` or newer (see [Build from source](https://docs.livepeer.org/v1/orchestrators/guides/install-go-livepeer#build-from-source)), then the app and client directly:

```sh
./livepeer -orchestrator -useLiveRunners -serviceAddr localhost:8935 -orchSecret abcdef -v 6
uv run runner.py --orchestrator https://localhost:8935 --orchSecret abcdef
uv run client.py --mode blur sample.mp4
```

Metered sessions rely on the session-scoped payment URL added in `v0.9.1` ([#4008](https://github.com/livepeer/go-livepeer/pull/4008)), which is the release the compose files pin.
