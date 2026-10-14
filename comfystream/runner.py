#!/usr/bin/env python3
"""ComfyStream example: Pipeline-backed analyze + live stream on Livepeer.

The comfystream package is installed in the image (pinned checkout). This file
is the live-runner app: registration, trickle channels, and the HTTP / WebSocket
surface, plus single-shot batch video tools. The tools run in a process
pool and do not share the live stream pipeline.

Agent surface:
  POST /analyze        video-in → text-out
  POST /start_stream   live video/audio (and optional text) trickle session
  POST /update_stream  mid-session prompt / resolution update
  GET  /ws_stream      browser-friendly WebSocket JPEG A/V (no trickle)
  GET  /text           buffered text outputs for the active session
  GET  /healthz
  GET  /tools/{name}/schema
  POST /tools/{name}   one-shot video tool (convert, compress, trim, resize,
                       gif, extract-audio, mute, rotate)

Livepeer integration (grep ``# Livepeer:``):
  1. register_runner()
  2. create_trickle_channels()  (analyze / start_stream only)
  3. registration.close() / on_session_release cleanup

``/ws_stream`` follows the realtime-transcription pattern: the orchestrator
proxies the WebSocket upgrade on the reserved session URL. Wire protocol:
  client -> server: text JSON {"type":"start","prompts":...,"width":N,"height":N}
  server -> client: text JSON {"type":"ready","session":...,"modalities":...}
  client -> server: binary JPEG frames (video)
  server -> client: binary JPEG frames (processed video)
  client -> server: text "eos" to end
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import os
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Optional

import av
import numpy as np
from aiohttp import web
from PIL import Image

from comfystream.modalities import WorkflowModality
from comfystream.pipeline import Pipeline
from comfystream.realtime.gpu import pinned_gpu
from comfystream.tool_pipeline import ToolPipeline
from comfystream.utils import convert_prompt
from livepeer_gateway.channel_writer import JSONLWriter
from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.live_runner import register_runner
from livepeer_gateway.media_decode import AudioDecodedMediaFrame, VideoDecodedMediaFrame
from livepeer_gateway.media_output import MediaOutput
from livepeer_gateway.media_publish import (
    AudioOutputConfig,
    MediaPublish,
    MediaPublishConfig,
    VideoOutputConfig,
)

log = logging.getLogger("comfystream")

APP_ID = "livepeer-example/comfystream"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8991
CHANNEL_MIME_VIDEO = "video/mp2t"
CHANNEL_MIME_JSONL = "application/jsonl"
TEXT_POLL_INTERVAL = 0.25
# aiohttp defaults each header field to 8190 bytes. Livepeer payment tickets
# for a metered session can exceed that and the server answers 400.
HEADER_LIMIT_BYTES = 262144
# 64 MB of video plus base64 overhead. Larger tool bodies are rejected.
TOOL_BODY_LIMIT_BYTES = 96 * 1024 * 1024
# LoadAudioTensor defaults to a 500ms buffer. JPEG ws_stream has no audio, so
# each video frame must enqueue at least this much silence or the graph waits
# 1s per frame and FPS collapses.
WS_SILENT_AUDIO_RATE = 48000
WS_SILENT_AUDIO_S = 0.5


@dataclass
class RunnerSession:
    session_id: str
    kind: str  # "analyze" | "stream" | "ws_stream"
    io: WorkflowModality
    in_url: str = ""
    out_url: str | None = None
    text_url: str | None = None
    media_in: MediaOutput | None = None
    video_out: MediaPublish | None = None
    text_out: JSONLWriter | None = None
    text_task: asyncio.Task | None = None
    output_task: asyncio.Task | None = None
    collected_text: list[str] = field(default_factory=list)
    prompts: Any = None

    def to_json(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "session": self.session_id,
            "kind": self.kind,
            "modalities": self.io,
        }
        if self.in_url:
            data["in"] = self.in_url
        if self.out_url:
            data["out"] = self.out_url
        if self.text_url:
            data["text"] = self.text_url
        return data


def _jpeg_to_av_frame(data: bytes) -> av.VideoFrame:
    bio = io.BytesIO(data)
    with av.open(bio, format="mjpeg") as container:
        for frame in container.decode(video=0):
            return frame.reformat(format="rgb24")
    raise ValueError("JPEG contained no video frame")


def _silent_audio_frame(
    duration_s: float = WS_SILENT_AUDIO_S,
    sample_rate: int = WS_SILENT_AUDIO_RATE,
) -> av.AudioFrame:
    samples = max(1, int(sample_rate * duration_s))
    frame = av.AudioFrame.from_ndarray(
        np.zeros((1, samples), dtype=np.int16),
        format="s16",
        layout="mono",
    )
    frame.sample_rate = sample_rate
    return frame


def _av_frame_to_jpeg(frame: av.VideoFrame, *, quality: int = 80) -> bytes:
    rgb = frame.to_ndarray(format="rgb24")
    img = Image.fromarray(rgb)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality)
    return buf.getvalue()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ComfyStream example app for the Livepeer network."
    )
    parser.add_argument(
        "--orchestrator",
        default=os.environ.get("LIVEPEER_ORCH_URL", "https://localhost:8935"),
    )
    parser.add_argument(
        "--orchSecret",
        default=os.environ.get(
            "LIVEPEER_ORCH_SECRET", os.environ.get("ORCH_SECRET", "abcdef")
        ),
    )
    parser.add_argument(
        "--runner-url",
        default=os.environ.get(
            "LIVEPEER_RUNNER_URL", f"http://{DEFAULT_HOST}:{DEFAULT_PORT}"
        ),
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("LIVEPEER_RUNNER_HOST", DEFAULT_HOST),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("LIVEPEER_RUNNER_PORT", str(DEFAULT_PORT))),
    )
    parser.add_argument(
        "--workspace",
        default=os.environ.get("COMFYUI_CWD", os.environ.get("COMFYUI_WORKSPACE", "")),
        help="ComfyUI workspace directory (COMFYUI_CWD).",
    )
    parser.add_argument("--width", type=int, default=512)
    parser.add_argument("--height", type=int, default=512)
    parser.add_argument(
        "--price",
        type=float,
        default=float(os.environ.get("LIVEPEER_RUNNER_PRICE", "0")),
        help="USD per hour (metered). 0 = free, the offchain default.",
    )
    parser.add_argument(
        "--capacity",
        type=int,
        default=int(os.environ.get("LIVEPEER_RUNNER_CAPACITY", "1")),
    )
    parser.add_argument(
        "--skip-bootstrap",
        action="store_true",
        help="Skip Pipeline default-workflow bootstrap (faster bring-up if workspace is ready).",
    )
    parser.add_argument(
        "--skip-tools",
        action="store_true",
        help="Do not register or serve the batch video tools.",
    )
    parser.add_argument(
        "--batch-workers",
        type=int,
        default=int(os.environ.get("COMFYSTREAM_BATCH_WORKERS", "2")),
        help="Process-pool size shared by the batch video tools.",
    )
    return parser.parse_args()


def _session_id(request: web.Request) -> str:
    session_id = request.headers.get("Livepeer-Session-Id", "").strip()
    if not session_id:
        raise web.HTTPBadRequest(text="missing Livepeer-Session-Id header")
    return session_id


def _channel_url(channel: dict[str, Any], *, internal: bool = False) -> str:
    if internal:
        return str(channel.get("internal_url") or channel["url"])
    return str(channel["url"])


def _extract_prompts(payload: dict[str, Any]) -> Any:
    prompts = payload.get("prompts", payload.get("prompt"))
    if prompts is None:
        raise web.HTTPBadRequest(text="missing prompts/prompt in body")
    if isinstance(prompts, str):
        prompts = json.loads(prompts)
    return prompts


def _convert_prompts(prompts: Any) -> list[dict[str, Any]]:
    if isinstance(prompts, list):
        return [convert_prompt(p, return_dict=True) for p in prompts]
    return [convert_prompt(prompts, return_dict=True)]


def _require_analyze_io(io: WorkflowModality) -> None:
    if not io["video"]["input"]:
        raise web.HTTPBadRequest(text="analyze requires a workflow with video input")
    if not io["text"]["output"]:
        raise web.HTTPBadRequest(text="analyze requires a workflow with text output")


def _require_stream_io(io: WorkflowModality) -> None:
    if not (io["video"]["input"] or io["audio"]["input"] or io["video"]["output"]):
        raise web.HTTPBadRequest(text="start_stream requires a workflow with media I/O")


async def _close_session(app: web.Application, *, stop_prompts: bool = True) -> None:
    session: RunnerSession | None = app.get("session")
    if session is None:
        return
    app["session"] = None

    if session.text_task is not None and not session.text_task.done():
        session.text_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await session.text_task
    if session.output_task is not None and not session.output_task.done():
        session.output_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await session.output_task

    with suppress(Exception):
        if session.media_in is not None:
            await session.media_in.close()
    with suppress(Exception):
        if session.video_out is not None:
            await session.video_out.close()
    with suppress(Exception):
        if session.text_out is not None:
            await session.text_out.close()

    pipeline: Pipeline | None = app.get("pipeline")
    if stop_prompts and pipeline is not None:
        with suppress(Exception):
            await pipeline.stop_prompts(cleanup=True)


async def _ensure_streaming(app: web.Application, pipeline: Pipeline) -> None:
    """Start the prompt runner once an input frame is already queued.

    Starting in ``/start_stream`` races trickle delivery: LoadTensor times out
    before the first segment arrives. Queue the frame, then start.
    """
    lock: asyncio.Lock = app["stream_lock"]
    async with lock:
        if pipeline.are_prompts_running():
            return
        if pipeline.state_manager.can_stream():
            await pipeline.start_streaming()


def _close_session_on_media_failure(app: web.Application, task: asyncio.Task) -> None:
    """Drop the session only when the trickle reader fails.

    A normal end of input must leave the session up: the analyze client reads
    ``/text`` after it finishes publishing, and ``stop_runner_session`` is what
    releases a persistent session.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        return
    # The publisher closing the trickle input surfaces as EOF. That is the
    # normal end of a file, not a failed session.
    if isinstance(exc, LivepeerGatewayError) and "EOF" in str(exc):
        return
    log.error("media input failed: %s", exc)
    asyncio.create_task(_close_session(app))


async def _on_session_release(app: web.Application, event: Any) -> None:
    session_id = getattr(event, "session_id", "") or ""
    session: RunnerSession | None = app.get("session")
    if session is None:
        return
    if session_id and session.session_id != session_id:
        return
    log.info("orchestrator released session %s; cleaning up", session.session_id)
    await _close_session(app)


async def _text_forward_loop(app: web.Application, session: RunnerSession) -> None:
    pipeline: Pipeline = app["pipeline"]
    while True:
        try:
            text = await pipeline.get_text_output()
            if text is None or str(text).strip() == "":
                await asyncio.sleep(TEXT_POLL_INTERVAL)
                continue
            text_str = str(text)
            session.collected_text.append(text_str)
            if session.text_out is not None:
                await session.text_out.write({"type": "text", "text": text_str})
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("text forwarder error")
            await asyncio.sleep(TEXT_POLL_INTERVAL)


def _ensure_output_pump(app: web.Application, session: RunnerSession) -> None:
    """Pull processed frames off the input callback.

    Awaiting ``get_processed_video_frame`` inside ``on_frame`` stalls the trickle
    reader, so the prompt runner times out waiting for the next input.
    """
    if session.kind == "analyze":
        return
    if session.output_task is not None and not session.output_task.done():
        return
    session.output_task = asyncio.create_task(_output_pump(app, session))


async def _output_pump(app: web.Application, session: RunnerSession) -> None:
    pipeline: Pipeline = app["pipeline"]
    try:
        while app.get("session") is session:
            if pipeline.produces_video_output():
                out = await pipeline.get_processed_video_frame()
                if app.get("session") is not session:
                    break
                if session.video_out is not None:
                    await session.video_out.write_frame(out)
            elif pipeline.accepts_video_input():
                await pipeline.video_incoming_frames.get()
            else:
                break
            if pipeline.produces_audio_output():
                with suppress(Exception):
                    audio = await asyncio.wait_for(
                        pipeline.get_processed_audio_frame(),
                        timeout=0.05,
                    )
                    if session.video_out is not None:
                        await session.video_out.write_frame(audio)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("output pump failed session=%s", session.session_id)


async def _handle_media_frame(
    app: web.Application,
    session: RunnerSession,
    decoded: AudioDecodedMediaFrame | VideoDecodedMediaFrame,
) -> None:
    pipeline: Pipeline = app["pipeline"]
    frame = decoded.frame
    if decoded.kind == "audio":
        await pipeline.put_audio_frame(frame)
        await _ensure_streaming(app, pipeline)
        _ensure_output_pump(app, session)
        return

    if decoded.kind != "video":
        return

    await pipeline.put_video_frame(frame)
    await _ensure_streaming(app, pipeline)
    if pipeline.produces_video_output():
        _ensure_output_pump(app, session)
    else:
        # Video-in / text-out: drain the sync queue without waiting for a video tensor.
        await pipeline.video_incoming_frames.get()


def _parse_send_audio(payload: dict[str, Any]) -> bool:
    send_audio = payload.get("audio", False)
    if not isinstance(send_audio, bool):
        raise web.HTTPBadRequest(text="audio must be a boolean")
    return send_audio


def _media_publish_for_stream(
    out_url: str,
    io: WorkflowModality,
    *,
    send_audio: bool,
) -> MediaPublish:
    tracks: list[VideoOutputConfig | AudioOutputConfig] = []
    if io["video"]["output"]:
        tracks.append(VideoOutputConfig())
    # Only declare an audio track when the client will publish audio, otherwise
    # the container stalls waiting for a first audio frame.
    if send_audio and io["audio"]["output"]:
        tracks.append(AudioOutputConfig(sample_rate=48000))
    if not tracks:
        tracks.append(VideoOutputConfig())
    return MediaPublish(out_url, config=MediaPublishConfig(tracks=tracks))


async def _apply_workflow(
    pipeline: Pipeline,
    prompts: Any,
    *,
    width: Optional[int],
    height: Optional[int],
    skip_warmup: bool = False,
    start_stream: bool = True,
) -> WorkflowModality:
    converted = _convert_prompts(prompts)
    if width and width > 0:
        pipeline.width = int(width)
    if height and height > 0:
        pipeline.height = int(height)
    await pipeline.apply_prompts(converted, skip_warmup=skip_warmup)
    if not skip_warmup:
        await pipeline.ensure_warmup(pipeline.width, pipeline.height)
    if start_stream and pipeline.state_manager.can_stream():
        await pipeline.start_streaming()
    elif pipeline.are_prompts_running():
        # apply_prompts restarts streaming if a previous session left it running.
        # A trickle stream has no frames yet, so leave the runner paused until
        # the first input is queued.
        await pipeline.stop_streaming()
    return pipeline.get_workflow_io_capabilities()


async def _handle_analyze(request: web.Request) -> web.Response:
    app = request.app
    session_id = _session_id(request)
    existing: RunnerSession | None = app.get("session")
    if existing is not None:
        if existing.session_id != session_id:
            raise web.HTTPConflict(text="runner already has an active session")
        return web.json_response(existing.to_json())

    payload = json.loads(await request.read() or b"{}")
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="body must be a JSON object")
    prompts = _extract_prompts(payload)
    width = payload.get("width")
    height = payload.get("height")

    pipeline: Pipeline = app["pipeline"]
    try:
        io = await _apply_workflow(
            pipeline,
            prompts,
            width=int(width) if width else None,
            height=int(height) if height else None,
        )
    except web.HTTPException:
        raise
    except Exception as exc:
        log.exception("failed to apply analyze workflow")
        raise web.HTTPBadRequest(text="invalid workflow") from exc

    _require_analyze_io(io)

    channels = await app["registration"].create_trickle_channels(  # Livepeer: 2
        request,
        [
            {"name": "in", "mime_type": CHANNEL_MIME_VIDEO},
            {"name": "text", "mime_type": CHANNEL_MIME_JSONL},
        ],
    )
    by_name = {c["name"]: c for c in channels}
    if "in" not in by_name or "text" not in by_name:
        raise web.HTTPInternalServerError(
            text="orchestrator did not return in/text channels"
        )

    session = RunnerSession(
        session_id=session_id,
        kind="analyze",
        io=io,
        in_url=_channel_url(by_name["in"]),
        text_url=_channel_url(by_name["text"]),
        text_out=JSONLWriter(_channel_url(by_name["text"], internal=True)),
        prompts=prompts,
    )

    async def _on_frame(decoded) -> None:
        await _handle_media_frame(app, session, decoded)

    session.media_in = MediaOutput(
        _channel_url(by_name["in"], internal=True),
        on_frame=_on_frame,
    )
    session.text_task = asyncio.create_task(_text_forward_loop(app, session))
    app["session"] = session

    for task in session.media_in.callback_tasks():
        task.add_done_callback(lambda t: _close_session_on_media_failure(app, t))

    log.info("started analyze session %s", session_id)
    return web.json_response(session.to_json())


async def _handle_start_stream(request: web.Request) -> web.Response:
    app = request.app
    session_id = _session_id(request)
    existing: RunnerSession | None = app.get("session")
    if existing is not None:
        if existing.session_id != session_id:
            raise web.HTTPConflict(text="runner already has an active session")
        return web.json_response(existing.to_json())

    payload = json.loads(await request.read() or b"{}")
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="body must be a JSON object")
    prompts = _extract_prompts(payload)
    width = payload.get("width")
    height = payload.get("height")
    send_audio = _parse_send_audio(payload)

    pipeline: Pipeline = app["pipeline"]
    try:
        # Skip warmup: trickle media is not connected until after this response,
        # so warmup would hang waiting for LoadTensor/LoadAudioTensor frames.
        io = await _apply_workflow(
            pipeline,
            prompts,
            width=int(width) if width else None,
            height=int(height) if height else None,
            skip_warmup=True,
            start_stream=False,
        )
    except Exception as exc:
        log.exception("failed to apply stream workflow")
        raise web.HTTPBadRequest(text="invalid workflow") from exc

    _require_stream_io(io)

    channel_reqs: list[dict[str, str]] = []
    if io["video"]["input"] or io["audio"]["input"]:
        channel_reqs.append({"name": "in", "mime_type": CHANNEL_MIME_VIDEO})
    if io["video"]["output"] or io["audio"]["output"]:
        channel_reqs.append({"name": "out", "mime_type": CHANNEL_MIME_VIDEO})
    if io["text"]["output"]:
        channel_reqs.append({"name": "text", "mime_type": CHANNEL_MIME_JSONL})
    if not channel_reqs:
        raise web.HTTPBadRequest(text="workflow produced no trickle channels")

    channels = await app["registration"].create_trickle_channels(  # Livepeer: 2
        request,
        channel_reqs,
    )
    by_name = {c["name"]: c for c in channels}

    session = RunnerSession(
        session_id=session_id,
        kind="stream",
        io=io,
        in_url=_channel_url(by_name["in"]) if "in" in by_name else "",
        out_url=_channel_url(by_name["out"]) if "out" in by_name else None,
        text_url=_channel_url(by_name["text"]) if "text" in by_name else None,
        prompts=prompts,
    )
    if "out" in by_name:
        session.video_out = _media_publish_for_stream(
            _channel_url(by_name["out"], internal=True),
            io,
            send_audio=send_audio,
        )
    if "text" in by_name:
        session.text_out = JSONLWriter(_channel_url(by_name["text"], internal=True))
        session.text_task = asyncio.create_task(_text_forward_loop(app, session))

    if "in" in by_name:

        async def _on_frame(decoded) -> None:
            await _handle_media_frame(app, session, decoded)

        session.media_in = MediaOutput(
            _channel_url(by_name["in"], internal=True),
            on_frame=_on_frame,
        )
        for task in session.media_in.callback_tasks():
            task.add_done_callback(lambda t: _close_session_on_media_failure(app, t))

    app["session"] = session
    log.info("started stream session %s audio=%s", session_id, send_audio)
    return web.json_response(session.to_json())


async def _handle_update_stream(request: web.Request) -> web.Response:
    app = request.app
    session_id = _session_id(request)
    session: RunnerSession | None = app.get("session")
    if session is None:
        raise web.HTTPNotFound(text="no active session")
    if session.session_id != session_id:
        raise web.HTTPConflict(text="runner has a different active session")

    payload = json.loads(await request.read() or b"{}")
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="body must be a JSON object")

    pipeline: Pipeline = app["pipeline"]
    width = payload.get("width")
    height = payload.get("height")
    if width:
        pipeline.width = int(width)
    if height:
        pipeline.height = int(height)

    if "prompts" in payload or "prompt" in payload:
        prompts = _extract_prompts(payload)
        try:
            io = await _apply_workflow(
                pipeline,
                prompts,
                width=int(width) if width else None,
                height=int(height) if height else None,
                skip_warmup=True,
            )
        except Exception as exc:
            log.exception("failed to update stream workflow")
            raise web.HTTPBadRequest(text="invalid workflow update") from exc
        session.io = io
        session.prompts = prompts
        if io["text"]["output"] and session.text_task is None:
            if session.text_out is None and session.text_url:
                session.text_out = JSONLWriter(session.text_url)
            if session.text_out is not None:
                session.text_task = asyncio.create_task(
                    _text_forward_loop(app, session)
                )

    return web.json_response(session.to_json())


async def _handle_text(request: web.Request) -> web.Response:
    session: RunnerSession | None = request.app.get("session")
    if session is None:
        raise web.HTTPNotFound(text="no active session")
    session_id = request.headers.get("Livepeer-Session-Id", "").strip()
    if session_id and session_id != session.session_id:
        raise web.HTTPConflict(text="runner has a different active session")
    return web.json_response(
        {
            "session": session.session_id,
            "texts": list(session.collected_text),
        }
    )


async def _handle_ws_stream(request: web.Request) -> web.WebSocketResponse:
    """Browser JPEG stream over orch-proxied WebSocket (no trickle)."""
    app = request.app
    session_id = request.headers.get("Livepeer-Session-Id", "").strip()
    if not session_id:
        raise web.HTTPBadRequest(text="missing Livepeer-Session-Id header")

    existing: RunnerSession | None = app.get("session")
    if existing is not None and existing.session_id != session_id:
        raise web.HTTPConflict(text="runner already has an active session")

    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=8 * 1024 * 1024)
    await ws.prepare(request)
    log.info("ws_stream opened session=%s", session_id)

    pipeline: Pipeline = app["pipeline"]
    session: RunnerSession | None = (
        existing if existing and existing.kind == "ws_stream" else None
    )
    started = session is not None
    streaming = pipeline.are_prompts_running()
    emit_task: asyncio.Task | None = None

    async def _emit_outputs() -> None:
        """Pull processed frames independently of JPEG ingest.

        Request/response on the same ``async for`` deadlocks: the prompt runner
        times out waiting for the next input while this handler is blocked on
        the previous output, so the browser freezes on the first frame.
        """
        try:
            while not ws.closed:
                if pipeline.produces_video_output():
                    out = await pipeline.get_processed_video_frame()
                    if ws.closed:
                        break
                    await ws.send_bytes(_av_frame_to_jpeg(out))
                elif pipeline.accepts_video_input():
                    await pipeline.video_incoming_frames.get()
                else:
                    break
                if pipeline.produces_audio_output():
                    with suppress(Exception):
                        await asyncio.wait_for(
                            pipeline.get_processed_audio_frame(),
                            timeout=0.05,
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ws_stream output failed session=%s", session_id)

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                text = msg.data.strip() if isinstance(msg.data, str) else ""
                if text == "eos":
                    break
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    await ws.send_json({"type": "error", "error": "invalid JSON"})
                    continue
                if not isinstance(payload, dict):
                    await ws.send_json(
                        {"type": "error", "error": "body must be a JSON object"}
                    )
                    continue
                if payload.get("type") != "start":
                    await ws.send_json(
                        {"type": "error", "error": "expected type=start"}
                    )
                    continue
                if started:
                    await ws.send_json(
                        {"type": "ready", **(session.to_json() if session else {})}
                    )
                    continue
                try:
                    prompts = _extract_prompts(payload)
                    width = payload.get("width")
                    height = payload.get("height")
                    io = await _apply_workflow(
                        pipeline,
                        prompts,
                        width=int(width) if width else None,
                        height=int(height) if height else None,
                        skip_warmup=True,
                        start_stream=False,
                    )
                    _require_stream_io(io)
                except web.HTTPException as exc:
                    await ws.send_json({"type": "error", "error": exc.text or str(exc)})
                    break
                except Exception:
                    log.exception("ws_stream failed to apply workflow")
                    await ws.send_json({"type": "error", "error": "invalid workflow"})
                    break

                session = RunnerSession(
                    session_id=session_id,
                    kind="ws_stream",
                    io=io,
                    prompts=prompts,
                )
                app["session"] = session
                started = True
                await ws.send_json({"type": "ready", **session.to_json()})
                log.info("ws_stream ready session=%s", session_id)

            elif msg.type == web.WSMsgType.BINARY:
                if not started or session is None:
                    await ws.send_json(
                        {"type": "error", "error": "send type=start before frames"}
                    )
                    continue
                if (
                    not pipeline.produces_video_output()
                    and not pipeline.accepts_video_input()
                ):
                    continue
                try:
                    frame = _jpeg_to_av_frame(msg.data)
                    # AV workflows still pull LoadAudioTensor when the browser
                    # only sends JPEGs.
                    if pipeline.accepts_audio_input():
                        await pipeline.put_audio_frame(_silent_audio_frame())
                    await pipeline.put_video_frame(frame)
                    if not streaming and pipeline.state_manager.can_stream():
                        await pipeline.start_streaming()
                        streaming = True
                        if emit_task is None or emit_task.done():
                            emit_task = asyncio.create_task(_emit_outputs())
                except Exception:
                    log.exception("ws_stream frame failed")
                    await ws.send_json(
                        {"type": "error", "error": "frame processing failed"}
                    )
                    break

            elif msg.type == web.WSMsgType.ERROR:
                log.warning("ws_stream error: %s", ws.exception())
                break
    finally:
        if emit_task is not None and not emit_task.done():
            emit_task.cancel()
            with suppress(asyncio.CancelledError):
                await emit_task
        current: RunnerSession | None = app.get("session")
        if (
            current is not None
            and current.session_id == session_id
            and current.kind == "ws_stream"
        ):
            await _close_session(app)
        log.info("ws_stream closed session=%s", session_id)
    return ws


async def _handle_healthz(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "app": APP_ID})


async def _handle_tool_schema(request: web.Request) -> web.Response:
    tools: ToolPipeline | None = request.app.get("tools")
    if tools is None:
        raise web.HTTPNotFound(text="video tools are disabled")
    item = tools.get(request.match_info["name"])
    if item is None:
        raise web.HTTPNotFound(text="unknown tool")
    return web.json_response(item.schema)


async def _handle_tool_execute(request: web.Request) -> web.Response:
    tools: ToolPipeline | None = request.app.get("tools")
    if tools is None:
        raise web.HTTPNotFound(text="video tools are disabled")
    status, payload = await tools.execute(
        request.match_info["name"], await request.read()
    )
    return web.json_response(payload, status=status)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = _parse_args()
    if not args.workspace:
        raise SystemExit("--workspace / COMFYUI_CWD is required")

    async def _on_startup(app: web.Application) -> None:
        pipeline = Pipeline(
            width=args.width,
            height=args.height,
            cwd=args.workspace,
            disable_cuda_malloc=True,
            gpu_only=True,
            preview_method="none",
            blacklist_custom_nodes=["ComfyUI-Manager", "ComfyUI-StreamDiffusion"],
            bootstrap_default_prompt=not args.skip_bootstrap,
        )
        await pipeline.initialize()
        app["pipeline"] = pipeline
        app["session"] = None
        app["stream_lock"] = asyncio.Lock()

        async def _release(event: Any) -> None:
            await _on_session_release(app, event)

        app["registration"] = await register_runner(  # Livepeer: 1
            args.orchestrator,
            secret=args.orchSecret,
            runner_url=args.runner_url,
            app=APP_ID,
            mode="persistent",
            capacity=args.capacity,
            price=args.price,
            currency="usd",
            unit="hour",
            metadata=(
                '{"modalities":"workflow-driven",'
                '"surfaces":["analyze","start_stream","update_stream","ws_stream"]}'
            ),
            gpu=pinned_gpu(),
            on_session_release=_release,
        )
        log.info(
            "registered app=%s runner_id=%s orchestrator=%s runner_url=%s",
            APP_ID,
            app["registration"].runner_id,
            app["registration"].orchestrator_url,
            args.runner_url,
        )

        app["tools"] = None
        if not args.skip_tools:
            tools = ToolPipeline(
                orchestrator=args.orchestrator,
                orch_secret=args.orchSecret,
                runner_base_url=args.runner_url,
                batch_workers=args.batch_workers,
                workspace=args.workspace,
            )
            await tools.start()
            app["tools"] = tools

    async def _on_cleanup(app: web.Application) -> None:
        await _close_session(app)
        tools = app.get("tools")
        if tools is not None:
            with suppress(Exception):
                await tools.close()
        with suppress(Exception):
            await app["registration"].close()  # Livepeer: 3

    app = web.Application(
        client_max_size=TOOL_BODY_LIMIT_BYTES,
        handler_args={
            "max_line_size": HEADER_LIMIT_BYTES,
            "max_field_size": HEADER_LIMIT_BYTES,
        },
    )
    app.router.add_post("/analyze", _handle_analyze)
    app.router.add_post("/start_stream", _handle_start_stream)
    app.router.add_post("/update_stream", _handle_update_stream)
    app.router.add_get("/ws_stream", _handle_ws_stream)
    app.router.add_get("/text", _handle_text)
    app.router.add_get("/healthz", _handle_healthz)
    app.router.add_get("/tools/{name}/schema", _handle_tool_schema)
    app.router.add_post("/tools/{name}", _handle_tool_execute)
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
