#!/usr/bin/env python3
"""Smoke client for the ComfyStream example: analyze, then optional live stream.

Usage:
  uv run client.py sample.mp4
  uv run client.py sample.mp4 --mode stream \\
    --workflow workflows/invert-color-av-passthrough-api.json

Livepeer integration (grep ``# Livepeer:``):
  1. reserve_session()
  2. post_json / MediaPublish through session.app_url (orch injects session headers)
  3. stop_runner_session()
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
from contextlib import suppress
from pathlib import Path
from typing import Any

import av

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.http import get_json, post_json
from livepeer_gateway.live_runner import call_runner, stop_runner_session
from livepeer_gateway.media_publish import (
    AudioOutputConfig,
    MediaPublish,
    MediaPublishConfig,
    VideoOutputConfig,
)
from livepeer_gateway.selection import reserve_session, runner_selector

APP_ID = "livepeer-example/comfystream"
DEFAULT_DISCOVERY = "https://localhost:8935/discovery"
DEFAULT_WORKFLOW = "workflows/analyze-stub-api.json"
log = logging.getLogger("comfystream-client")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ComfyStream live-runner smoke client."
    )
    parser.add_argument("input", help="Input video file.")
    parser.add_argument("--discovery", default=DEFAULT_DISCOVERY)
    parser.add_argument(
        "--signer", default="", help="Remote signer URL for on-chain path."
    )
    parser.add_argument(
        "--signer-auth",
        default="",
        help='Authorization header value for signer (e.g. "Bearer pmth_..." or "Bearer app_...").',
    )
    parser.add_argument(
        "--discovery-auth",
        default="",
        help="Optional Authorization header for discovery (defaults to --signer-auth).",
    )
    parser.add_argument(
        "--workflow",
        default=DEFAULT_WORKFLOW,
        help="ComfyUI API-format workflow JSON (default: analyze stub).",
    )
    parser.add_argument(
        "--mode",
        choices=("analyze", "stream", "both"),
        default="analyze",
        help="Which surface to exercise (default: analyze).",
    )
    parser.add_argument("--max-frames", type=int, default=30)
    parser.add_argument("--width", type=int, default=512)
    parser.add_argument("--height", type=int, default=512)
    parser.add_argument(
        "--audio",
        action="store_true",
        help="Publish interleaved audio when the input has an audio stream.",
    )
    parser.add_argument(
        "--update-workflow",
        default="",
        help="Optional second workflow JSON for update_stream.",
    )
    parser.add_argument(
        "--tool",
        default="",
        help=(
            "Batch video tool: convert, compress, trim, resize, gif, "
            "extract-audio, mute, or rotate."
        ),
    )
    parser.add_argument(
        "--format", default="", help="Output format for convert or extract-audio."
    )
    parser.add_argument(
        "--quality", default="", help="compress quality: high, medium, or low."
    )
    parser.add_argument(
        "--target-bytes", type=int, default=0, help="compress toward this size."
    )
    parser.add_argument("--start", default="", help="trim or gif start time.")
    parser.add_argument("--end", default="", help="trim end time.")
    parser.add_argument(
        "--trim-mode",
        default="precise",
        choices=("fast", "precise"),
        help="trim mode.",
    )
    parser.add_argument(
        "--duration", type=float, default=0, help="gif length in seconds, max 30."
    )
    parser.add_argument("--fps", type=int, default=10, help="gif frame rate.")
    parser.add_argument("--tool-width", type=int, default=0, help="gif width.")
    parser.add_argument(
        "--tool-height",
        type=int,
        default=720,
        help="resize height: 2160, 1440, 1080, 720, 480, 360, 240, or 144.",
    )
    parser.add_argument(
        "--rotation", type=int, default=0, help="clockwise degrees: 0, 90, 180, 270."
    )
    parser.add_argument(
        "--flip",
        default="none",
        choices=("none", "horizontal", "vertical"),
        help="flip after rotation.",
    )
    parser.add_argument("--output", default="", help="Where to write the tool result.")
    return parser.parse_args()


def _load_workflow(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


async def _publish_frames(
    publish_url: str,
    input_path: str,
    *,
    max_frames: int,
    send_audio: bool,
) -> None:
    tracks: list[VideoOutputConfig | AudioOutputConfig] = [VideoOutputConfig()]
    if send_audio:
        tracks.append(AudioOutputConfig(sample_rate=48000))
    publisher = MediaPublish(
        publish_url,
        config=MediaPublishConfig(tracks=tracks),
    )
    try:
        container = av.open(input_path)
        if send_audio and not container.streams.audio:
            raise LivepeerGatewayError(
                f"--audio requested but input has no audio stream: {input_path}"
            )
        sent_video = 0
        frames = container.decode() if send_audio else container.decode(video=0)
        for frame in frames:
            if isinstance(frame, av.VideoFrame):
                await publisher.write_frame(frame)
                sent_video += 1
                if max_frames and sent_video >= max_frames:
                    break
            else:
                await publisher.write_frame(frame)
        container.close()
        log.info(
            "published %d video frames to %s (audio=%s)",
            sent_video,
            publish_url,
            send_audio,
        )
    finally:
        await publisher.close()


def _auth_headers(
    args: argparse.Namespace,
) -> tuple[str | None, dict[str, str] | None, dict[str, str] | None]:
    signer_url = args.signer.strip() or None
    auth = args.signer_auth.strip()
    discovery_auth = args.discovery_auth.strip() or auth
    signer_headers = {"Authorization": auth} if auth else None
    discovery_headers = {"Authorization": discovery_auth} if discovery_auth else None
    return signer_url, signer_headers, discovery_headers


async def _run_analyze(args: argparse.Namespace, workflow: Any) -> None:
    signer_url, signer_headers, discovery_headers = _auth_headers(args)
    session = await reserve_session(  # Livepeer: 1
        discovery_url=args.discovery,
        app=APP_ID,
        signer_url=signer_url,
        signer_headers=signer_headers,
        discovery_headers=discovery_headers,
    )
    try:
        async with session:
            data = await post_json(  # Livepeer: 2
                f"{session.app_url.rstrip('/')}/analyze",
                {
                    "prompts": workflow,
                    "width": args.width,
                    "height": args.height,
                },
                timeout=120.0,
            )
            log.info("analyze started: %s", data)
            await _publish_frames(
                data["in"],
                args.input,
                max_frames=args.max_frames,
                send_audio=False,
            )
            await asyncio.sleep(2.0)
            texts = await get_json(f"{session.app_url.rstrip('/')}/text", timeout=30.0)
            log.info("analyze texts: %s", texts)
    except LivepeerGatewayError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    finally:
        with suppress(Exception):
            await stop_runner_session(session)  # Livepeer: 3


async def _run_stream(args: argparse.Namespace, workflow: Any) -> None:
    signer_url, signer_headers, discovery_headers = _auth_headers(args)
    session = await reserve_session(  # Livepeer: 1
        discovery_url=args.discovery,
        app=APP_ID,
        signer_url=signer_url,
        signer_headers=signer_headers,
        discovery_headers=discovery_headers,
    )
    try:
        async with session:
            data = await post_json(  # Livepeer: 2
                f"{session.app_url.rstrip('/')}/start_stream",
                {
                    "prompts": workflow,
                    "width": args.width,
                    "height": args.height,
                    "audio": bool(args.audio),
                },
                timeout=120.0,
            )
            log.info("stream started: %s", data)

            publish = asyncio.create_task(
                _publish_frames(
                    data["in"],
                    args.input,
                    max_frames=args.max_frames,
                    send_audio=bool(args.audio),
                )
            )
            if args.update_workflow:
                await asyncio.sleep(1.0)
                update = _load_workflow(args.update_workflow)
                updated = await post_json(
                    f"{session.app_url.rstrip('/')}/update_stream",
                    {"prompts": update},
                    timeout=60.0,
                )
                log.info("update_stream: %s", updated)
            await publish
            # Trickle delivers a segment on close, and LoadTensor waits up to 1s
            # before retrying. Stay connected long enough for that retry to run.
            await asyncio.sleep(3.0)
    except LivepeerGatewayError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    finally:
        with suppress(Exception):
            await stop_runner_session(session)  # Livepeer: 3
        log.info("stream session stopped")


def _tool_payload(args: argparse.Namespace, video: bytes) -> dict[str, Any]:
    payload: dict[str, Any] = {"video_base64": base64.b64encode(video).decode("ascii")}
    if args.format:
        payload["format"] = args.format
    if args.quality:
        payload["quality"] = args.quality
    if args.target_bytes:
        payload["target_bytes"] = args.target_bytes
    if args.start:
        payload["start"] = args.start
    if args.end:
        payload["end"] = args.end
    if args.tool == "trim":
        payload["mode"] = args.trim_mode
    if args.duration:
        payload["duration"] = args.duration
    if args.tool == "gif":
        payload["fps"] = args.fps
        if args.tool_width:
            payload["width"] = args.tool_width
    if args.tool == "resize":
        payload["height"] = args.tool_height
    if args.tool == "rotate":
        payload["rotation"] = args.rotation
        payload["flip"] = args.flip
    return payload


async def _run_tool(args: argparse.Namespace) -> int:
    signer_url, signer_headers, discovery_headers = _auth_headers(args)
    app_id = f"{APP_ID}-{args.tool}"
    video = Path(args.input).read_bytes()
    if len(video) > 64 * 1024 * 1024:
        raise SystemExit("input exceeds 64 MB")
    try:
        cursor = await runner_selector(  # Livepeer: 1
            discovery_url=args.discovery,
            app=app_id,
            signer_url=signer_url,
            signer_headers=signer_headers,
            discovery_headers=discovery_headers,
        )
        runner = cursor.candidates[0]
        result = await call_runner(  # Livepeer: 2
            runner=runner,
            runner_url=runner.url.rstrip("/") + f"/tools/{args.tool}",
            payload=_tool_payload(args, video),
            signer_url=signer_url,
            signer_headers=signer_headers,
            timeout=180.0,
        )
    except LivepeerGatewayError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    data = result.data or {}
    encoded = data.get("video_base64") or data.get("audio_base64")
    if not isinstance(encoded, str) or not encoded:
        raise SystemExit(f"ERROR: tool returned no media: {data}")
    output = args.output or str(
        Path(args.input).with_name(f"{Path(args.input).stem}-{args.tool}")
    )
    Path(output).write_bytes(base64.b64decode(encoded))
    log.info("wrote %s (%s)", output, data.get("content_type", ""))
    return 0


async def _amain() -> int:
    args = _parse_args()
    input_path = Path(args.input).expanduser()
    if not input_path.exists():
        raise SystemExit(f"input file does not exist: {input_path}")
    args.input = str(input_path)
    if args.tool:
        return await _run_tool(args)
    workflow = _load_workflow(args.workflow)
    if args.mode in ("analyze", "both"):
        await _run_analyze(args, workflow)
    if args.mode in ("stream", "both"):
        await _run_stream(args, workflow)
    return 0


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    try:
        raise SystemExit(asyncio.run(_amain()))
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
