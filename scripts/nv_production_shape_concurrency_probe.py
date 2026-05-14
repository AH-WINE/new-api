#!/usr/bin/env python3
"""Production-shape NewAPI/NVIDIA v4-pro concurrency probe.

This intentionally does NOT use tiny max_tokens=1 smoke requests. It either
replays a real Hermes request dump or first asks Hermes to create one, then runs
concurrent streaming /v1/chat/completions requests with the same long-message +
tools shape.

Typical use:

  source /home/wine/.hermes/.env
  python3 scripts/nv_production_shape_concurrency_probe.py \
    --template ~/.hermes/sessions/request_dump_....json \
    --concurrency 35,70,100,200 --timeout 360

Generate a fresh production-shape template first:

  source /home/wine/.hermes/.env
  python3 scripts/nv_production_shape_concurrency_probe.py \
    --generate-template /home/wine/.hermes/tmp/nv-prod-shape-template.json \
    --dry-run

The script prints a compact per-step summary and writes JSON results for audit.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import math
import os
import pathlib
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import aiohttp

DEFAULT_BASE_URL = "http://10.0.0.13:3000/v1"
DEFAULT_MODEL = "deepseek-ai/deepseek-v4-pro"
DEFAULT_SESSIONS_DIR = pathlib.Path.home() / ".hermes" / "sessions"
DEFAULT_OUTPUT_DIR = pathlib.Path.home() / ".hermes" / "tmp"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    data = sorted(values)
    idx = max(0, min(len(data) - 1, math.ceil((pct / 100.0) * len(data)) - 1))
    return round(data[idx], 3)


def compact_float(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 3)


def endpoint_from_base_url(base_url: str) -> str:
    url = base_url.rstrip("/")
    if url.endswith("/chat/completions"):
        return url
    return url + "/chat/completions"


def load_dump_body(path: pathlib.Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and "request" in raw:
        return raw["request"]["body"]
    if isinstance(raw, dict) and "body" in raw and isinstance(raw["body"], dict):
        return raw["body"]
    if isinstance(raw, dict) and "messages" in raw:
        return raw
    raise ValueError(f"cannot find request body in template: {path}")


def save_body(body: dict[str, Any], path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at": utc_now(),
        "source": "nv_production_shape_concurrency_probe.py",
        "body": body,
        "metrics": body_metrics(body),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def body_metrics(body: dict[str, Any]) -> dict[str, Any]:
    messages = body.get("messages") or []
    tools = body.get("tools") or []
    return {
        "model": body.get("model"),
        "message_count": len(messages) if isinstance(messages, list) else None,
        "message_repr_chars": sum(len(str(m)) for m in messages) if isinstance(messages, list) else None,
        "tools_count": len(tools) if isinstance(tools, list) else 0,
        "has_stream": body.get("stream") is True,
        "has_stream_options": isinstance(body.get("stream_options"), dict),
        "max_tokens": body.get("max_tokens") or body.get("max_completion_tokens"),
        "keys": sorted(body.keys()),
    }


def newest_request_dump_after(start_ts: float, model: str | None = None) -> pathlib.Path:
    candidates: list[pathlib.Path] = []
    for p in DEFAULT_SESSIONS_DIR.glob("request_dump_*.json"):
        try:
            if p.stat().st_mtime + 0.001 < start_ts:
                continue
            if model:
                try:
                    body = load_dump_body(p)
                    if body.get("model") != model:
                        continue
                except Exception:
                    continue
            candidates.append(p)
        except OSError:
            continue
    if not candidates:
        raise FileNotFoundError(
            f"no request_dump_*.json created under {DEFAULT_SESSIONS_DIR} after {start_ts}"
        )
    return max(candidates, key=lambda p: p.stat().st_mtime)


def build_hermes_prompt(repeat: int, response_chars: int) -> str:
    base = (
        "不要调用工具。你会看到一段模拟长上下文。"
        f"请输出约{response_chars}字中文，主题是 NewAPI NVIDIA v4-pro 生产形态并发上限测试，"
        "必须包含：长上下文、65 tools、streaming、SSE、HTTP/2 over proxy、HTTP/1.1 proxy、"
        "tls bad record MAC、EOF、429、500、p95/p99、回滚标准。\n\n"
    )
    chunk = (
        "[模拟上下文] NewAPI NVIDIA v4-pro 链路观测：request_id、channel、"
        "TLS bad record MAC、EOF、proxy、stream、tools、长上下文、重试窗口、"
        "HTTP1.1代理修复、并发阶梯、CT112、PVE Mihomo。该段用于复现 Hermes "
        "实例长输入形态，不需要逐字分析。\n"
    )
    return base + chunk * repeat


def generate_template_via_hermes(args: argparse.Namespace) -> pathlib.Path:
    prompt = build_hermes_prompt(args.large_context_repeat, args.response_chars)
    env = os.environ.copy()
    env["HERMES_DUMP_REQUESTS"] = "1"
    start = time.time()
    cmd = [
        "hermes",
        "-z",
        prompt,
        "--provider",
        args.provider,
        "-m",
        args.model,
    ]
    print(
        f"[template] running Hermes to create real request dump; prompt_chars={len(prompt)} timeout={args.hermes_timeout}s",
        flush=True,
    )
    proc = subprocess.run(
        cmd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=args.hermes_timeout,
    )
    print(f"[template] hermes_exit={proc.returncode}", flush=True)
    if proc.stdout.strip():
        print("[template] hermes_stdout_head=" + proc.stdout.strip()[:500].replace("\n", "\\n"), flush=True)
    if proc.stderr.strip():
        print("[template] hermes_stderr_head=" + proc.stderr.strip()[:500].replace("\n", "\\n"), flush=True)
    dump = newest_request_dump_after(start, args.model)
    body = load_dump_body(dump)
    body = normalize_body_for_probe(body, args)
    output = pathlib.Path(args.generate_template).expanduser()
    save_body(body, output)
    print(f"[template] source_dump={dump}", flush=True)
    print(f"[template] saved={output}", flush=True)
    print("[template] metrics=" + json.dumps(body_metrics(body), ensure_ascii=False), flush=True)
    return output


def find_latest_template_for_model(model: str) -> pathlib.Path:
    candidates: list[pathlib.Path] = []
    for p in DEFAULT_SESSIONS_DIR.glob("request_dump_*.json"):
        try:
            body = load_dump_body(p)
            if body.get("model") == model:
                candidates.append(p)
        except Exception:
            continue
    if not candidates:
        raise FileNotFoundError(f"no request_dump_*.json for model={model} under {DEFAULT_SESSIONS_DIR}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def normalize_body_for_probe(body: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    normalized = copy.deepcopy(body)
    normalized["model"] = args.model
    if args.force_stream:
        normalized["stream"] = True
        normalized["stream_options"] = {"include_usage": True}
    if args.max_tokens is not None:
        normalized["max_tokens"] = args.max_tokens
        normalized.pop("max_completion_tokens", None)
    # Transport-only/debug-only keys should not be sent in raw HTTP JSON.
    normalized.pop("timeout", None)
    return normalized


def add_unique_suffix(body: dict[str, Any], index: int, batch_id: str) -> dict[str, Any]:
    req = copy.deepcopy(body)
    messages = req.get("messages")
    if not isinstance(messages, list):
        return req
    for msg in reversed(messages):
        if not isinstance(msg, dict) or msg.get("role") != "user":
            continue
        suffix = f"\n\n[probe_request_id={batch_id}-{index}]"
        content = msg.get("content")
        if isinstance(content, str):
            msg["content"] = content + suffix
            break
        if isinstance(content, list):
            msg["content"] = content + [{"type": "text", "text": suffix}]
            break
    return req


async def one_request(
    session: aiohttp.ClientSession,
    endpoint: str,
    api_key: str,
    body: dict[str, Any],
    index: int,
    batch_id: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    req_body = add_unique_suffix(body, index, batch_id) if args.unique_suffix else copy.deepcopy(body)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream" if req_body.get("stream") else "application/json",
    }
    started = time.perf_counter()
    first_byte_at: float | None = None
    status: int | None = None
    bytes_read = 0
    done_seen = False
    error = None
    body_head = bytearray()
    try:
        async with session.post(endpoint, headers=headers, json=req_body) as resp:
            status = resp.status
            response_request_id = resp.headers.get("X-Oneapi-Request-Id") or resp.headers.get("x-oneapi-request-id")
            async for chunk in resp.content.iter_chunked(args.chunk_size):
                if first_byte_at is None:
                    first_byte_at = time.perf_counter()
                bytes_read += len(chunk)
                if len(body_head) < args.error_body_bytes:
                    body_head.extend(chunk[: args.error_body_bytes - len(body_head)])
                if b"[DONE]" in chunk:
                    done_seen = True
            elapsed = time.perf_counter() - started
            ok = status == 200 and bytes_read > 0 and (done_seen if req_body.get("stream") else True)
            if status == 200 and req_body.get("stream") and not done_seen:
                error = "stream_without_done"
            elif status != 200:
                error = f"http_{status}"
            return {
                "index": index,
                "ok": ok,
                "status": status,
                "request_id": response_request_id,
                "elapsed_s": compact_float(elapsed),
                "first_byte_s": compact_float(first_byte_at - started) if first_byte_at is not None else None,
                "bytes": bytes_read,
                "done_seen": done_seen,
                "error": error,
                "body_head": body_head.decode("utf-8", errors="replace")[: args.error_body_bytes] if error else "",
            }
    except Exception as exc:  # noqa: BLE001 - probe must record all failures
        elapsed = time.perf_counter() - started
        return {
            "index": index,
            "ok": False,
            "status": status,
            "elapsed_s": compact_float(elapsed),
            "first_byte_s": compact_float(first_byte_at - started) if first_byte_at is not None else None,
            "bytes": bytes_read,
            "done_seen": done_seen,
            "error": f"{type(exc).__name__}: {exc}",
            "body_head": body_head.decode("utf-8", errors="replace")[: args.error_body_bytes],
        }


async def run_batch(concurrency: int, body: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    api_key = os.environ.get(args.api_key_env, "").strip()
    if not api_key:
        raise RuntimeError(f"missing API key env: {args.api_key_env}")
    endpoint = endpoint_from_base_url(args.base_url)
    timeout = aiohttp.ClientTimeout(
        total=args.timeout,
        connect=args.connect_timeout,
        sock_connect=args.connect_timeout,
        sock_read=args.read_timeout,
    )
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S") + f"-c{concurrency}"
    wall_start = time.perf_counter()
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = []
        for i in range(concurrency):
            if args.ramp_delay_ms > 0 and i > 0:
                await asyncio.sleep(args.ramp_delay_ms / 1000.0)
            tasks.append(asyncio.create_task(one_request(session, endpoint, api_key, body, i, batch_id, args)))
        results = await asyncio.gather(*tasks)
    wall = time.perf_counter() - wall_start
    return summarize_batch(concurrency, batch_id, results, wall)


def summarize_batch(concurrency: int, batch_id: str, results: list[dict[str, Any]], wall: float) -> dict[str, Any]:
    total = len(results)
    ok_results = [r for r in results if r.get("ok")]
    failed = [r for r in results if not r.get("ok")]
    latencies = [float(r["elapsed_s"]) for r in ok_results if r.get("elapsed_s") is not None]
    first_bytes = [float(r["first_byte_s"]) for r in ok_results if r.get("first_byte_s") is not None]
    status_counts = Counter(str(r.get("status")) for r in results)
    error_counts = Counter(str(r.get("error")) for r in failed)
    return {
        "batch_id": batch_id,
        "started_at": utc_now(),
        "concurrency": concurrency,
        "total": total,
        "ok": len(ok_results),
        "failed": len(failed),
        "ok_rate": round(len(ok_results) / total, 4) if total else 0,
        "wall_s": compact_float(wall),
        "status_counts": dict(status_counts),
        "error_counts": dict(error_counts),
        "latency_s": {
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": compact_float(max(latencies)) if latencies else None,
        },
        "first_byte_s": {
            "p50": percentile(first_bytes, 50),
            "p95": percentile(first_bytes, 95),
            "p99": percentile(first_bytes, 99),
            "max": compact_float(max(first_bytes)) if first_bytes else None,
        },
        "bytes": {
            "median": int(statistics.median([r.get("bytes", 0) for r in ok_results])) if ok_results else 0,
            "min": min([r.get("bytes", 0) for r in ok_results], default=0),
            "max": max([r.get("bytes", 0) for r in ok_results], default=0),
        },
        "failure_samples": failed[:10],
        "results": results,
    }


def parse_concurrency(value: str) -> list[int]:
    out: list[int] = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            # start-stop-step, e.g. 35-200-35
            bits = [int(x) for x in part.split("-")]
            if len(bits) != 3:
                raise argparse.ArgumentTypeError("range form must be start-stop-step")
            start, stop, step = bits
            out.extend(range(start, stop + 1, step))
        else:
            out.append(int(part))
    if not out:
        raise argparse.ArgumentTypeError("empty concurrency list")
    if any(x <= 0 for x in out):
        raise argparse.ArgumentTypeError("concurrency must be positive")
    return out


def print_batch_line(summary: dict[str, Any]) -> None:
    lat = summary["latency_s"]
    fb = summary["first_byte_s"]
    print(
        "[batch] "
        f"c={summary['concurrency']} ok={summary['ok']}/{summary['total']} "
        f"ok_rate={summary['ok_rate']:.2%} wall={summary['wall_s']}s "
        f"lat_p50/p95/p99/max={lat['p50']}/{lat['p95']}/{lat['p99']}/{lat['max']}s "
        f"fb_p50/p95/p99={fb['p50']}/{fb['p95']}/{fb['p99']}s "
        f"status={summary['status_counts']} errors={summary['error_counts']}",
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.environ.get("NEWAPI_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--model", default=os.environ.get("NEWAPI_PROBE_MODEL", DEFAULT_MODEL))
    parser.add_argument("--provider", default="newapi", help="Hermes provider used only for template generation")
    parser.add_argument("--api-key-env", default="NEW_API_API_KEY")
    parser.add_argument("--template", help="request_dump JSON or saved template JSON. Defaults to latest dump for model")
    parser.add_argument("--generate-template", help="run Hermes once and save a production-shape template body here")
    parser.add_argument("--large-context-repeat", type=int, default=360)
    parser.add_argument("--response-chars", type=int, default=1200)
    parser.add_argument("--hermes-timeout", type=int, default=360)
    parser.add_argument("--concurrency", type=parse_concurrency, default=parse_concurrency("1"))
    parser.add_argument("--timeout", type=float, default=360.0)
    parser.add_argument("--connect-timeout", type=float, default=30.0)
    parser.add_argument("--read-timeout", type=float, default=300.0)
    parser.add_argument("--chunk-size", type=int, default=8192)
    parser.add_argument("--error-body-bytes", type=int, default=2000)
    parser.add_argument("--ramp-delay-ms", type=int, default=0)
    parser.add_argument("--settle-seconds", type=float, default=20.0)
    parser.add_argument("--max-tokens", type=int, default=None, help="optional cap; omitted by default to preserve Hermes dump shape")
    parser.add_argument("--force-stream", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--unique-suffix", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", help="result JSON path; default under ~/.hermes/tmp")
    parser.add_argument("--stop-on-fail-rate", type=float, default=0.01)
    parser.add_argument("--safe-p99-seconds", type=float, default=120.0)
    args = parser.parse_args()

    if args.generate_template:
        template_path = generate_template_via_hermes(args)
    elif args.template:
        template_path = pathlib.Path(args.template).expanduser()
    else:
        template_path = find_latest_template_for_model(args.model)

    body = normalize_body_for_probe(load_dump_body(template_path), args)
    metrics = body_metrics(body)
    print(f"[probe] template={template_path}", flush=True)
    print("[probe] body_metrics=" + json.dumps(metrics, ensure_ascii=False), flush=True)
    print(f"[probe] endpoint={endpoint_from_base_url(args.base_url)}", flush=True)

    if metrics.get("tools_count", 0) < 50:
        print("[warn] tools_count < 50; this is probably not a real Hermes production-shape dump", file=sys.stderr)
    if not metrics.get("has_stream"):
        print("[warn] request is not stream=true", file=sys.stderr)
    if (metrics.get("message_repr_chars") or 0) < 50_000:
        print("[warn] message_repr_chars < 50k; increase --large-context-repeat for upper-bound testing", file=sys.stderr)

    if args.dry_run:
        return 0

    all_summaries: list[dict[str, Any]] = []
    safe_concurrency: int | None = None
    for pos, concurrency in enumerate(args.concurrency):
        summary = asyncio.run(run_batch(concurrency, body, args))
        all_summaries.append(summary)
        print_batch_line(summary)
        fail_rate = 1.0 - summary["ok_rate"]
        p99 = summary["latency_s"].get("p99")
        if fail_rate <= args.stop_on_fail_rate and (p99 is None or p99 <= args.safe_p99_seconds):
            safe_concurrency = concurrency
        if fail_rate > args.stop_on_fail_rate:
            print(
                f"[stop] fail_rate={fail_rate:.2%} exceeds threshold={args.stop_on_fail_rate:.2%}; stop further steps",
                flush=True,
            )
            break
        if pos < len(args.concurrency) - 1 and args.settle_seconds > 0:
            print(f"[settle] sleeping {args.settle_seconds}s", flush=True)
            time.sleep(args.settle_seconds)

    output = pathlib.Path(args.output).expanduser() if args.output else DEFAULT_OUTPUT_DIR / (
        "nv-prod-shape-concurrency-" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "created_at": utc_now(),
        "script": str(pathlib.Path(__file__).resolve()),
        "template": str(template_path),
        "endpoint": endpoint_from_base_url(args.base_url),
        "body_metrics": metrics,
        "thresholds": {
            "stop_on_fail_rate": args.stop_on_fail_rate,
            "safe_p99_seconds": args.safe_p99_seconds,
        },
        "safe_concurrency_by_client_metrics": safe_concurrency,
        "summaries": all_summaries,
    }
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[probe] result_json={output}", flush=True)
    print(f"[probe] safe_concurrency_by_client_metrics={safe_concurrency}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
