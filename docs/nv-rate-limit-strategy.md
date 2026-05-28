# NV Rate-Limit Strategy (v0.6.0.14-nv-weight-fix)

Deployed: 2026-05-17

## Empirical NV Behavior

Tested directly against `integrate.api.nvidia.com`:

| Behavior | Finding |
|----------|---------|
| Per-key burst | **2 concurrent** requests before 429 |
| Recovery | ~60s per-minute window reset |
| Key independence | Different keys do not share throttle counters |
| Per-account limit | Likely 40 RPM total (not per-key) |

## Before/After

| Metric | Before | After |
|--------|--------|-------|
| 429 rate @ 180 RPM | 37% (2 min) | **0% (10 min)** |
| Total requests | 270 | 1,710 |
| Success rate | 55-63% | **76%** |
| Root cause | NewAPI self-limit 100/min | N/A — bottleneck removed |

## Fix Stack

### 1. Disabled NewAPI ModelRequestRateLimit
- `ModelRequestRateLimitEnabled=false` in DB options
- Was blocking at 100 req/min — the #1 bottleneck
- Command: `UPDATE options SET value='false' WHERE key='ModelRequestRateLimitEnabled';`

### 2. Token bucket (model/channel_cache.go)
- Per-channel: 0.667 tokens/sec, burst=1
- Each key gets at most 1 request per 1.5s (= 40 RPM)
- Prevents same-key concurrent overload matching NV burst=2

### 3. Weight system fix (model/channel_cache.go)
- `computeLatencyWeight`: channels >20s → weight=1 (was 10)
- Fast channels (<500ms) → weight=200 (preferred)
- Slow channels act as overflow only, not active participants

### 4. 429 cooldown (service/channel.go)
- 45/75/120s recovery with backoff
- Auto-re-enable channels after NV 429

### 5. Pool guard (model/channel_cache.go)
- MIN_HEALTHY_POOL_429=20
- Stops disabling channels when pool ≤ 20 healthy

## Key Insight

**More channels > fewer channels.** Deleting slow channels concentrates load on remaining keys and accelerates 429 cascades. Slow channels at weight=1 take negligible request volume but spread bursts across more NV keys.

## Debug endpoint

`GET /api/channel/token-buckets` — returns per-channel token bucket status:
```json
{"total_channels": 40, "summary": {"ready": 40, "draining": 0, "empty": 0},
 "token_buckets": [{"channel_id": 14, "tokens": 1.0, "cooldown_s": 0}, ...]}
```

## Related Skills

- newapi-operations: channel management, source deployment
- hermes-agent: gateway configuration
