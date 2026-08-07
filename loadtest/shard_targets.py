#!/usr/bin/env python3
"""Generate cooked ports + s2s health URLs for N local compose shards.

Port map matches scripts/run-shard.sh (default stride 10):
  shard 0 → cooked 39998, health 8081
  shard 1 → cooked 40008, health 8091
  …

Usage:
  python shard_targets.py --shards 4
  eval "$(python shard_targets.py --shards 4 --export)"
  python -m loadtest run -s S1 --eps 20000 --cooked-ports "$COOKED_PORTS" \\
    --s2s-health-urls "$S2S_HEALTH_URLS"
"""

from __future__ import annotations

import argparse


DEFAULT_COOKED_BASE = 39998
DEFAULT_HEALTH_BASE = 8081
DEFAULT_STRIDE = 10


def cooked_ports(
    shards: int,
    *,
    base: int = DEFAULT_COOKED_BASE,
    stride: int = DEFAULT_STRIDE,
) -> list[int]:
    if shards < 1:
        raise ValueError("shards must be >= 1")
    return [base + i * stride for i in range(shards)]


def health_urls(
    shards: int,
    *,
    host: str = "127.0.0.1",
    base: int = DEFAULT_HEALTH_BASE,
    stride: int = DEFAULT_STRIDE,
) -> list[str]:
    if shards < 1:
        raise ValueError("shards must be >= 1")
    return [f"http://{host}:{base + i * stride}/health" for i in range(shards)]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--shards", type=int, required=True, help="Number of writer shards")
    p.add_argument("--host", default="127.0.0.1", help="Health URL host (default 127.0.0.1)")
    p.add_argument("--cooked-base", type=int, default=DEFAULT_COOKED_BASE)
    p.add_argument("--health-base", type=int, default=DEFAULT_HEALTH_BASE)
    p.add_argument("--stride", type=int, default=DEFAULT_STRIDE)
    p.add_argument(
        "--export",
        action="store_true",
        help="Print shell exports (COOKED_PORTS, S2S_HEALTH_URLS, SHARD_COUNT)",
    )
    args = p.parse_args()

    ports = cooked_ports(
        args.shards, base=args.cooked_base, stride=args.stride
    )
    urls = health_urls(
        args.shards, host=args.host, base=args.health_base, stride=args.stride
    )
    ports_csv = ",".join(str(x) for x in ports)
    urls_csv = ",".join(urls)

    if args.export:
        print(f'export SHARD_COUNT={args.shards}')
        print(f'export COOKED_PORTS="{ports_csv}"')
        print(f'export S2S_HEALTH_URLS="{urls_csv}"')
        return

    print(f"shards={args.shards}")
    print(f"cooked_ports={ports_csv}")
    print(f"s2s_health_urls={urls_csv}")
    print(
        f"# example: python -m loadtest run -s S1 --eps {args.shards * 5000} "
        f'--cooked-ports "{ports_csv}" --s2s-health-urls "{urls_csv}"'
    )


if __name__ == "__main__":
    main()
