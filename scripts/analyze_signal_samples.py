#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import median
from typing import Any


SIDES = {
    "long": {
        "label": "Long Var / Short Lighter",
        "adjusted": "long_adjusted_pct",
        "current": "long_current_pct",
    },
    "short": {
        "label": "Short Var / Long Lighter",
        "adjusted": "short_adjusted_pct",
        "current": "short_current_pct",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze signal_samples.jsonl and suggest entry/exit threshold candidates."
    )
    parser.add_argument(
        "--file",
        default="log/signal_samples.jsonl",
        help="Path to signal sample JSONL. Default: log/signal_samples.jsonl",
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        default=24.0,
        help="Only use samples from the last N hours. Default: 24",
    )
    parser.add_argument(
        "--side",
        choices=["both", "long", "short"],
        default="both",
        help="Which side to analyze. Default: both",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=300,
        help="Warn when fewer than this many samples are available. Default: 300",
    )
    return parser.parse_args()


def parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def percentile(values: list[Decimal], pct: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / Decimal("100")) * Decimal(len(ordered) - 1)
    lower_index = int(rank)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = rank - Decimal(lower_index)
    return ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction


def fmt_pct(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{value:.6f}%"


def load_rows(path: Path, window_hours: float) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = parse_time(row.get("logged_at"))
            if ts is None:
                continue
            if ts >= cutoff:
                rows.append(row)
    return rows


def analyze_side(rows: list[dict[str, Any]], side: str, min_samples: int) -> None:
    meta = SIDES[side]
    adjusted_values: list[Decimal] = []
    current_values: list[Decimal] = []

    for row in rows:
        adjusted = parse_decimal(row.get(meta["adjusted"]))
        current = parse_decimal(row.get(meta["current"]))
        if adjusted is not None:
            adjusted_values.append(adjusted)
        if current is not None:
            current_values.append(current)

    print()
    print(f"Side: {meta['label']}")
    print(f"Usable samples: {len(adjusted_values)}")
    if len(adjusted_values) < min_samples:
        print(f"Warning: fewer than {min_samples} samples. Treat suggestions as rough.")

    if not adjusted_values:
        print("No usable adjusted samples.")
        return

    p50 = percentile(adjusted_values, Decimal("50"))
    p75 = percentile(adjusted_values, Decimal("75"))
    p90 = percentile(adjusted_values, Decimal("90"))
    p95 = percentile(adjusted_values, Decimal("95"))
    p99 = percentile(adjusted_values, Decimal("99"))
    current_median = Decimal(str(median(current_values))) if current_values else None

    print("Adjusted pct = current cross spread pct - average venue book-spread pct")
    print(f"Current pct median:  {fmt_pct(current_median)}")
    print(f"Adjusted p50:        {fmt_pct(p50)}")
    print(f"Adjusted p75:        {fmt_pct(p75)}")
    print(f"Adjusted p90:        {fmt_pct(p90)}")
    print(f"Adjusted p95:        {fmt_pct(p95)}")
    print(f"Adjusted p99:        {fmt_pct(p99)}")

    suggestions = [
        ("aggressive", p90, p50),
        ("balanced", p95, p50),
        ("conservative", p99, p50),
    ]
    print("Suggested threshold candidates:")
    for name, entry, exit_value in suggestions:
        hit_count = sum(1 for value in adjusted_values if entry is not None and value >= entry)
        hit_pct = Decimal(hit_count) / Decimal(len(adjusted_values)) * Decimal("100")
        print(
            f"  {name}: entry >= {fmt_pct(entry)}, exit <= {fmt_pct(exit_value)} "
            f"(entry samples {hit_count}/{len(adjusted_values)} = {hit_pct:.2f}%)"
        )


def main() -> int:
    args = parse_args()
    path = Path(args.file)
    if not path.exists():
        raise SystemExit(f"Sample file not found: {path}")

    rows = load_rows(path, args.window_hours)
    print(f"File: {path}")
    print(f"Window: last {args.window_hours:g} hours")
    print(f"Rows in window: {len(rows)}")

    sides = ["long", "short"] if args.side == "both" else [args.side]
    for side in sides:
        analyze_side(rows, side, args.min_samples)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
