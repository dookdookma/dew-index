#!/usr/bin/env python3
from __future__ import annotations
import argparse, datetime as dt, json
from pathlib import Path
from collections import Counter

BASE = Path(__file__).resolve().parents[1]
CACHE = BASE / "cache"
DAILY = CACHE / "daily"
WEEKLY = CACHE / "weekly"
MONTHLY = CACHE / "monthly"
STATE = CACHE / "state.json"


def _load_json(path: Path, default):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _parse_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def build_weekly(ref: dt.date | None = None):
    ref = ref or dt.date.today()
    year, week, _ = ref.isocalendar()
    prefix = f"{year}-W{week:02d}"

    days = []
    for p in sorted(DAILY.glob("*.json")):
        d = _parse_date(p.stem)
        y, w, _ = d.isocalendar()
        if y == year and w == week:
            days.append((d, _load_json(p, {})))

    if not days:
        return None

    source_days = [d.isoformat() for d, _ in days]
    sig_states = Counter()
    fail_modes = Counter()
    for _, row in days:
        for s in row.get("signals", []):
            sig_states[s.get("state", "unknown")] += 1
        rej = row.get("markets", {}).get("rejections", {})
        for k, v in rej.items():
            fail_modes[k] += int(v or 0)

    out = {
        "week": prefix,
        "generatedAt": dt.datetime.utcnow().isoformat() + "Z",
        "sourceDays": source_days,
        "gravityCenter": "",
        "emergentCollisions": [],
        "nextBottleneck": "",
        "signalStats": {
            "confirmedStructural": sig_states.get("confirmed_structural", 0),
            "superficialNoise": sig_states.get("superficial_noise", 0),
            "insufficientEvidence": sig_states.get("insufficient_evidence", 0)
        },
        "marketStats": {
            "passAllRate": 0.0,
            "topFailModes": [{"name": k, "count": c} for k, c in fail_modes.most_common(5)]
        }
    }
    _save_json(WEEKLY / f"{prefix}.json", out)

    state = _load_json(STATE, {"lastDaily": None, "lastWeekly": None, "lastMonthly": None, "rollupCoverage": {}})
    state["lastWeekly"] = prefix
    state.setdefault("rollupCoverage", {})[prefix] = source_days
    _save_json(STATE, state)
    return out


def build_monthly(ref: dt.date | None = None):
    ref = ref or dt.date.today()
    prefix = f"{ref.year}-{ref.month:02d}"

    weeks = []
    for p in sorted(WEEKLY.glob("*.json")):
      row = _load_json(p, {})
      if not row:
          continue
      src = row.get("sourceDays", [])
      if any(s.startswith(prefix) for s in src):
          weeks.append((p.stem, row))

    if not weeks:
        return None

    out = {
      "month": prefix,
      "generatedAt": dt.datetime.utcnow().isoformat() + "Z",
      "sourceWeeks": [w for w, _ in weeks],
      "structuralRegime": "",
      "dominantLenses": [],
      "persistentBottlenecks": [],
      "forecast": {"30d": "", "90d": "", "180d": ""}
    }
    _save_json(MONTHLY / f"{prefix}.json", out)

    state = _load_json(STATE, {"lastDaily": None, "lastWeekly": None, "lastMonthly": None, "rollupCoverage": {}})
    state["lastMonthly"] = prefix
    state.setdefault("rollupCoverage", {})[prefix] = [w for w, _ in weeks]
    _save_json(STATE, state)
    return out


def prune(ref: dt.date | None = None, daily_days=30, weekly_weeks=26, monthly_months=24):
    ref = ref or dt.date.today()
    state = _load_json(STATE, {"rollupCoverage": {}})
    coverage = state.get("rollupCoverage", {})

    # daily prune
    cutoff_daily = ref - dt.timedelta(days=daily_days)
    weekly_sources = set()
    for k, v in coverage.items():
        if k.startswith(str(ref.year)) and 'W' in k:
            weekly_sources.update(v)
        elif 'W' in k:
            weekly_sources.update(v)

    for p in DAILY.glob("*.json"):
        d = _parse_date(p.stem)
        if d < cutoff_daily and p.stem in weekly_sources:
            p.unlink(missing_ok=True)

    # weekly prune (if covered by monthly)
    cutoff_week = ref - dt.timedelta(weeks=weekly_weeks)
    monthly_sources = set()
    for k, v in coverage.items():
        if '-W' not in k:
            monthly_sources.update(v)

    for p in WEEKLY.glob("*.json"):
        stem = p.stem
        try:
            y = int(stem.split('-W')[0]); w = int(stem.split('-W')[1])
            monday = dt.date.fromisocalendar(y, w, 1)
        except Exception:
            continue
        if monday < cutoff_week and stem in monthly_sources:
            p.unlink(missing_ok=True)


def main():
    ap = argparse.ArgumentParser(description="Build DEW rollups and prune retention windows.")
    ap.add_argument("action", choices=["daily-touch", "weekly", "monthly", "prune", "all"])
    ap.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    args = ap.parse_args()

    ref = dt.date.fromisoformat(args.date) if args.date else dt.date.today()

    if args.action == "daily-touch":
        row = {
          "date": ref.isoformat(),
          "generatedAt": dt.datetime.utcnow().isoformat() + "Z",
          "inputs": {"headlines": [], "commentary": [], "timeline": []},
          "signals": [],
          "markets": {"candidatesTotal": 0, "passAllTotal": 0, "rejections": {}}
        }
        _save_json(DAILY / f"{ref.isoformat()}.json", row)
        state = _load_json(STATE, {"lastDaily": None, "lastWeekly": None, "lastMonthly": None, "rollupCoverage": {}})
        state["lastDaily"] = ref.isoformat()
        _save_json(STATE, state)
        return

    if args.action in ("weekly", "all"):
        build_weekly(ref)
    if args.action in ("monthly", "all"):
        build_monthly(ref)
    if args.action in ("prune", "all"):
        prune(ref)


if __name__ == "__main__":
    main()
