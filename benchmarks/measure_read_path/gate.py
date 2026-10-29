"""Regression gate shared by the read-path tools.

A cell (one config and scenario) regresses when the candidate tree is worse
than the baseline tree by more than the allowed percentage, and by more than
twice the noise measured for either tree. The noise rule keeps a noisy cell
from failing a run on a difference it cannot resolve; the allowed percentage
is the floor below which a slowdown or growth is accepted.

The noise rule has a ceiling, --max-limit (or the allowed percentage, when
that is higher). A cell whose noise term would exceed it is inconclusive: it
is too noisy to catch a regression of that size, so it fails the gate instead
of passing unseen. run_bench gives inconclusive cells
the same extra rounds as regressed ones before the verdict.

Every tool ends with the same summary: whether the gate passed, the allowed
percentage, and for every regressed or inconclusive cell the baseline and
candidate numbers, the delta and the limit. With
--fail-on-regression the tool exits 1 when a cell regressed or is
inconclusive, after writing its results.
"""

import sys

# Default allowed regression, in percent. Timing noise on a quiet machine is
# 1-8% per cell, so 5% plus the noise rule passes run-to-run variance while
# still catching the slowdowns that matter for every command. Peak memory
# moves by a few percent between runs too, so mem_probe also measures its
# noise across passes.
DEFAULT_ALLOWED_PCT = 5.0
NOISE_FACTOR = 2.0
# Largest limit the noise rule may set, in percent. Above it a cell cannot
# catch a regression that size and is reported as inconclusive.
DEFAULT_MAX_LIMIT_PCT = 10.0


def add_gate_args(parser, default_allowed=DEFAULT_ALLOWED_PCT):
    parser.add_argument(
        "--compare",
        nargs="*",
        default=[],
        help="candidate:baseline label pairs to compare, e.g. pr:master",
    )
    parser.add_argument(
        "--allowed-regression",
        type=float,
        default=default_allowed,
        metavar="PCT",
        help=f"accepted slowdown or growth in percent (default {default_allowed})",
    )
    parser.add_argument(
        "--max-limit",
        type=float,
        default=DEFAULT_MAX_LIMIT_PCT,
        metavar="PCT",
        help="largest limit the noise rule may set; noisier cells are "
        f"inconclusive and fail the gate (default {DEFAULT_MAX_LIMIT_PCT:g})",
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="exit 1 when a compared pair regressed or is inconclusive",
    )
    parser.add_argument(
        "--summary",
        metavar="FILE",
        help="also append the summary to FILE, e.g. $GITHUB_STEP_SUMMARY",
    )


def compare_pairs(args, trees):
    pairs = []
    for pair in args.compare:
        new, old = pair.split(":", 1)
        if new not in trees or old not in trees:
            raise SystemExit(f"--compare {pair}: unknown tree label")
        pairs.append((new, old))
    return pairs


def evaluate(cells, allowed_pct, max_limit_pct=DEFAULT_MAX_LIMIT_PCT):
    """Return the cells with delta, noise, limit, regressed and inconclusive
    filled in.

    Each cell is a dict with config, scenario, old, new (lower is better),
    old_noise and new_noise (relative, 0 when unknown).
    """
    out = []
    for c in cells:
        delta = (c["new"] - c["old"]) / c["old"]
        noise = max(c.get("old_noise", 0.0), c.get("new_noise", 0.0))
        limit = max(allowed_pct / 100, NOISE_FACTOR * noise)
        regressed = delta > limit
        out.append(
            {
                **c,
                "delta": delta,
                "noise": noise,
                "limit": limit,
                "regressed": regressed,
                # only the noise term is capped: an allowed regression above
                # --max-limit is the caller's choice, not a noisy cell
                "inconclusive": not regressed
                and NOISE_FACTOR * noise > max(allowed_pct, max_limit_pct) / 100,
            }
        )
    return out


def table(rows, new, old):
    lines = [
        f"| config | scenario | {old} | {new} | delta | limit |",
        "|---|---|---|---|---|---|",
    ]
    for c in rows:
        lines.append(
            f"| {c['config']} | {c['scenario']} | {c['old']:.2f} | {c['new']:.2f} "
            f"| {c['delta'] * 100:+.1f}% | +{c['limit'] * 100:.1f}% |"
        )
    return lines


def render(title, evaluated, new, old, allowed_pct, unit, max_limit_pct=None):
    if max_limit_pct is None:
        max_limit_pct = DEFAULT_MAX_LIMIT_PCT
    regressed = sorted(
        (c for c in evaluated if c["regressed"]), key=lambda c: -c["delta"]
    )
    inconclusive = sorted(
        (c for c in evaluated if c["inconclusive"]), key=lambda c: -c["limit"]
    )
    problems = []
    if regressed:
        problems.append(f"{len(regressed)} regression(s)")
    if inconclusive:
        problems.append(f"{len(inconclusive)} inconclusive")
    status = f"**FAILED: {', '.join(problems)}**" if problems else "**passed**"
    lines = [
        f"### {title}: {new} vs {old}, {status}",
        "",
        f"Allowed regression: {allowed_pct:g}%, or {NOISE_FACTOR:g}x the measured "
        f"noise when that is larger, up to {max_limit_pct:g}%. A cell whose limit "
        f"would exceed {max_limit_pct:g}% is inconclusive. {len(evaluated)} cells "
        "compared. "
        f"Values are {unit}; lower is better.",
        "",
    ]
    if regressed:
        lines += ["Regressions:", "", *table(regressed, new, old), ""]
    if inconclusive:
        lines += [
            f"Inconclusive, too noisy to catch a {max_limit_pct:g}% regression:",
            "",
            *table(inconclusive, new, old),
            "",
        ]
    if not problems:
        # nothing failed: show the closest cells, so the margin is visible
        closest = sorted(evaluated, key=lambda c: -c["delta"])[:5]
        lines += [
            "Largest deltas (none above the limit):",
            "",
            *table(closest, new, old),
        ]
    return "\n".join(lines).rstrip(), bool(problems)


def finish(args, summaries):
    """Print and optionally append the summaries; exit 1 on a regression or
    an inconclusive cell when --fail-on-regression is set."""
    text = "\n\n".join(s for s, _ in summaries)
    print("\n" + text)
    if args.summary:
        with open(args.summary, "a") as f:
            f.write(text + "\n\n")
    if args.fail_on_regression and any(failed for _, failed in summaries):
        sys.exit(1)
