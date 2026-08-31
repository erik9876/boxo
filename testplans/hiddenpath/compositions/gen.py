#!/usr/bin/env python3
"""Generate the Testground compositions for the Hidden Path evaluation.

One composition per (cell, seed). The cell table below is the single source
of truth for the measurement series; shared cells appear once and carry all
their series tags:

  - {vanilla|sphinx}-{hit|nohit}-f{0,12}   latency; hit/nohit via
    provider_blocks, vanilla-nohit-f0 doubles as calibration (cal)
  - sphinx-hit-f{0,2,5,8,12,16,20,24}      churn; f=0 and f=12 shared
    with the latency cells
  - sphinx-hit-f{...}-{pat,ind}            the same ladder under
    draw_policy=per-attempt and =independent

Usage:
  ./gen.py                                 # regenerate into ./gen
  ./gen.py --boxo-pin <version>            # set the fork pin (see README)
  ./gen.py --timeout-ms 5000               # re-pin T after calibration

The boxo pin is the module version of the pushed fork the docker:go builder
swaps in for github.com/ipfs/boxo. Without it the compositions carry the
placeholder SET-ME, which does not resolve to a module version.
"""

import argparse
import json
import pathlib

# --- Fixed evaluation parameters -------------------------------------------

N = 50
SEEDS = [1, 2, 3, 4, 5]
JOBS = 20
CHURN_F = (0, 2, 5, 8, 12, 16, 20, 24)
LATENCY_MS = 100
BANDWIDTH = 1048576  # FPA-compatible LinkShape value ("1 MiB")

# Calibrated from the cal series (vanilla-nohit-f0, 100 jobs): p99 =
# 1835 ms, T = 3.3x p99. Higher breaks the 7-minute refresh-free window
DEFAULT_TIMEOUT_MS = 6000
DEFAULT_PROXY_TIMEOUT_MS = 3000

BOXO_MODULE = "github.com/ipfs/boxo"
BOXO_FORK = "github.com/erik9876/boxo"

# The measured series were generated with
#   ./gen.py --boxo-pin v0.41.1-0.20260823211837-1a39fc4a8159


def cells():
    """The deduplicated cell table: name -> (params, series tags).

    Experiment 1 (lat): {vanilla, sphinx} x {hit, nohit} x f in {0, 12}.
    Experiment 2 (churn): sphinx-hit ladder f in {0,2,5,8,12,16,20,24};
    f=0 and f=12 share their cells with lat. vanilla-nohit-f0 doubles as
    the calibration cell. The churn ladder exists twice, once per draw
    policy, so both arms are measured under one machine state. All cells:
    k=2, m=3, l=0, retransmit on, edge_epsilon=-1 (the full-mesh testbed
    makes the edge bias a victim magnet: unconnected == dead after the
    kill window; the independent draw also refuses a live bias).
    """
    out = {}

    def add(name, series, mode, blocks, f, draw_policy="exclusive"):
        if name in out:
            known = out[name]["series"]
            known += [s for s in series if s not in known]
            return
        out[name] = {
            "series": list(series),
            "mode": mode,
            "provider_blocks": blocks,
            "f": f,
            "draw_policy": draw_policy,
        }

    for mode in ("vanilla", "sphinx"):
        for hit, blocks in (("hit", True), ("nohit", False)):
            for f in (0, 12):
                series = ["lat"]
                if mode == "vanilla" and hit == "nohit" and f == 0:
                    series = ["cal", "lat"]
                add(f"{mode}-{hit}-f{f}", series, mode, blocks, f)

    for f in CHURN_F:
        add(f"sphinx-hit-f{f}", ["churn"], "sphinx", True, f)

    # Further arms of the same ladder, identical but for the relay draw.
    # The name carries the arm because add() deduplicates on it, so a
    # same-named cell would silently keep the exclusive policy
    for suffix, policy in (("ind", "independent"), ("pat", "per-attempt")):
        for f in CHURN_F:
            add(f"sphinx-hit-f{f}-{suffix}", ["churn", "draw"], "sphinx",
                True, f, draw_policy=policy)

    return out


def composition(cell, seed, args, jobs=JOBS):
    p = [
        "[global]",
        'plan = "hiddenpath"',
        'case = "discovery"',
        'builder = "docker:go"',
        'runner = "local:docker"',
        f"total_instances = {N}",
        "",
        "  [global.build_config]",
        '  go_proxy_mode = "remote"',
        '  go_proxy_url = "https://proxy.golang.org"',
        "",
        "[[groups]]",
        'id = "nodes"',
        "",
        "  [groups.instances]",
        f"  count = {N}",
        "",
        "  [groups.build]",
        "",
        "    [[groups.build.dependencies]]",
        f'    module = "{BOXO_MODULE}"',
        f'    target = "{BOXO_FORK}"',
        f'    version = "{args.boxo_pin}"',
        "",
        "  [groups.run]",
        "",
        "    [groups.run.test_params]",
        f'    mode = "{cell["mode"]}"',
        '    l = "0"',
        '    k = "2"',
        '    m = "3"',
        f'    f = "{cell["f"]}"',
        f'    provider_blocks = "{"true" if cell["provider_blocks"] else "false"}"',
        '    edge_epsilon = "-1"',
        f'    timeout_ms = "{args.timeout_ms}"',
        f'    proxy_timeout_ms = "{args.proxy_timeout_ms}"',
        '    retransmit = "true"',
        f'    draw_policy = "{cell.get("draw_policy", "exclusive")}"',
        f'    seed = "{seed}"',
        '    mesh_quorum = "0.9"',
        f'    jobs = "{jobs}"',
        f'    latency_ms = "{LATENCY_MS}"',
        f'    bandwidth = "{BANDWIDTH}"',
        "",
    ]
    return "\n".join(p)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--boxo-pin", default="SET-ME",
                    help="module version of the pushed fork (e.g. a "
                         "pseudo-version); SET-ME does not resolve")
    ap.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS)
    ap.add_argument("--proxy-timeout-ms", type=int,
                    default=DEFAULT_PROXY_TIMEOUT_MS)
    ap.add_argument("--out", default="gen")
    args = ap.parse_args()

    here = pathlib.Path(__file__).resolve().parent
    out = here / args.out
    out.mkdir(parents=True, exist_ok=True)

    table = cells()

    for stale in out.glob("*.toml"):
        stale.unlink()

    # Execution order: the draw arms alternate per (f, seed), so the drift
    # a night-long series accumulates hits all of them the same way. Cells
    # outside the ladder keep the table order
    ladder = [(f"sphinx-hit-f{f}{arm}", seed)
              for f in CHURN_F for seed in SEEDS
              for arm in ("", "-pat", "-ind")]
    in_ladder = set(ladder)
    order = [(name, seed) for name in table for seed in SEEDS
             if (name, seed) not in in_ladder] + ladder

    runs = []
    for name, seed in order:
        run = f"{name}-s{seed}"
        (out / f"{run}.toml").write_text(composition(table[name], seed, args))
        runs.append(run)

    # Acceptance smoke: sphinx-hit, f=0, jobs=2, one seed. One per arm,
    # since the exclusive smoke never exercises the independent draw
    smoke = dict(table["sphinx-hit-f0"])
    (out / "smoke.toml").write_text(composition(smoke, 1, args, jobs=2))
    smoke_ind = dict(table["sphinx-hit-f0-ind"])
    (out / "smoke-ind.toml").write_text(composition(smoke_ind, 1, args, jobs=2))

    (out / "runs.txt").write_text("\n".join(runs) + "\n")
    (out / "cells.json").write_text(json.dumps(table, indent=2) + "\n")

    print(f"{len(table)} cells x {len(SEEDS)} seeds = {len(runs)} "
          f"compositions (+ 2 smokes) -> {out}")
    print(f"timeout_ms={args.timeout_ms} proxy_timeout_ms="
          f"{args.proxy_timeout_ms} boxo pin={args.boxo_pin}")
    if args.boxo_pin == "SET-ME":
        print("WARNING: no --boxo-pin set; these compositions cannot "
              "build until you regenerate with a real pin.")


if __name__ == "__main__":
    main()
