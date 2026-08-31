#!/usr/bin/env python3
"""Numbers for the empirical evaluation, read straight out of the Testground archives.

Expects the collected run archives in ./hiddenpath-results-fix, one .tgz per
cell and seed, as run_all collects them; they are too large to ship here.
output.txt next to this file is the output over that series.
"""

import json
import math
import os
import re
import statistics
import tarfile
from collections import Counter

ROOT = os.path.dirname(os.path.abspath(__file__))

SERIES = {"fix": "hiddenpath-results-fix"}

FIX = "fix"

ARCHIVE = re.compile(r"^(vanilla|sphinx)-(hit|nohit)-f(\d+)(?:-(ind|pat))?-s(\d+)\.tgz$")
POLICY = {None: "exclusive", "ind": "independent", "pat": "per-attempt"}

LADDER_F = [0, 2, 5, 8, 12, 16, 20, 24]
HOP_NAMES = ["R1", "R2", "proxy"]
POOL = 49  # everyone but the initiator is drawable
RELAY = "/sphinx/relay/1.0.0"

HOP_MS = 100  # link latency per hop
HOPS = 6  # three forward, three back
PROBE_MS = 1000  # probe window
CELL = {"hit": "probe", "nohit": "fallback"}


def read_archive(path):
    initiator, bandwidth = None, []
    with tarfile.open(path) as tf:
        for member in tf:
            if not member.name.endswith("/result.json"):
                continue
            node = json.load(tf.extractfile(member))
            if node.get("role") == "initiator":
                initiator = node
            if node.get("bw_jobs_by_protocol"):  # killed nodes report null
                bandwidth.append(node["bw_jobs_by_protocol"])
    return initiator, bandwidth


def load_runs():
    runs = []
    for series, folder in SERIES.items():
        directory = os.path.join(ROOT, folder)
        for name in sorted(os.listdir(directory)):
            match = ARCHIVE.match(name)
            if not match:  # skips the smoke archives and the run logs
                continue
            mode, target, f, suffix, seed = match.groups()
            initiator, bandwidth = read_archive(os.path.join(directory, name))
            runs.append({
                "series": series,
                "mode": mode,
                "target": target,
                "f": int(f),
                "seed": int(seed),
                "policy": POLICY[suffix],
                "init": initiator,
                "bw": bandwidth,
            })
    return runs


def pick(runs, **want):
    return [r for r in runs if all(r[k] == v for k, v in want.items())]


def jobs_of(runs):
    return [job for r in runs for job in r["init"]["job_results"]]


def quantile(values, q):
    values = sorted(values)
    pos = (len(values) - 1) * q
    low = int(pos)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (pos - low)


def wilson(k, n, z=1.96):
    if n == 0:
        return 0.0, 0.0
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return 100 * (center - half), 100 * (center + half)


def table(header, rows):
    rows = [[str(c) for c in row] for row in rows]
    width = [max(len(c) for c in col) for col in zip(header, *rows)]
    line = "  ".join(h.rjust(w) for h, w in zip(header, width))
    print(line)
    print("-" * len(line))
    for row in rows:
        print("  ".join(c.rjust(w) for c, w in zip(row, width)))
    print()


def attempt_ok(attempt):
    return any(b["outcome"] == "ok" for b in attempt["branches"])


def delivering_attempt(job):
    for attempt in job["attempts"]:
        if attempt_ok(attempt):
            return attempt["attempt"]
    return None


def branch_slots(branch):
    return branch["forward"] + [p for path in branch["returns"] for p in path]


def structurally_intact(branch, victims):
    if any(p in victims for p in branch["forward"]):
        return False
    return not all(any(p in victims for p in path) for path in branch["returns"])


def death_site(branch, victims):
    if branch["outcome"] == "ok":
        return "ok"
    for hop, peer in zip(HOP_NAMES, branch["forward"]):
        if peer in victims:
            return hop
    if all(any(p in victims for p in path) for path in branch["returns"]):
        return "return"
    return "intact"


def victims_of(run):
    return set(run["init"].get("victim_peers") or [])


def cell_jobs(runs, mode, target, f):
    # the fix series carries the ind and pat arms as well, latency stays on the exclusive draw
    return jobs_of(pick(runs, series=FIX, mode=mode, target=target, f=f, policy="exclusive"))


def exclusive_hit(runs):
    return [r for r in runs if r["series"] == FIX and r["mode"] == "sphinx"
            and r["target"] == "hit" and r["policy"] == "exclusive"]


def succeeded(runs, mode, target, f):
    return [j for j in cell_jobs(runs, mode, target, f) if j["outcome"] == "success"]


def median_first(runs, mode, target, f):
    return quantile([j["first_ms"] for j in succeeded(runs, mode, target, f)], 0.5)


def settle_gaps(runs, target):
    gaps = []
    for job in succeeded(runs, "sphinx", target, 0):
        branches = job["attempts"][0]["branches"]
        if len(branches) == 2 and all(b["outcome"] == "ok" for b in branches):
            gaps.append(branches[1]["answered_ms"] - branches[0]["answered_ms"])
    return gaps


def report_latency(runs):
    print("1. Latency, %s series, successful jobs, ms" % FIX)
    print("f = 0")
    rows = []
    for mode, target in (("vanilla", "hit"), ("vanilla", "nohit"),
                         ("sphinx", "hit"), ("sphinx", "nohit")):
        jobs = succeeded(runs, mode, target, 0)
        first = [j["first_ms"] for j in jobs]
        if mode == "vanilla":
            # vanilla keeps asking after the first block, its settled_ms is not comparable
            settled = ["-", "-"]
        else:
            values = [j["settled_ms"] for j in jobs]
            settled = [round(quantile(values, 0.5)), round(quantile(values, 0.9))]
        if (mode, target) == ("vanilla", "nohit"):
            record = round(quantile([j["dht_first_record_ms"] for j in jobs], 0.5))
        else:
            record = "-"
        rows.append(["%s %s" % (mode, CELL[target]), len(jobs),
                     round(quantile(first, 0.5)), round(quantile(first, 0.9))]
                    + settled + [record])
    table(["cell", "n", "first p50", "first p90", "settled p50", "settled p90", "dht record p50"], rows)

    model = HOPS * HOP_MS
    delta = median_first(runs, "sphinx", "nohit", 0) - median_first(runs, "vanilla", "nohit", 0)
    print("fallback tunnel: delta %+.0f ms, model %d ms (%d hops x %d ms), residual %+.0f ms"
          % (delta, model, HOPS, HOP_MS, delta - model))
    probe = median_first(runs, "sphinx", "hit", 0)
    print("probe decomposition: %.0f - %d probe window - %d tunnel = %+.0f ms residual"
          % (probe, PROBE_MS, model, probe - PROBE_MS - model))
    parts = []
    for target in ("hit", "nohit"):
        gaps = settle_gaps(runs, target)
        parts.append("sphinx %s %.0f / %.0f / %.0f (n=%d)"
                     % (CELL[target], quantile(gaps, 0.25), quantile(gaps, 0.5),
                        quantile(gaps, 0.75), len(gaps)))
    print("arrival spread within attempt 1, branch 2 minus branch 1, p25 / p50 / p75 ms: %s\n"
          % ", ".join(parts))

    print("f = 12, vanilla. First is censored at the walk cut for the fallback, hence the dht record.")
    rows = []
    for target, key in (("hit", "first_ms"), ("nohit", "dht_first_record_ms")):
        jobs = succeeded(runs, "vanilla", target, 12)
        values = [j[key] for j in jobs]
        rows.append(["vanilla %s" % CELL[target], "first" if key == "first_ms" else "dht record",
                     len(jobs), round(quantile(values, 0.5)), round(quantile(values, 0.9))])
    table(["cell", "metric", "n", "p50", "p90"], rows)

    print("f = 12, sphinx, first_ms split by the attempt that delivered")
    rows = []
    for target in ("hit", "nohit"):
        jobs = cell_jobs(runs, "sphinx", target, 12)
        done = [j for j in jobs if j["outcome"] == "success"]
        row = ["sphinx %s" % CELL[target], "%d/%d" % (len(done), len(jobs))]
        medians = []
        for want in (1, 2):
            first = [j["first_ms"] for j in done if delivering_attempt(j) == want]
            medians.append(quantile(first, 0.5) if first else None)
            row += [len(first), round(medians[-1]) if first else "-"]
        gap = "%+.0f" % (medians[1] - medians[0]) if None not in medians else "-"
        rows.append(row + [gap])
    table(["cell", "success", "n att1", "p50 att1", "n att2", "p50 att2", "att2 - att1"], rows)


def all_alive(m, pool, dead):
    return math.comb(pool - dead, m) / math.comb(pool, m)


def returns_intact(f):
    rest = POOL - 3
    # at least one of the three return paths intact, over the six peers left to draw
    return 3 * all_alive(2, rest, f) - 3 * all_alive(4, rest, f) + all_alive(6, rest, f)


def convolve(a, b):
    out = [0] * (len(a) + len(b) - 1)
    for i, x in enumerate(a):
        for j, y in enumerate(b):
            out[i + j] += x * y
    return out


def branch_failure_counts():
    """
    counts[d] = number of dead/alive assignments on the 9 branch slots
    with exactly d dead slots for which the branch fails.

    Slots:
      0..2: forward path
      3..4, 5..6, 7..8: three two-hop return paths
    """
    counts = [0] * 10

    for mask in range(1 << 9):
        dead = [bool(mask & (1 << i)) for i in range(9)]
        d = sum(dead)

        forward_dead = any(dead[:3])
        all_returns_dead = all(
            dead[a] or dead[b]
            for a, b in ((3, 4), (5, 6), (7, 8))
        )

        if forward_dead or all_returns_dead:
            counts[d] += 1

    return counts


def exact_success(f, fail_counts, slots):
    """
    Success probability with exactly f dead peers in POOL and
    'slots' pairwise-distinct drawn peers.
    """
    failures = 0

    for d, patterns in enumerate(fail_counts):
        remaining_dead = f - d
        remaining_peers = POOL - slots

        if 0 <= remaining_dead <= remaining_peers:
            failures += patterns * math.comb(remaining_peers, remaining_dead)

    return 1 - failures / math.comb(POOL, f)


BRANCH_FAIL = branch_failure_counts()
ATTEMPT_FAIL = convolve(BRANCH_FAIL, BRANCH_FAIL)
JOB_FAIL = convolve(ATTEMPT_FAIL, ATTEMPT_FAIL)


def path_model(f):
    branch = exact_success(f, BRANCH_FAIL, 9)
    attempt = exact_success(f, ATTEMPT_FAIL, 18)
    job = exact_success(f, JOB_FAIL, 36)
    return branch, attempt, job


def crossing(points, level):
    # first f where the curve drops through level, linear between the two neighbours
    for (f0, v0), (f1, v1) in zip(points, points[1:]):
        if v0 >= level > v1:
            return f0 + (f1 - f0) * (v0 - level) / (v0 - v1)
    return None


def marks(curve):
    hits = [(level, crossing(curve, level)) for level in (99, 90, 50)]
    return ", ".join("%d %% at f=%.2f (p=%.3f)" % (level, f, f / POOL)
                     for level, f in hits if f is not None)


def success_rate(jobs):
    return 100 * sum(1 for j in jobs if j["outcome"] == "success") / len(jobs)


def ladder_stats(runs):
    fix = exclusive_hit(runs)
    stats = []
    for f in LADDER_F:
        jobs = jobs_of([r for r in fix if r["f"] == f])
        first = [b for j in jobs for b in j["attempts"][0]["branches"]]
        retried = [j for j in jobs if len(j["attempts"]) > 1 and not attempt_ok(j["attempts"][0])]
        done = sum(1 for j in jobs if j["outcome"] == "success")
        ok1 = sum(1 for j in jobs if attempt_ok(j["attempts"][0]))
        okb = sum(1 for b in first if b["outcome"] == "ok")
        stats.append({"f": f, "n": len(jobs), "success": done, "job": 100 * done / len(jobs),
                      "att1_ok": ok1, "att1": 100 * ok1 / len(jobs), "branches": len(first),
                      "branch_ok": okb, "branch": 100 * okb / len(first),
                      "started": len(retried),
                      "rescued": sum(1 for j in retried if attempt_ok(j["attempts"][1]))})
    return stats


def report_model(stats):
    print("2. Measurement against the path-geometry model, sphinx-hit, exclusive draw, fix series. "
          "%d jobs and %d attempt-1 branches per f." % (stats[0]["n"], stats[0]["branches"]))
    def row(s, k, n, model):
        lo, hi = wilson(k, n)
        # Wilson never quite reaches 100, so the check compares what the table prints
        inside = round(lo, 1) <= round(model, 1) <= round(hi, 1)
        return [s["f"], "%.3f" % (s["f"] / POOL), "%.1f" % (100 * k / n), "%.1f" % model,
                "[%.1f, %.1f]" % (lo, hi), "yes" if inside else "NO"]

    for label, ok, size, level in (("branch ok", "branch_ok", "branches", 0),
                                   ("att1", "att1_ok", "n", 1), ("job", "success", "n", 2)):
        table(["f", "p", "%s %%" % label, "model", "95% CI", "model in CI"],
              [row(s, s[ok], s[size], 100 * path_model(s["f"])[level]) for s in stats])


def report_redundancy(stats):
    print("3. What the redundancy buys, sphinx-hit, exclusive draw, fix series. Lift is job "
          "minus attempt 1, which is also the share of all jobs the retry saved.")
    rows = []
    for s in stats:
        rescue = "%.1f" % (100 * s["rescued"] / s["started"]) if s["started"] else "-"
        rows.append([s["f"], "%.1f" % s["branch"], "%.1f" % s["att1"], "%.1f" % s["job"],
                     "%+.1f" % (s["job"] - s["att1"]),
                     "%d/%d" % (s["rescued"], s["started"]), rescue])
    table(["f", "branch ok %", "att1 %", "job %", "lift pp", "att2 ok/started", "rescue %"], rows)

    for name, key in (("branch", "branch"), ("attempt 1", "att1"), ("job", "job")):
        print("%s success crosses %s" % (name, marks([(s["f"], s[key]) for s in stats])))
    print()


def site_model(f):
    a1, a2, a3 = [(POOL - f - i) / (POOL - i) for i in range(3)]
    return {"R1": f / POOL, "R2": a1 * f / (POOL - 1), "proxy": a1 * a2 * f / (POOL - 2),
            "return": a1 * a2 * a3 * (1 - returns_intact(f))}


def report_mechanism(runs):
    print("4. Mechanism check, sphinx-hit, exclusive draw, fix series, all attempts")
    sites, branches_at = Counter(), Counter()
    geometry = Counter()  # keyed by structurally intact, delivered
    forward_ok, returns_ok = 0, 0
    for run in exclusive_hit(runs):
        victims = victims_of(run)
        for job in run["init"]["job_results"]:
            for attempt in job["attempts"]:
                for branch in attempt["branches"]:
                    ok = branch["outcome"] == "ok"
                    branches_at[run["f"]] += 1
                    sites[death_site(branch, victims)] += 1
                    geometry[structurally_intact(branch, victims), ok] += 1
                    if ok and any(p in victims for p in branch["forward"]):
                        forward_ok += 1
                    if ok and all(any(p in victims for p in path) for path in branch["returns"]):
                        returns_ok += 1

    dead = sum(sites.values()) - sites["ok"]
    for label, key in (("intact", True), ("dead", False)):
        print("structurally %s branches: %d, of them delivered: %d"
              % (label, geometry[key, True] + geometry[key, False], geometry[key, True]))
    print("intact yet dead, over all dead branches: %d/%d (%.1f %%)"
          % (sites["intact"], dead, 100 * sites["intact"] / dead))
    print("delivered with a dead relay on the forward path: %d" % forward_ok)
    print("delivered with all three return paths dead: %d\n" % returns_ok)

    expected = Counter()
    for f, count in branches_at.items():
        for site, p in site_model(f).items():
            expected[site] += count * p
    total = sum(expected.values())
    # intact rides along so the shares add up even if a geometrically fine branch ever dies
    rows = [[site, sites[site], "%.1f" % (100 * sites[site] / dead),
             "%.1f" % (100 * expected[site] / total)]
            for site in ("R1", "R2", "proxy", "return", "intact")]
    table(["death site", "branches", "share %", "model %"], rows)


T95 = {25: 2.064}  # two-sided 5% t, the paired design gives one pair per f and seed


def mean_ci(values):
    t = T95[len(values)]  # a different pair count means the design changed, so let it raise
    mean = statistics.mean(values)
    half = t * statistics.stdev(values) / math.sqrt(len(values))
    return mean, mean - half, mean + half


def draw_shape(jobs):
    distinct, overlap, reused, total = [], [], 0, 0
    for job in jobs:
        drawn = []
        for attempt in job["attempts"]:
            slots = [branch_slots(b) for b in attempt["branches"]]
            for branch in slots:
                total += 1
                if len(set(branch)) < len(branch):
                    reused += 1
            drawn.append(set(sum(slots, [])))
            distinct.append(len(drawn[-1]))
        if len(drawn) > 1:
            overlap.append(len(drawn[0] & drawn[1]))
    return ["%.3f" % statistics.mean(distinct),
            "%.2f" % (statistics.mean(overlap) if overlap else 0),
            "%d/%d" % (reused, total), "%.1f" % (100 * reused / total)]


def report_draw(runs):
    print("5. Draw policy, sphinx-hit in the fix series. Success pools f >= 8, the draw shape "
          "columns use every f.")
    arms = [r for r in runs if r["series"] == FIX and r["mode"] == "sphinx" and r["target"] == "hit"]
    pool = [r for r in arms if r["f"] >= 8]
    rows = []
    for policy in ("exclusive", "per-attempt", "independent"):
        jobs = jobs_of(pick(pool, policy=policy))
        retried = [j for j in jobs if len(j["attempts"]) > 1]
        rescued = sum(1 for j in retried if attempt_ok(j["attempts"][1]))
        rows.append([policy, len(jobs), "%.1f" % success_rate(jobs),
                     "%d/%d" % (rescued, len(retried))]
                    + draw_shape(jobs_of(pick(arms, policy=policy))))
    table(["arm", "jobs", "job %", "att2 ok/started", "distinct of 18",
           "att1/att2 overlap", "repeat peer in branch", "%"], rows)

    baseline = {(r["f"], r["seed"]): success_rate(r["init"]["job_results"])
                for r in pick(pool, policy="exclusive")}
    rows = []
    for policy in ("per-attempt", "independent"):
        diffs = [success_rate(r["init"]["job_results"]) - baseline[(r["f"], r["seed"])]
                 for r in pick(pool, policy=policy)]
        mean, lo, hi = mean_ci(diffs)
        rows.append(["%s minus exclusive" % policy, len(diffs),
                     "%+.2f" % mean, "[%+.2f, %+.2f]" % (lo, hi)])
    table(["paired by f and seed", "n pairs", "mean diff pp", "95% CI pp"], rows)


def report_overhead(runs):
    print("6. Bytes per job during the job loop, fix series, mean over the five seeds. "
          "Killed nodes report nothing, so f=12 sums 38 nodes.")

    def per_job(run):
        n = len(run["init"]["job_results"])
        relay = sum(b.get(RELAY, {}).get("out", 0) for b in run["bw"])
        initiator = sum(v["in"] + v["out"] for v in run["init"]["bw_jobs_by_protocol"].values())
        network = sum(v["in"] + v["out"] for b in run["bw"] for v in b.values())
        return relay / n, initiator / n, network / n

    means = {}
    rows = []
    for f in (0, 12):
        for target in ("hit", "nohit"):
            for mode in ("vanilla", "sphinx"):
                cells = pick(runs, series=FIX, mode=mode, target=target, f=f, policy="exclusive")
                values = [per_job(r) for r in cells]
                relay, initiator, network = [statistics.mean(v[i] for v in values) for i in range(3)]
                means[(f, target, mode)] = (initiator, network)
                rows.append([f, target, mode, len(cells), round(relay), round(initiator), round(network)])
    table(["f", "target", "mode", "runs", "relay out", "initiator in+out", "network in+out"], rows)

    rows = []
    for f in (0, 12):
        for target in ("hit", "nohit"):
            sphinx, vanilla = means[(f, target, "sphinx")], means[(f, target, "vanilla")]
            rows.append([f, target, "%.2f" % (sphinx[0] / vanilla[0]), "%.2f" % (sphinx[1] / vanilla[1])])
    table(["f", "target", "initiator sphinx/vanilla", "network sphinx/vanilla"], rows)


def main():
    runs = load_runs()
    print("loaded %d runs\n" % len(runs))
    report_latency(runs)
    stats = ladder_stats(runs)
    report_model(stats)
    report_redundancy(stats)
    report_mechanism(runs)
    report_draw(runs)
    report_overhead(runs)


if __name__ == "__main__":
    main()
