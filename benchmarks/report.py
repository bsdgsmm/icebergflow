"""Render a benchmark JSON report as Markdown tables and, optionally, a PNG chart.

Usage: python benchmarks/report.py [results.json] [--chart path.png]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DEFAULT_RESULTS = Path(__file__).parent / "results" / "latest.json"


def _mib(value: int | None) -> str:
    return "-" if value is None else f"{value / 1024 / 1024:.1f}"


def markdown(report: dict) -> str:
    env, params = report["environment"], report["parameters"]
    lines = [
        f"Measured {env['measured_at']} on {env['os']}, {env['logical_cpus']} logical CPUs, "
        f"Python {env['python']}, PySpark {env['pyspark']}, Iceberg {env['iceberg']}, "
        f"Java {env['java']}, `{env['spark_master']}`, driver memory {env['driver_memory']}, "
        f"{env['shuffle_partitions']} shuffle partitions. Median of {params['repeat']} runs "
        "after one warm-up.",
        "",
        f"### Table layout ({params['rows']:,} rows over {params['days']} days)",
        "",
        "| Layout | Query | Median (s) | Files scanned / total | MiB scanned |",
        "|---|---|---:|---:|---:|",
    ]
    for row in report["layout"]:
        lines.append(
            f"| {row['layout']} | {row['query']} | {row['median_seconds']:.3f} | "
            f"{row['data_files_scanned']} / {row['data_files_total']} | "
            f"{_mib(row['bytes_scanned'])} |"
        )
    lines += [
        "",
        f"### Silver processing after one new batch ({params['batches']} batches in Bronze)",
        "",
        "| Mode | Median (s) | Bronze rows read | Bronze rows total |",
        "|---|---:|---:|---:|",
    ]
    for row in report["processing"]:
        lines.append(
            f"| {row['mode']} | {row['median_seconds']:.3f} | {row['bronze_rows_read']:,} | "
            f"{row['bronze_rows_total']:,} |"
        )
    return "\n".join(lines) + "\n"


def chart(report: dict, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    layouts = list(dict.fromkeys(r["layout"] for r in report["layout"]))
    queries = list(dict.fromkeys(r["query"] for r in report["layout"]))
    by_key = {(r["layout"], r["query"]): r for r in report["layout"]}
    colors = ["#2a6f97", "#61a5c2", "#a9d6e5"]

    fig, (left, right) = plt.subplots(1, 2, figsize=(11, 4.2))
    width = 0.8 / len(layouts)
    for i, layout in enumerate(layouts):
        xs = [q + i * width for q in range(len(queries))]
        left.bar(xs, [by_key[(layout, q)]["median_seconds"] for q in queries], width,
                 label=layout, color=colors[i % len(colors)])  # fmt: skip
        right.bar(xs, [(by_key[(layout, q)]["bytes_scanned"] or 0) / 2**20 for q in queries],
                  width, label=layout, color=colors[i % len(colors)])  # fmt: skip
    for axis, title, unit in ((left, "Median query time", "seconds"),
                              (right, "Data scanned", "MiB")):  # fmt: skip
        axis.set_title(title)
        axis.set_ylabel(unit)
        axis.set_xticks([q + width for q in range(len(queries))])
        axis.set_xticklabels([q.replace("_", "\n") for q in queries], fontsize=8)
        axis.spines[["top", "right"]].set_visible(False)
    left.legend(fontsize=8, frameon=False)
    rows = report["parameters"]["rows"]
    fig.suptitle(f"Iceberg table layout benchmark ({rows:,} rows)", fontsize=11)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("results", nargs="?", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--chart", type=Path, help="write a PNG chart to this path")
    args = parser.parse_args()
    report = json.loads(args.results.read_text(encoding="utf-8"))
    sys.stdout.write(markdown(report))
    if args.chart:
        chart(report, args.chart)


if __name__ == "__main__":
    main()
