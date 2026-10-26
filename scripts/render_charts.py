"""Render documentation charts from the Gold tables of the local warehouse.

Run after `icebergflow run` on the default dataset:

    python scripts/render_charts.py --output docs/screenshots/daily_revenue.png
"""

from __future__ import annotations

import argparse
from pathlib import Path

from icebergflow.config import Settings
from icebergflow.lakehouse.spark import create_spark_session
from icebergflow.logging_config import configure_logging

CHANNEL_COLORS = {
    "unknown": "#b0b7bf",
    "web": "#2a6f97",
    "mobile_app": "#61a5c2",
    "marketplace": "#a9d6e5",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=Path("docs/screenshots/daily_revenue.png"))
    args = parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    spark = create_spark_session(settings, "icebergflow-charts")
    daily = spark.table("gold.daily_sales").orderBy("order_date").collect()
    by_channel = spark.sql(
        "SELECT order_date, channel, net_revenue FROM gold.channel_daily ORDER BY order_date"
    ).collect()
    spark.stop()

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    dates = [r.order_date for r in daily]
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(10, 6.5), sharex=True)

    bottoms = [0.0] * len(dates)
    index = {d: i for i, d in enumerate(dates)}
    for channel, color in CHANNEL_COLORS.items():
        values = [0.0] * len(dates)
        for row in by_channel:
            if row.channel == channel:
                values[index[row.order_date]] = float(row.net_revenue)
        top.bar(dates, values, bottom=bottoms, color=color, label=channel)
        bottoms = [b + v for b, v in zip(bottoms, values, strict=True)]
    top.set_title("Net revenue by channel (gold.channel_daily)")
    top.set_ylabel("USD")
    top.legend(frameon=False, fontsize=8, ncols=4)

    rates = [r.cancelled_orders / r.orders * 100 for r in daily]
    bottom.plot(dates, [r.net_orders for r in daily], marker="o", color="#2a6f97",
                label="net orders")  # fmt: skip
    bottom.set_ylabel("orders")
    twin = bottom.twinx()
    twin.plot(dates, rates, marker="s", color="#c8553d", label="cancellation rate (%)")
    twin.set_ylabel("cancelled %")
    bottom.set_title("Net orders and cancellation rate (gold.daily_sales)")
    lines = bottom.get_legend_handles_labels()[0] + twin.get_legend_handles_labels()[0]
    bottom.legend(lines, [h.get_label() for h in lines], frameon=False, fontsize=8)

    for axis in (top, bottom, twin):
        axis.spines[["top"]].set_visible(False)
    fig.autofmt_xdate()
    fig.tight_layout()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=130)


if __name__ == "__main__":
    main()
