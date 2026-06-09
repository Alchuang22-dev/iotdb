#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import argparse
import csv
import json
import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

SCRIPT_DIR = Path(__file__).resolve().parent
IOTDB_REPO_ROOT = SCRIPT_DIR.parent
WORKSPACE_ROOT = IOTDB_REPO_ROOT.parent
DEFAULT_OUT_ROOT = SCRIPT_DIR / "locomotif-functional"
DEFAULT_DATASET_ROOT = WORKSPACE_ROOT / "datasets" / "locomotif-udf" / "small"

DEFAULT_CACHE_ROOT = Path(tempfile.gettempdir()) / "iotdb-locomotif-plot-cache"
cache_root = Path(os.environ.get("LOCOMOTIF_PLOT_CACHE_DIR", DEFAULT_CACHE_ROOT))
(cache_root / "matplotlib").mkdir(parents=True, exist_ok=True)
(cache_root / "xdg").mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(cache_root / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(cache_root / "xdg"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

MOTIF_COLORS = [
    "#dc2626",
    "#16a34a",
    "#f59e0b",
    "#7c3aed",
    "#0891b2",
    "#be123c",
    "#4d7c0f",
    "#c2410c",
]


@dataclass(frozen=True)
class PlotCase:
    name: str
    title: str
    y_label: str
    series_loader: Callable[[Path], tuple[list[int], list[float]]]
    note: str


def latest_run_dir(out_root: Path) -> Path:
    candidates = [p for p in out_root.iterdir() if p.is_dir() and (p / "summary.csv").is_file()]
    if not candidates:
        raise FileNotFoundError(f"No LoCoMotif run directory with summary.csv under {out_root}")
    return sorted(candidates)[-1]


def read_numeric_csv(path: Path, value_column: str, limit: int, row_filter=None) -> tuple[list[int], list[float]]:
    x_values: list[int] = []
    y_values: list[float] = []
    with path.open(newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row_filter is not None and not row_filter(row):
                continue
            raw_value = row[value_column]
            if raw_value == "" or raw_value == "NaN":
                continue
            y_values.append(float(raw_value))
            x_values.append(len(x_values) + 1)
            if len(y_values) >= limit:
                break
    return x_values, y_values


def load_synthetic(_: Path) -> tuple[list[int], list[float]]:
    pattern = [0, 1, 2, 1, 0, -1, -2, -1, 0, 1, 0, -1]
    values = pattern * 4
    return list(range(1, len(values) + 1)), [float(v) for v in values]


def load_tsmd_mitdb1(dataset_root: Path) -> tuple[list[int], list[float]]:
    return read_numeric_csv(dataset_root / "tsmd-real" / "mitdb1" / "mitdb1_0.csv", "data", 5000)


def load_tsmd_ppg(dataset_root: Path) -> tuple[list[int], list[float]]:
    return read_numeric_csv(dataset_root / "tsmd-real" / "ptt-ppg" / "ppg_0.csv", "data", 5000)


def load_tsmd_refit(dataset_root: Path) -> tuple[list[int], list[float]]:
    return read_numeric_csv(
        dataset_root / "tsmd-real" / "refit" / "refit_0.csv",
        "data",
        7000,
        row_filter=lambda row: int(row[""]) >= 130000,
    )


def load_pamap2(dataset_root: Path) -> tuple[list[int], list[float]]:
    columns = [
        "hand_acc_x",
        "hand_acc_y",
        "hand_acc_z",
        "chest_acc_x",
        "chest_acc_y",
        "chest_acc_z",
        "ankle_acc_x",
        "ankle_acc_y",
        "ankle_acc_z",
    ]
    x_values: list[int] = []
    y_values: list[float] = []
    with (dataset_root / "pamap2" / "subject101_activity_sample.csv").open(newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if any(row[column] in ("", "NaN") for column in columns):
                continue
            values = [float(row[column]) for column in columns]
            y_values.append(math.sqrt(sum(value * value for value in values)))
            x_values.append(len(x_values) + 1)
            if len(y_values) >= 3000:
                break
    return x_values, y_values


def load_refit_house1(dataset_root: Path) -> tuple[list[int], list[float]]:
    return read_numeric_csv(dataset_root / "refit" / "CLEAN_House1_head_20000.csv", "Aggregate", 5000)


CASES = [
    PlotCase(
        name="synthetic_pair",
        title="synthetic_pair_locomotif",
        y_label="s1",
        series_loader=load_synthetic,
        note="合成双变量重复片段，图中展示 s1 通道。",
    ),
    PlotCase(
        name="tsmd_mitdb1_ecg",
        title="tsmd_mitdb1_ecg_locomotif",
        y_label="ECG value",
        series_loader=load_tsmd_mitdb1,
        note="TSMD mitdb1_0 ECG 单变量序列。",
    ),
    PlotCase(
        name="tsmd_ppg",
        title="tsmd_ppg_locomotif",
        y_label="PPG value",
        series_loader=load_tsmd_ppg,
        note="TSMD ppg_0 PPG 单变量序列。",
    ),
    PlotCase(
        name="pamap2_multivariate",
        title="pamap2_multivariate_locomotif",
        y_label="9D acceleration norm",
        series_loader=load_pamap2,
        note="PAMAP2 9 维 IMU 加速度输入，图中展示 9D L2 norm。",
    ),
    PlotCase(
        name="tsmd_refit_load",
        title="tsmd_refit_load_locomotif",
        y_label="Load value",
        series_loader=load_tsmd_refit,
        note="TSMD REFIT refit_0 负载序列切片。",
    ),
    PlotCase(
        name="refit_house1_load",
        title="refit_house1_load_locomotif",
        y_label="Aggregate power",
        series_loader=load_refit_house1,
        note="REFIT House1 多变量输入，图中展示 Aggregate 通道。",
    ),
]


def extract_json_objects(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    objects: list[dict] = []
    position = 0
    while True:
        start = text.find('{"motifId"', position)
        if start < 0:
            break
        try:
            parsed, end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            position = start + 1
            continue
        objects.append(parsed)
        position = start + end
    return objects


def read_motif_results(run_dir: Path, case_name: str) -> list[dict]:
    log_file = run_dir / "results" / f"{case_name}.log"
    if not log_file.is_file():
        return []
    return extract_json_objects(log_file.read_text())


def motif_intervals(motifs: list[dict]) -> list[tuple[int, int, int]]:
    intervals: list[tuple[int, int, int]] = []
    for motif in motifs:
        motif_id = int(motif["motifId"])
        for member in motif.get("members", []):
            begin = int(member["beginTime"])
            end = int(member["endTime"])
            if end >= begin:
                intervals.append((begin, end, motif_id))
    return intervals


def motif_color(motif_id: int) -> str:
    return MOTIF_COLORS[motif_id % len(MOTIF_COLORS)]


def plot_case(case: PlotCase, run_dir: Path, dataset_root: Path, figure_dir: Path) -> Path:
    x_values, y_values = case.series_loader(dataset_root)
    motifs = read_motif_results(run_dir, case.name)
    intervals = motif_intervals(motifs)

    fig, ax = plt.subplots(figsize=(16, 9), dpi=150)
    ax.plot(x_values, y_values, color="blue", linewidth=2.0, label="Normal Values")

    for begin, end, motif_id in intervals:
        start_index = max(0, begin - 1)
        end_index = min(len(x_values), end)
        if start_index >= end_index:
            continue
        ax.plot(
            x_values[start_index:end_index],
            y_values[start_index:end_index],
            color=motif_color(motif_id),
            linewidth=2.6,
        )

    for motif in motifs:
        representative = motif.get("representative")
        if representative:
            begin = int(representative["beginTime"])
            end = int(representative["endTime"])
            ax.axvspan(begin, end, color=motif_color(int(motif["motifId"])), alpha=0.12, linewidth=0)

    ax.set_title(case.title, fontsize=26, pad=12)
    ax.set_xlabel("Time", fontsize=20)
    ax.set_ylabel(case.y_label, fontsize=20)
    ax.tick_params(axis="both", labelsize=16)
    ax.grid(True, color="#d4d4d4", linewidth=0.8, alpha=0.55)

    legend_handles = [
        Line2D([0], [0], color="blue", lw=3, label="Normal Values"),
    ]
    legend_handles.extend(
        Line2D(
            [0],
            [0],
            color=motif_color(int(motif["motifId"])),
            lw=3,
            label=f"Motif {motif['motifId']} (fitness={float(motif['fitness']):.3f}, n={len(motif.get('members', []))})",
        )
        for motif in motifs
    )
    ax.legend(handles=legend_handles, loc="best", fontsize=18, frameon=True)

    if motifs:
        summary_text = ", ".join(
            f"id {motif['motifId']}: fitness={float(motif['fitness']):.3f}, members={len(motif.get('members', []))}"
            for motif in motifs
        )
        ax.text(
            0.01,
            0.02,
            summary_text,
            transform=ax.transAxes,
            fontsize=12,
            color="#111827",
            bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#d1d5db", "alpha": 0.88},
        )

    fig.tight_layout()
    output = figure_dir / f"{case.name}.png"
    fig.savefig(output)
    plt.close(fig)
    return output


def parse_seconds(raw_value: str) -> float:
    raw_value = raw_value.strip()
    if raw_value.endswith("s"):
        raw_value = raw_value[:-1]
    try:
        return float(raw_value)
    except ValueError:
        return 0.0


def read_summary(run_dir: Path) -> list[dict]:
    with (run_dir / "summary.csv").open(newline="") as file:
        return list(csv.DictReader(file))


def plot_summary(run_dir: Path, figure_dir: Path) -> Path:
    rows = read_summary(run_dir)
    names = [row["name"] for row in rows]
    motif_rows = [int(row["motif_rows"]) for row in rows]
    costs = [parse_seconds(row["cli_cost"]) for row in rows]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), dpi=150)
    axes[0].barh(names, motif_rows, color="#2563eb")
    axes[0].set_xlabel("Returned motif rows", fontsize=15)
    axes[0].set_title("LoCoMotif result count", fontsize=20)

    axes[1].barh(names, costs, color="#dc2626")
    axes[1].set_xlabel("CLI cost (s)", fontsize=15)
    axes[1].set_title("Query runtime", fontsize=20)

    for ax in axes:
        ax.tick_params(axis="both", labelsize=12)
        ax.grid(True, axis="x", color="#d4d4d4", linewidth=0.8, alpha=0.55)
        ax.invert_yaxis()

    fig.suptitle("LoCoMotif UDF Functional Experiment Summary", fontsize=24)
    fig.tight_layout()
    output = figure_dir / "locomotif_functional_summary.png"
    fig.savefig(output)
    plt.close(fig)
    return output


def write_figure_report(run_dir: Path, figure_paths: list[Path], dataset_root: Path) -> Path:
    report = run_dir / "locomotif_functional_figures.md"
    relative_paths = [path.relative_to(run_dir) for path in figure_paths]
    notes = {case.name: case.note for case in CASES}

    with report.open("w") as file:
        file.write("# LoCoMotif UDF 功能实验图示\n\n")
        file.write(f"- 实验目录: `{run_dir}`\n")
        file.write(f"- 数据目录: `{dataset_root}`\n")
        file.write("- 蓝色曲线表示原始序列，不同颜色曲线表示不同 `motifId` 的 member 区间。\n")
        file.write("- 半透明背景表示对应颜色 motif 的 representative 区间。\n\n")
        file.write("## 汇总\n\n")
        file.write(f"![LoCoMotif functional summary]({relative_paths[-1]})\n\n")
        file.write("## 分数据集结果\n\n")
        for case, path in zip(CASES, relative_paths):
            file.write(f"### `{case.name}`\n\n")
            file.write(f"{notes[case.name]}\n\n")
            file.write(f"![{case.name}]({path})\n\n")

    latest_report = SCRIPT_DIR / "locomotif_functional_figures.md"
    latest_report.write_text(report.read_text())
    return report


def update_main_report(run_dir: Path, figure_paths: list[Path]) -> None:
    main_report = run_dir / "report.md"
    if not main_report.is_file():
        return

    relative_paths = [path.relative_to(run_dir) for path in figure_paths]
    section_lines = [
        "",
        "## 实验图示",
        "",
        "- 蓝色曲线表示原始序列，不同颜色曲线表示不同 `motifId` 的 member 区间。",
        "- 半透明背景表示对应颜色 motif 的 representative 区间。",
        "- 完整图示索引: `locomotif_functional_figures.md`",
        "",
        "![LoCoMotif functional summary](figures/locomotif_functional_summary.png)",
        "",
    ]
    for case, path in zip(CASES, relative_paths):
        section_lines.extend(
            [
                f"### `{case.name}`",
                "",
                f"![{case.name}]({path})",
                "",
            ]
        )

    marker = "\n## 实验图示\n"
    report_text = main_report.read_text()
    base_text = report_text.split(marker, 1)[0].rstrip()
    main_report.write_text(base_text + "\n" + "\n".join(section_lines))
    (SCRIPT_DIR / "locomotif_functional_report.md").write_text(main_report.read_text())


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot LoCoMotif UDF functional experiment results.")
    parser.add_argument("--run-dir", type=Path, default=None, help="LoCoMotif functional run directory.")
    parser.add_argument("--dataset-root", type=Path, default=DEFAULT_DATASET_ROOT, help="Local small dataset root.")
    args = parser.parse_args()

    run_dir = args.run_dir.resolve() if args.run_dir else latest_run_dir(DEFAULT_OUT_ROOT).resolve()
    dataset_root = args.dataset_root.resolve()
    figure_dir = run_dir / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)

    figure_paths = [plot_case(case, run_dir, dataset_root, figure_dir) for case in CASES]
    figure_paths.append(plot_summary(run_dir, figure_dir))
    report = write_figure_report(run_dir, figure_paths, dataset_root)
    update_main_report(run_dir, figure_paths)
    print(f"Wrote figures to {figure_dir}")
    print(f"Wrote figure report to {report}")


if __name__ == "__main__":
    main()
