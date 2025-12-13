import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from scipy import stats

# AWS Lambda on-demand pricing (x86), ignoring free tier:
# $0.0000166667 per GB-second and $0.20 per 1M requests
PRICE_PER_GB_S = 0.0000166667
PRICE_PER_REQUEST = 0.20 / 1_000_000


def load_faas_runner_combined(path: Path) -> pd.DataFrame:
    """
    FaaS Runner COMBINED outputs often prepend a few log lines before the true CSV header.
    We locate the first line that contains 'uuid' and read from there.
    """
    header_line_idx = None
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if "uuid" in line and "," in line:
                header_line_idx = i
                break

    if header_line_idx is None:
        raise ValueError(f"Could not find CSV header line in {path.name}")

    # Read from the header line onward; skip malformed lines
    return pd.read_csv(path, skiprows=header_line_idx, engine="python", on_bad_lines="skip")


def extract_rows_from_filename(val) -> float:
    """
    Prefer patterns like '100 Sales Records...' so we don't accidentally grab random digits.
    """
    if not isinstance(val, str):
        return np.nan
    if "Sales" not in val:
        return np.nan
    m = re.search(r"(\d{2,6})\s*Sales", val)
    return float(m.group(1)) if m else np.nan


def summarize_runtime(group: pd.DataFrame) -> pd.Series:
    x = group["runtime_ms"].dropna().to_numpy()
    if len(x) == 0:
        return pd.Series(dtype=float)

    return pd.Series(
        {
            "n": len(x),
            "runtime_mean_ms": float(np.mean(x)),
            "runtime_median_ms": float(np.median(x)),
            "runtime_std_ms": float(np.std(x, ddof=1)) if len(x) > 1 else 0.0,
            "runtime_p95_ms": float(np.percentile(x, 95)),
            "throughput_mean_rows_s": float(np.mean(group["throughput_rows_per_s"].dropna())),
            "cost_per_million_mean_usd": float(np.mean(group["est_cost_per_million_usd"].dropna())),
            "cold_start_rate": float(group["is_cold_start"].mean()),
        }
    )


def est_cost_per_million(runtime_ms: float, memory_mb: float) -> float:
    runtime_s = runtime_ms / 1000.0
    mem_gb = memory_mb / 1024.0
    return (mem_gb * runtime_s * PRICE_PER_GB_S + PRICE_PER_REQUEST) * 1_000_000


def main(java_path: Path, python_path: Path, outdir: Path):
    outdir.mkdir(parents=True, exist_ok=True)

    java_df = load_faas_runner_combined(java_path)
    py_df = load_faas_runner_combined(python_path)
    java_df["lang"] = "java"
    py_df["lang"] = "python"
    df = pd.concat([java_df, py_df], ignore_index=True, sort=False)

    # Coerce key columns
    for col in ["runtime", "latency", "frameworkRuntime", "userRuntime", "functionMemory", "newcontainer"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Keep only rows where fileName looks like a real Sales CSV
    if "fileName" not in df.columns:
        raise ValueError("Expected a 'fileName' column in the COMBINED output.")

    df["dataset_rows"] = df["fileName"].apply(extract_rows_from_filename)
    df = df[df["dataset_rows"].notna()].copy()

    # Runtime is in ms in your outputs
    df["runtime_ms"] = df["runtime"]
    df["runtime_s"] = df["runtime_ms"] / 1000.0
    df["throughput_rows_per_s"] = df["dataset_rows"] / df["runtime_s"]

    df["is_cold_start"] = df["newcontainer"].fillna(0).astype(int).astype(bool)

    # Cost estimate using functionMemory from the logs (and a normalized 512MB comparison)
    df["memory_mb"] = df["functionMemory"]
    df["est_cost_per_million_usd"] = df.apply(
        lambda r: est_cost_per_million(r["runtime_ms"], r["memory_mb"]) if pd.notna(r["memory_mb"]) else np.nan,
        axis=1,
    )
    df["est_cost_per_million_512mb"] = df["runtime_ms"].apply(lambda x: est_cost_per_million(x, 512))

    # Summary stats by dataset + lang
    stats_tbl = (
        df.groupby(["dataset_rows", "lang"], as_index=False)
        .apply(summarize_runtime)
        .reset_index(drop=True)
        .sort_values(["dataset_rows", "lang"])
    )
    stats_tbl.to_csv(outdir / "faas_runner_summary_by_dataset_and_lang.csv", index=False)

    # If you want a single “overall” comparison (across all dataset sizes in the inputs)
    java_rt = df.loc[df.lang == "java", "runtime_ms"].dropna().to_numpy()
    py_rt = df.loc[df.lang == "python", "runtime_ms"].dropna().to_numpy()

    t = stats.ttest_ind(java_rt, py_rt, equal_var=False, nan_policy="omit")
    u = stats.mannwhitneyu(java_rt, py_rt, alternative="two-sided")

    def cohens_d(a, b):
        a = np.asarray(a)
        b = np.asarray(b)
        na, nb = len(a), len(b)
        sa2 = np.var(a, ddof=1)
        sb2 = np.var(b, ddof=1)
        sp = np.sqrt(((na - 1) * sa2 + (nb - 1) * sb2) / (na + nb - 2))
        return (np.mean(a) - np.mean(b)) / sp

    d = cohens_d(java_rt, py_rt)

    overall = pd.DataFrame(
        [
            {
                "lang": "java",
                "n": len(java_rt),
                "runtime_mean_ms": float(np.mean(java_rt)),
                "runtime_p95_ms": float(np.percentile(java_rt, 95)),
            },
            {
                "lang": "python",
                "n": len(py_rt),
                "runtime_mean_ms": float(np.mean(py_rt)),
                "runtime_p95_ms": float(np.percentile(py_rt, 95)),
            },
        ]
    )
    overall["welch_t_pvalue"] = t.pvalue
    overall["mannwhitney_pvalue"] = u.pvalue
    overall["cohens_d_java_minus_python"] = d
    overall.to_csv(outdir / "faas_runner_overall_runtime_stats.csv", index=False)

    # Charts (PDF + PNGs)
    pdf_path = outdir / "faas_runner_report.pdf"
    with PdfPages(pdf_path) as pdf:
        # Runtime boxplot
        fig = plt.figure()
        plt.boxplot([java_rt, py_rt], labels=["Java", "Python"], showmeans=True)
        plt.ylabel("Runtime (ms)")
        plt.title("Runtime distribution (all included dataset sizes)")
        pdf.savefig(fig, bbox_inches="tight")
        fig.savefig(outdir / "runtime_boxplot.png", bbox_inches="tight", dpi=200)
        plt.close(fig)

        # Histogram overlay
        fig = plt.figure()
        plt.hist(java_rt, bins=30, alpha=0.6, label="Java")
        plt.hist(py_rt, bins=30, alpha=0.6, label="Python")
        plt.xlabel("Runtime (ms)")
        plt.ylabel("Count")
        plt.title("Runtime histogram")
        plt.legend()
        pdf.savefig(fig, bbox_inches="tight")
        fig.savefig(outdir / "runtime_histogram.png", bbox_inches="tight", dpi=200)
        plt.close(fig)

        # Throughput per dataset size (means)
        fig = plt.figure()
        g = df.groupby(["dataset_rows", "lang"])["throughput_rows_per_s"].mean().unstack("lang")
        g.plot(kind="bar")
        plt.ylabel("Rows / second (mean)")
        plt.title("Throughput by dataset size")
        pdf.savefig(fig, bbox_inches="tight")
        fig.savefig(outdir / "throughput_by_dataset.png", bbox_inches="tight", dpi=200)
        plt.close(fig)

        # Cost per million (actual memory)
        fig = plt.figure()
        c = df.groupby(["dataset_rows", "lang"])["est_cost_per_million_usd"].mean().unstack("lang")
        c.plot(kind="bar")
        plt.ylabel("USD per 1M invocations (estimated)")
        plt.title("Estimated cost per 1M (uses functionMemory)")
        pdf.savefig(fig, bbox_inches="tight")
        fig.savefig(outdir / "cost_per_million_by_dataset_actual.png", bbox_inches="tight", dpi=200)
        plt.close(fig)

        # Cost per million normalized to 512MB
        fig = plt.figure()
        c512 = df.groupby(["dataset_rows", "lang"])["est_cost_per_million_512mb"].mean().unstack("lang")
        c512.plot(kind="bar")
        plt.ylabel("USD per 1M invocations (estimated)")
        plt.title("Estimated cost per 1M (normalized to 512MB)")
        pdf.savefig(fig, bbox_inches="tight")
        fig.savefig(outdir / "cost_per_million_by_dataset_512mb.png", bbox_inches="tight", dpi=200)
        plt.close(fig)

    print(f"Wrote: {pdf_path}")
    print(f"Wrote: {outdir / 'faas_runner_summary_by_dataset_and_lang.csv'}")
    print(f"Wrote: {outdir / 'faas_runner_overall_runtime_stats.csv'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--java", required=True, type=Path)
    ap.add_argument("--python", required=True, type=Path)
    ap.add_argument("--outdir", default=Path("./out"), type=Path)
    args = ap.parse_args()
    main(args.java, args.python, args.outdir)
