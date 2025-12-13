import re
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------
# Configuration
# -------------------------------
JAVA_DIR = Path("./Java")
PYTHON_DIR = Path("./Python")
OUT_DIR = Path("./analysis_output")
OUT_DIR.mkdir(exist_ok=True)

PRICE_PER_GB_S = 0.0000166667
PRICE_PER_REQUEST = 0.20 / 1_000_000

# -------------------------------
# Helpers
# -------------------------------
def load_faas_runner_combined(path: Path) -> pd.DataFrame:
    """
    Safely load FaaS Runner COMBINED CSVs by skipping preamble logs.
    """
    header_idx = None
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if "uuid" in line and "," in line:
                header_idx = i
                break
    if header_idx is None:
        raise ValueError(f"CSV header not found in {path.name}")

    return pd.read_csv(
        path,
        skiprows=header_idx,
        engine="python",
        on_bad_lines="skip"
    )

def extract_dataset_size(filename: str):
    if not isinstance(filename, str):
        return np.nan
    m = re.search(r"(\d{2,6})\s*Sales", filename)
    return float(m.group(1)) if m else np.nan

def est_cost_per_million(runtime_ms, memory_mb):
    runtime_s = runtime_ms / 1000.0
    mem_gb = memory_mb / 1024.0
    return (mem_gb * runtime_s * PRICE_PER_GB_S + PRICE_PER_REQUEST) * 1_000_000

# -------------------------------
# Load all CSVs
# -------------------------------
def load_all(dir_path: Path, lang: str):
    frames = []
    for csv in dir_path.glob("*.csv"):
        df = load_faas_runner_combined(csv)
        df["lang"] = lang
        frames.append(df)
    return pd.concat(frames, ignore_index=True)

java_df = load_all(JAVA_DIR, "Java")
py_df   = load_all(PYTHON_DIR, "Python")

df = pd.concat([java_df, py_df], ignore_index=True)

# -------------------------------
# Cleaning & Feature Engineering
# -------------------------------
for col in ["runtime", "latency", "frameworkRuntime", "userRuntime",
            "functionMemory", "newcontainer"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df[df["fileName"].notna()]
df["dataset_rows"] = df["fileName"].apply(extract_dataset_size)
df = df[df["dataset_rows"].notna()]

df["runtime_ms"] = df["runtime"]
df["runtime_s"] = df["runtime_ms"] / 1000.0
df["throughput_rows_s"] = df["dataset_rows"] / df["runtime_s"]

df["is_cold_start"] = df["newcontainer"].fillna(0).astype(int)
df["cost_per_million"] = df.apply(
    lambda r: est_cost_per_million(r["runtime_ms"], r["functionMemory"]),
    axis=1
)

# -------------------------------
# Joint Summary Tables
# -------------------------------
summary = (
    df.groupby(["dataset_rows", "lang"])
      .agg(
          runs=("runtime_ms", "count"),
          runtime_mean_ms=("runtime_ms", "mean"),
          runtime_median_ms=("runtime_ms", "median"),
          runtime_p95_ms=("runtime_ms", lambda x: np.percentile(x, 95)),
          throughput_mean_rows_s=("throughput_rows_s", "mean"),
          cold_start_rate=("is_cold_start", "mean"),
          cost_per_million_usd=("cost_per_million", "mean"),
      )
      .reset_index()
      .sort_values(["dataset_rows", "lang"])
)

summary.to_csv(OUT_DIR / "joint_language_comparison_table.csv", index=False)

print("\n=== Joint Comparison Table ===")
print(summary.to_string(index=False))

# -------------------------------
# Plotting (Fixes blank images)
# -------------------------------
def safe_savefig(fig, path):
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)

# Runtime vs dataset size
fig, ax = plt.subplots()
for lang, g in summary.groupby("lang"):
    ax.plot(g["dataset_rows"], g["runtime_mean_ms"], marker="o", label=lang)

ax.set_xlabel("Dataset size (rows)")
ax.set_ylabel("Mean runtime (ms)")
ax.set_title("Mean Runtime vs Dataset Size")
ax.legend()
safe_savefig(fig, OUT_DIR / "runtime_vs_dataset.png")

# Throughput vs dataset size
fig, ax = plt.subplots()
for lang, g in summary.groupby("lang"):
    ax.plot(g["dataset_rows"], g["throughput_mean_rows_s"], marker="o", label=lang)

ax.set_xlabel("Dataset size (rows)")
ax.set_ylabel("Throughput (rows/sec)")
ax.set_title("Throughput vs Dataset Size")
ax.legend()
safe_savefig(fig, OUT_DIR / "throughput_vs_dataset.png")

# Cost vs dataset size
fig, ax = plt.subplots()
for lang, g in summary.groupby("lang"):
    ax.plot(g["dataset_rows"], g["cost_per_million_usd"], marker="o", label=lang)

ax.set_xlabel("Dataset size (rows)")
ax.set_ylabel("USD per 1M invocations (est.)")
ax.set_title("Estimated Cost vs Dataset Size")
ax.legend()
safe_savefig(fig, OUT_DIR / "cost_vs_dataset.png")

print(f"\nArtifacts written to: {OUT_DIR.resolve()}")
