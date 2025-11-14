from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# 1) Typed containers
# -----------------------------
@dataclass(frozen=True)
class SwellComponent:
    height_m: float
    period_s: float
    direction_deg: float

@dataclass(frozen=True)
class ForecastRecord:
    issue_date: pd.Timestamp            # midnight of the issue day (UTC)
    valid_time: pd.Timestamp            # forecast valid time (UTC)
    surf_min: float
    surf_max: float
    swells: List[SwellComponent]        # up to 6 components


# -----------------------------
# 2) CSV reading utilities
# -----------------------------
def _parse_issue_date_from_name(filename: str) -> pd.Timestamp:
    """
    Parse issue date from filenames like forecast_wave_YYMMDD.csv → UTC midnight timestamp.
    Example: forecast_wave_251110.csv → 2025-11-10 00:00:00+00:00
    """
    m = re.search(r"forecast_wave_(\d{6})\.csv$", filename)
    if not m:
        raise ValueError(f"Filename does not match expected pattern: {filename}")
    y, mo, d = m.group(1)[:2], m.group(1)[2:4], m.group(1)[4:6]
    year = 2000 + int(y)  # assumes 20YY
    return pd.Timestamp(year=year, month=int(mo), day=int(d), tz="UTC")


def _expected_column_names() -> List[str]:
    cols = ["timestamp", "surf_min", "surf_max"]
    # swell1..swell6, each has height_m, period_s, direction_deg
    for k in range(1, 7):
        cols += [f"swell{k}_height_m", f"swell{k}_period_s", f"swell{k}_direction_deg"]
    return cols


def read_forecast_folder(folder: str | Path) -> pd.DataFrame:
    """
    Reads all CSVs matching forecast_wave_*.csv.
    Returns a tidy DataFrame with one row per (issue_date, valid_time).
    """
    folder = Path(folder)
    files = sorted(folder.glob("forecast_wave_*.csv"))
    if not files:
        raise FileNotFoundError(f"No files found in {folder} matching 'forecast_wave_*.csv'")

    expected_cols = _expected_column_names()
    all_rows: List[pd.DataFrame] = []

    for fp in files:
        issue_date = _parse_issue_date_from_name(fp.name)

        # Try to read. The user’s sample suggests *no* header; enforce names.
        df = pd.read_csv(fp, header=None, names=expected_cols)

        # If someone *did* include a header row by accident, fix it:
        # If first row "timestamp" is a string header, drop it.
        if isinstance(df.loc[0, "timestamp"], str) and df.loc[0, "timestamp"].lower() == "timestamp":
            print("Im going here \n \n ijiafdsf")
            df = df.iloc[1:].reset_index(drop=True)
            df = df.astype({c: float for c in expected_cols if c != "timestamp"})
            df["timestamp"] = df["timestamp"].astype(np.int64)

        # Types + time
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["timestamp"]).copy()
        df["valid_time"] = pd.to_datetime(df["timestamp"].astype(np.int64), unit="s", utc=True)
        df["issue_date"] = issue_date

        # Surf midpoint (simple scalar for accuracy calc)
        df["surf_mid"] = (pd.to_numeric(df["surf_min"], errors="coerce") +
                          pd.to_numeric(df["surf_max"], errors="coerce")) / 2.0

        all_rows.append(df)

    out = pd.concat(all_rows, ignore_index=True)
    # Ensure proper dtypes for swell columns
    for k in range(1, 7):
        for fld in ("height_m", "period_s", "direction_deg"):
            col = f"swell{k}_{fld}"
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")

    # Useful helpers
    out["valid_date"] = out["valid_time"].dt.floor("D")
    out["issue_day"] = out["issue_date"].dt.date
    return out


# -----------------------------
# 3) Convert to Python objects (optional)
# -----------------------------
def to_records(df: pd.DataFrame) -> List[ForecastRecord]:
    """
    Convert the combined DF to a list of typed ForecastRecord objects.
    Handy if you want structured access elsewhere in your code.
    """
    records: List[ForecastRecord] = []
    for _, r in df.iterrows():
        swells = []
        for k in range(1, 7):
            h, p, d = r.get(f"swell{k}_height_m", np.nan), r.get(f"swell{k}_period_s", np.nan), r.get(f"swell{k}_direction_deg", np.nan)
            if not (pd.isna(h) and pd.isna(p) and pd.isna(d)):
                swells.append(SwellComponent(float(h or 0.0), float(p or 0.0), float(d or 0.0)))
        records.append(
            ForecastRecord(
                issue_date=pd.Timestamp(r["issue_date"]),
                valid_time=pd.Timestamp(r["valid_time"]),
                surf_min=float(r["surf_min"]),
                surf_max=float(r["surf_max"]),
                swells=swells,
            )
        )
    return records


# -----------------------------
# 4) “24-hour forecast” accuracy metric
# -----------------------------
def compute_24h_forecast_drift(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare day D forecast vs day D+1 forecast for the same valid_time.
    """
    base = df[["issue_date", "valid_time", "surf_mid"]].dropna().copy()

    # Day D
    left = base.copy()
    left["next_issue_date"] = left["issue_date"] + pd.Timedelta(days=1)
    left = left.rename(columns={"surf_mid": "surf_mid_D"})

    # Day D+1 (don't rename issue_date here!)
    right = base.rename(columns={"surf_mid": "surf_mid_Dplus1"})

    # Merge: match valid_time AND (issue_date + 1 day)
    merged = pd.merge(
        left,
        right,
        left_on=["valid_time", "next_issue_date"],
        right_on=["valid_time", "issue_date"],
        how="inner",
    )

    merged["error"] = merged["surf_mid_Dplus1"] - merged["surf_mid_D"]
    merged["abs_error"] = merged["error"].abs()
    merged["hour"] = merged["valid_time"].dt.hour

    return merged


def plot_accuracy_by_hour(merged: pd.DataFrame) -> None:
    """
    Plot mean absolute drift by UTC hour of valid_time.
    """
    hourly = merged.groupby("hour")["abs_error"].mean().reindex(range(24))
    plt.figure(figsize=(8, 4.5))
    plt.plot(hourly.index, hourly.values, marker="o")
    plt.title("Mean Absolute 24-Hour Forecast Drift vs. Hour (UTC)")
    plt.xlabel("Hour of Day (UTC)")
    plt.ylabel("Mean |Δ surf_mid| (ft)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_time_series_errors(merged: pd.DataFrame) -> None:
    """
    Optional: time-series plot of absolute drift over valid_time.
    """
    plt.figure(figsize=(10, 4))
    plt.plot(merged["valid_time"], merged["abs_error"], marker=".", linestyle="-")
    plt.title("24-Hour Forecast Drift Over Time")
    plt.xlabel("Valid Time (UTC)")
    plt.ylabel("|Δ surf_mid| (ft)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


# -----------------------------
# 5) Example usage
# -----------------------------
if __name__ == "__main__":
    # Point this at the folder containing forecast_wave_*.csv files
    data_folder = "/Users/colinweaver/Documents/Personal Projects/Surflie/forecasts"  # <-- change as needed

    df_all = read_forecast_folder(data_folder)
    # If you want typed Python objects:
    # records = to_records(df_all)

    # --- Extract “values for each day: what the hourly forecast is” ---
    # This is the tidy per-hour table; example: print a preview grouped by issue day.
    for day, grp in df_all.groupby("issue_day"):
        print(f"\nIssue day: {day}  (rows={len(grp)})")
        preview = grp[["valid_time", "surf_min", "surf_max", "surf_mid"]].sort_values("valid_time").head(6)
        print(preview.to_string(index=False))

    # --- Compute and plot the 24h forecast drift metric ---
    drift = compute_24h_forecast_drift(df_all)
    print(f"\nMatched pairs for 24h drift: {len(drift)}")
    if not drift.empty:
        print("Overall MAE (ft):", drift["abs_error"].mean())
        plot_accuracy_by_hour(drift)
        # Optional: time-series view
        # plot_time_series_errors(drift)
    else:
        print("No overlapping valid times found across consecutive issue days.")