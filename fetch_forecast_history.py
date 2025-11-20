import os
import sys
import time
from datetime import datetime, timedelta

from surfline_client import (
    SurflineAPI,
    SurfCSV,
    SwellsCSV,
    RatingCSV,
    SpectraCSV,
    SunlightCSV,
    WindCSV,
    TidesCSV,
    RegionConditionsCSV,
    DEFAULT_AUTHZ_B64,
    extract_spot_name_from_surf,
    slugify_name,
)

SLEEP_BETWEEN_DAYS = 10.0  # tweak this as needed (e.g. 5, 10, 30)

"""
How to run:

- Copy paste following code snippet into command line
- adjust spot-id, subregion-id, start-date, end-date, days-ahead, interval-hours as needed

python fetch_forecast_history.py \
  --spot-id 5842041f4e65fad6a7708827 \
  --subregion-id 58581a836630e24c44878fd6 \
  --start-date 2024-11-04 \
  --end-date 2025-11-19 \
  --days-ahead 16 \
  --interval-hours 1

"""

# ---------------------- .env loader for local secrets ---------------------- #

def load_env_file(path: str = ".env") -> None:
    """
    Minimal .env loader.

    Example .env:
        SURFLINE_USER=you@example.com
        SURFLINE_PASS=supersecret
        SURFLINE_ACCESS_TOKEN=9bf7...aaac
    """
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)

# ----------------------------- Auth helpers ----------------------------- #

def ensure_logged_in_or_token(api: SurflineAPI, authz_b64: str = DEFAULT_AUTHZ_B64) -> None:
    """
    Use SURFLINE_ACCESS_TOKEN if present;
    otherwise try SURFLINE_USER/SURFLINE_PASS;
    otherwise proceed anonymous.
    """
    token = os.getenv("SURFLINE_ACCESS_TOKEN")
    if token:
        api._access_token = token
        print("[ok] using SURFLINE_ACCESS_TOKEN from environment")
        return

    user = os.getenv("SURFLINE_USER")
    pwd = os.getenv("SURFLINE_PASS")
    if user and pwd:
        from requests import HTTPError
        for short in (False, True):
            try:
                api.login(user, pwd, is_short_lived=short, authz_b64=authz_b64)
                print(f"[ok] logged in via username/password (is_short_lived={short})")
                return
            except Exception as e:
                if isinstance(e, HTTPError) and e.response is not None:
                    try:
                        print(
                            f"[login {short}] status={e.response.status_code} body={e.response.text}",
                            file=sys.stderr,
                        )
                    except Exception:
                        pass
                else:
                    print(f"[login {short}] {e}", file=sys.stderr)
        print(
            "[warn] SURFLINE_USER/PASS set but login failed; proceeding without auth",
            file=sys.stderr,
        )
        return

    print(
        "[info] No SURFLINE_ACCESS_TOKEN or SURFLINE_USER/PASS found; "
        "using anonymous access (historic forecasts may be restricted).",
        file=sys.stderr,
    )

# ----------------------------- Date defaults ----------------------------- #

def get_default_dates() -> tuple[datetime, datetime]:
    """
    Default: roughly a week of start dates from about a month ago.
    """
    today = datetime.utcnow().date()
    end = today - timedelta(days=30)
    start = end - timedelta(days=6)
    return (
        datetime.combine(start, datetime.min.time()),
        datetime.combine(end, datetime.min.time()),
    )

# ----------------------------- Main harvesting ----------------------------- #

def harvest_forecasts_for_range(
    spot_id: str,
    subregion_id: str | None,
    start_date: datetime,
    end_date: datetime,
    days_ahead: int,
    interval_hours: int = 1,
    out_root: str = "forecasts",
) -> None:
    api = SurflineAPI()
    ensure_logged_in_or_token(api)

    first_start_str = start_date.date().strftime("%Y-%m-%d")
    print(f"[info] probing spot metadata for {spot_id} at start={first_start_str}")
    try:
        probe_surf = api.get_surf(
            spot_id=spot_id,
            days=1,
            interval_hours=interval_hours,
            start=first_start_str,
        )
    except Exception as e:
        print(f"[warn] failed to probe surf metadata: {e}", file=sys.stderr)
        probe_surf = {}

    spot_name = extract_spot_name_from_surf(probe_surf, spot_id)
    spot_slug = slugify_name(spot_name)
    base_folder = os.path.join(out_root, spot_slug)
    os.makedirs(base_folder, exist_ok=True)
    print(f"[ok] writing CSVs under: {base_folder} (spot='{spot_name}')")

    cur = start_date.date()
    end = end_date.date()

    while cur <= end:
        start_str = cur.strftime("%Y-%m-%d")
        date_tag = cur.strftime("%Y%m%d")
        print(f"\n[info] fetching forecasts for start={start_str} (days_ahead={days_ahead})")

        # ---- SURF ----
        try:
            surf_json = api.get_surf(
                spot_id=spot_id,
                days=days_ahead,
                interval_hours=interval_hours,
                start=start_str,
            )
            surf_rows = SurfCSV.flatten(surf_json)
            SurfCSV.write(
                surf_rows,
                os.path.join(base_folder, f"{spot_slug}_surf_{date_tag}.csv"),
            )
        except Exception as e:
            print(f"[err] surf fetch failed for {start_str}: {e}", file=sys.stderr)

        # ---- SWELLS ----
        try:
            swells_json = api.get_swells(
                spot_id=spot_id,
                days=days_ahead,
                interval_hours=interval_hours,
                start=start_str,
            )
            swells_rows = SwellsCSV.flatten(swells_json)
            SwellsCSV.write(
                swells_rows,
                os.path.join(base_folder, f"{spot_slug}_swells_{date_tag}.csv"),
            )
        except Exception as e:
            print(f"[err] swells fetch failed for {start_str}: {e}", file=sys.stderr)

        # ---- RATING ----
        try:
            rating_json = api.get_rating(
                spot_id=spot_id,
                days=days_ahead,
                interval_hours=interval_hours,
                start=start_str,
            )
            rating_rows = RatingCSV.flatten(rating_json)
            RatingCSV.write(
                rating_rows,
                os.path.join(base_folder, f"{spot_slug}_rating_{date_tag}.csv"),
            )
        except Exception as e:
            print(f"[err] rating fetch failed for {start_str}: {e}", file=sys.stderr)

        # ---- SPECTRA ----
        try:
            spectra_json = api.get_spectra(
                spot_id=spot_id,
                days=days_ahead,
                interval_hours=interval_hours,
                start=start_str,
            )
            spectra_rows = SpectraCSV.flatten(spectra_json)
            SpectraCSV.write(
                spectra_rows,
                os.path.join(base_folder, f"{spot_slug}_spectra_{date_tag}.csv"),
            )
        except Exception as e:
            print(f"[err] spectra fetch failed for {start_str}: {e}", file=sys.stderr)

        # ---- SUNLIGHT ----
        try:
            sunlight_json = api.get_sunlight(
                spot_id=spot_id,
                days=days_ahead,
                interval_hours=interval_hours,
                start=start_str,
            )
            sunlight_rows = SunlightCSV.flatten(sunlight_json)
            SunlightCSV.write(
                sunlight_rows,
                os.path.join(base_folder, f"{spot_slug}_sunlight_{date_tag}.csv"),
            )
        except Exception as e:
            print(f"[err] sunlight fetch failed for {start_str}: {e}", file=sys.stderr)

        # ---- WIND ----
        try:
            wind_json = api.get_wind(
                spot_id=spot_id,
                days=days_ahead,
                interval_hours=interval_hours,
                start=start_str,
            )
            wind_rows = WindCSV.flatten(wind_json)
            WindCSV.write(
                wind_rows,
                os.path.join(base_folder, f"{spot_slug}_wind_{date_tag}.csv"),
            )
        except Exception as e:
            print(f"[err] wind fetch failed for {start_str}: {e}", file=sys.stderr)

        # ---- TIDES ----
        try:
            tides_json = api.get_tides(
                spot_id=spot_id,
                days=days_ahead,
                start=start_str,
            )
            tides_rows = TidesCSV.flatten(tides_json)
            TidesCSV.write(
                tides_rows,
                os.path.join(base_folder, f"{spot_slug}_tides_{date_tag}.csv"),
            )
        except Exception as e:
            print(f"[err] tides fetch failed for {start_str}: {e}", file=sys.stderr)

        # ---- REGION CONDITIONS (optional) ----
        if subregion_id:
            try:
                cond_json = api.get_region_conditions(
                    subregion_id=subregion_id,
                    days=days_ahead,
                    start=start_str,
                )
                cond_rows = RegionConditionsCSV.flatten(cond_json)
                RegionConditionsCSV.write(
                    cond_rows,
                    os.path.join(base_folder, f"{spot_slug}_conditions_{date_tag}.csv"),
                )
            except Exception as e:
                print(f"[err] conditions fetch failed for {start_str}: {e}", file=sys.stderr)

        # Small pause between each day's batch of requests to be nice to Surfline
        if SLEEP_BETWEEN_DAYS > 0:
            print(f"[info] sleeping {SLEEP_BETWEEN_DAYS:.1f}s before next day...")
            time.sleep(SLEEP_BETWEEN_DAYS)

        cur += timedelta(days=1)

# ------------------------------- CLI wrapper ------------------------------- #

def parse_args():
    import argparse

    default_start, default_end = get_default_dates()

    parser = argparse.ArgumentParser(
        description="Fetch Surfline historic forecasts (all endpoints) over a date range and save CSVs."
    )
    parser.add_argument(
        "--spot-id",
        default="5842041f4e65fad6a7708827",
        help="Surfline spotId (default: your Huntington spot).",
    )
    parser.add_argument(
        "--subregion-id",
        default=None,
        help="Optional Surfline subregionId for regional conditions "
             "(e.g. 58581a836630e24c44878fd6).",
    )
    parser.add_argument(
        "--start-date",
        default=default_start.strftime("%Y-%m-%d"),
        help=f"First start date (YYYY-MM-DD). Default: {default_start.strftime('%Y-%m-%d')} (~1 month ago).",
    )
    parser.add_argument(
        "--end-date",
        default=default_end.strftime("%Y-%m-%d"),
        help=f"Last start date (YYYY-MM-DD, inclusive). Default: {default_end.strftime('%Y-%m-%d')}.",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=7,
        help="Number of forecast days ahead for each start date (default: 7).",
    )
    parser.add_argument(
        "--interval-hours",
        type=int,
        default=1,
        help="Interval hours for surf/swells/rating/spectra/sunlight/wind (default: 1).",
    )
    parser.add_argument(
        "--out-root",
        default="forecasts",
        help='Root output folder (default: "forecasts").',
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help='Path to .env file with SURFLINE_* variables (default: ".env").',
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)

    try:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"[err] invalid date format: {e}", file=sys.stderr)
        sys.exit(2)

    if end_dt < start_dt:
        print("[err] end-date must be >= start-date", file=sys.stderr)
        sys.exit(2)

    harvest_forecasts_for_range(
        spot_id=args.spot_id,
        subregion_id=args.subregion_id,
        start_date=start_dt,
        end_date=end_dt,
        days_ahead=args.days_ahead,
        interval_hours=args.interval_hours,
        out_root=args.out_root,
    )

if __name__ == "__main__":
    main()