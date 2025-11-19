import os
import sys
import time
from datetime import datetime
from surfline_client import SurflineAPI, WaveCSV, TidesCSV, DEFAULT_AUTHZ_B64
# If WindCSV is in the same module, import it too:
from surfline_client import WindCSV  # <-- add this after you define it

date_str = datetime.now().strftime("%y%m%d")

# ---------------- SIMPLE USER CONFIG (edit these) ---------------- #
COMMAND        = "wave"                       # "wave", "tides", or "wind"
SPOT_ID        = "5842041f4e65fad6a7708827"   # <-- put your spot ID here
DAYS           = 6                            # >6 usually needs login
OUT_PATH       = f"forecasts/forecast_{COMMAND}_{date_str}.csv"
SLEEP_SECONDS  = 0.2
USE_LOGIN      = False                        # True to log in
INTERVAL_HOURS = 1                            # used for "wave" and "wind" (1..24)
MAX_HEIGHTS    = False                        # only for "wave"

# Optional: override the app client string if the default breaks
# AUTHZ_B64 = "<paste new Base64 client string here>"
AUTHZ_B64 = DEFAULT_AUTHZ_B64

def maybe_login(api: SurflineAPI) -> None:
    if not USE_LOGIN:
        return
    user = os.getenv("SURFLINE_USER")
    pwd  = os.getenv("SURFLINE_PASS")

    if not user or not pwd:
        print("[err] USE_LOGIN=True but SURFLINE_USER/SURFLINE_PASS are not set in your env", file=sys.stderr)
        sys.exit(2)

    from requests import HTTPError
    for short in (False, True):
        try:
            api.login(user, pwd, is_short_lived=short, authz_b64=AUTHZ_B64)
            print(f"[ok] logged in (is_short_lived={short})")
            return
        except Exception as e:
            if isinstance(e, HTTPError) and e.response is not None:
                try:
                    print(f"[login {short}] status={e.response.status_code} body={e.response.text}", file=sys.stderr)
                except Exception:
                    pass
            else:
                print(f"[login {short}] {e}", file=sys.stderr)

    print("[err] login failed after both attempts; check credentials and AUTHZ_B64", file=sys.stderr)
    sys.exit(2)

def main() -> None:
    api = SurflineAPI()
    maybe_login(api)

    if COMMAND == "wave":
        if not SPOT_ID:
            print("[err] SPOT_ID is required for wave", file=sys.stderr)
            sys.exit(2)
        wave_json = api.get_wave(
            spot_id=SPOT_ID,
            days=DAYS,
            interval_hours=INTERVAL_HOURS,
            max_heights=MAX_HEIGHTS,
        )
        rows = WaveCSV.flatten(wave_json)
        WaveCSV.write(rows, OUT_PATH)
        time.sleep(SLEEP_SECONDS)

    elif COMMAND == "tides":
        if not SPOT_ID:
            print("[err] SPOT_ID is required for tides", file=sys.stderr)
            sys.exit(2)
        tides_json = api.get_tides(spot_id=SPOT_ID, days=DAYS)
        rows = TidesCSV.flatten(tides_json)
        TidesCSV.write(rows, OUT_PATH)
        time.sleep(SLEEP_SECONDS)

    elif COMMAND == "wind":
        if not SPOT_ID:
            print("[err] SPOT_ID is required for wind", file=sys.stderr)
            sys.exit(2)
        wind_json = api.get_wind(
            spot_id=SPOT_ID,
            days=DAYS,
            interval_hours=INTERVAL_HOURS,
        )
        rows = WindCSV.flatten(wind_json)
        WindCSV.write(rows, OUT_PATH)
        time.sleep(SLEEP_SECONDS)

    else:
        print(f"[err] unknown COMMAND: {COMMAND}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()

