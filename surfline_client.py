"""
Surfline v2 API (unofficial) — minimal OOP client + CSV helpers.

- Anonymous use typically returns ~6 days of forecast.
- Authenticated use (login) can unlock longer ranges.
- Endpoints/shape can change; Surfline does not offer a public API.

Quick start (script mode):
    python surfline_oop.py wave --spot 5842041f4e65fad6a7708827 --days 6 --out waves.csv
    python surfline_oop.py wave --spot 5842041f4e65fad6a7708827 --days 16 --out waves.csv --login

Quick start (library):
    from surfline_oop import SurflineAPI, WaveCSV

    api = SurflineAPI()
    api.login(os.getenv("SURFLINE_USER"), os.getenv("SURFLINE_PASS"))  # optional
    wave_json = api.get_wave(spot_id="5842041f4e65fad6a7708827", days=6, interval_hours=1)
    rows = WaveCSV.flatten(wave_json)
    WaveCSV.write(rows, "waves.csv")

Notes on “historic forecasts”:
- Surfline's UI exposes historic-forecast products, but there's no public, stable bulk API.
- This client targets standard v2 forecast endpoints. Expect forward forecasts; backfill isn't guaranteed.
"""

from __future__ import annotations

import csv
import os
import sys
import time

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from requests import HTTPError

# ----------------------------- Configuration ----------------------------- #

BASE_SURF      = "https://services.surfline.com/kbyg/spots/forecasts/surf"
BASE_SWELLS    = "https://services.surfline.com/kbyg/spots/forecasts/swells"
BASE_SPECTRA   = "https://services.surfline.com/kbyg/spots/forecasts/spectra"
BASE_RATING    = "https://services.surfline.com/kbyg/spots/forecasts/rating"
BASE_SUNLIGHT  = "https://services.surfline.com/kbyg/spots/forecasts/sunlight"
BASE_WIND      = "https://services.surfline.com/kbyg/spots/forecasts/wind"
BASE_TIDES     = "https://services.surfline.com/kbyg/spots/forecasts/tides"
BASE_COND_REG  = "https://services.surfline.com/kbyg/regions/forecasts/conditions"
BASE_LOGIN     = "https://services.surfline.com/trusted/token"

# Static client authorization string used by Surfline's app (documented publicly in surflinef).
DEFAULT_AUTHZ_B64 = (
    "Basic NWM1OWU3YzNmMGI2Y2IxYWQwMmJhZjY2OnNrX1FxWEpkbjZOeTVzTVJ1MjdBbWcz"
)

# ------------------------------- Data Types ------------------------------- #

@dataclass
class LoginOptions:
    username: str
    password: str
    is_short_lived: bool = False
    authz_b64: str = DEFAULT_AUTHZ_B64

# ------------------------------ Core Client ------------------------------- #

class SurflineAPI:
    """
    Minimal OOP wrapper over Surfline v2 “kbyg” endpoints you care about.

    Provides:
      - get_surf
      - get_swells
      - get_rating
      - get_spectra
      - get_sunlight
      - get_wind
      - get_tides
      - get_region_conditions
    """

    def __init__(self, session: Optional[requests.Session] = None, timeout: int = 30) -> None:
        self.session = session or requests.Session()
        self.timeout = timeout
        self._access_token: Optional[str] = None

    # --------- Authentication --------- #

    def login(
        self,
        username: str,
        password: str,
        *,
        is_short_lived: bool = False,
        authz_b64: str = DEFAULT_AUTHZ_B64,
    ) -> str:
        """
        Log in and store the resulting access token for later requests.
        Returns the token.
        """
        token = self.get_access_token(LoginOptions(username, password, is_short_lived, authz_b64))
        self._access_token = token
        return token

    @staticmethod
    def get_access_token(opts: LoginOptions) -> str:
        params = {"isShortLived": str(opts.is_short_lived).lower()}
        payload = {
            "authorizationString": opts.authz_b64,
            "device_id": "",
            "device_type": "",
            "forced": True,
            "grant_type": "password",
            "password": opts.password,
            "username": opts.username,
        }
        r = requests.post(BASE_LOGIN, params=params, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        token = data.get("accessToken") or data.get("access_token")
        if not token:
            raise RuntimeError(f"Login response missing access token: {data}")
        return token

    def _resolve_token(self, access_token: Optional[str]) -> Optional[str]:
        return access_token or self._access_token

    def _get(
        self,
        base_url: str,
        params: Dict[str, Any],
        *,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        token = self._resolve_token(access_token)
        if token:
            params.setdefault("accessToken", token)

        max_attempts = 5
        base_sleep = 5.0  # seconds

        for attempt in range(1, max_attempts + 1):
            try:
                r = self.session.get(base_url, params=params, timeout=self.timeout)
                r.raise_for_status()
                return r.json()
            except HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status == 429:
                    # Rate limited: respect Retry-After if present, otherwise backoff
                    retry_after_hdr = e.response.headers.get("Retry-After") if e.response is not None else None
                    if retry_after_hdr and retry_after_hdr.isdigit():
                        wait = int(retry_after_hdr)
                    else:
                        wait = base_sleep * attempt  # exponential-ish backoff

                    print(
                        f"[rate-limit] 429 on {base_url} (attempt {attempt}/{max_attempts}), "
                        f"sleeping {wait:.1f}s...",
                        file=sys.stderr,
                    )
                    time.sleep(wait)
                    continue  # retry
                # If it's not a 429 or we've hit some other HTTPError, re-raise
                raise
            except Exception:
                # Non-HTTP errors: just re-raise
                raise

        raise RuntimeError(f"Too many 429 responses from {base_url}; giving up")

    # --------- Spot forecast endpoints --------- #

    def get_surf(
        self,
        spot_id: str,
        *,
        days: int = 6,
        interval_hours: int = 1,
        start: Optional[str] = None,
        units_wave_height: str = "FT",
        cache_enabled: bool = True,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
            "cacheEnabled": str(cache_enabled).lower(),
            "units[waveHeight]": units_wave_height,
        }
        if start:
            params["start"] = start
        return self._get(BASE_SURF, params, access_token=access_token)

    def get_swells(
        self,
        spot_id: str,
        *,
        days: int = 6,
        interval_hours: int = 1,
        start: Optional[str] = None,
        units_swell_height: str = "FT",
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
            "units[swellHeight]": units_swell_height,
        }
        if start:
            params["start"] = start
        return self._get(BASE_SWELLS, params, access_token=access_token)

    def get_rating(
        self,
        spot_id: str,
        *,
        days: int = 6,
        interval_hours: int = 1,
        start: Optional[str] = None,
        cache_enabled: bool = True,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
            "cacheEnabled": str(cache_enabled).lower(),
        }
        if start:
            params["start"] = start
        return self._get(BASE_RATING, params, access_token=access_token)

    def get_spectra(
        self,
        spot_id: str,
        *,
        days: int = 6,
        interval_hours: int = 1,
        start: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
        }
        if start:
            params["start"] = start
        return self._get(BASE_SPECTRA, params, access_token=access_token)

    def get_sunlight(
        self,
        spot_id: str,
        *,
        days: int = 6,
        interval_hours: int = 1,
        start: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
        }
        if start:
            params["start"] = start
        return self._get(BASE_SUNLIGHT, params, access_token=access_token)

    def get_wind(
        self,
        spot_id: str,
        *,
        days: int = 6,
        interval_hours: int = 1,
        start: Optional[str] = None,
        units_wind_speed: str = "KTS",
        corrected: bool = True,
        cache_enabled: bool = True,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
            "corrected": str(corrected).lower(),
            "cacheEnabled": str(cache_enabled).lower(),
            "units[windSpeed]": units_wind_speed,
        }
        if start:
            params["start"] = start
        return self._get(BASE_WIND, params, access_token=access_token)

    def get_tides(
        self,
        spot_id: str,
        *,
        days: int = 6,
        start: Optional[str] = None,
        units_tide_height: str = "FT",
        cache_enabled: bool = True,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "spotId": spot_id,
            "days": days,
            "cacheEnabled": str(cache_enabled).lower(),
            "units[tideHeight]": units_tide_height,
        }
        if start:
            params["start"] = start
        return self._get(BASE_TIDES, params, access_token=access_token)

    # --------- Regional conditions endpoint --------- #

    def get_region_conditions(
        self,
        subregion_id: str,
        *,
        days: int = 6,
        start: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "subregionId": subregion_id,
            "days": days,
        }
        if start:
            params["start"] = start
        return self._get(BASE_COND_REG, params, access_token=access_token)

# ------------------------------ CSV Helpers ------------------------------- #

def _write_dict_rows(rows: List[Dict[str, Any]], path: str) -> None:
    if not rows:
        print(f"[warn] no rows to write for {path}", file=sys.stderr)
        return
    fieldnames: List[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[ok] wrote {len(rows)} rows → {path}")

def _first_list_in_data(data: Dict[str, Any]) -> List[Any]:
    for v in data.values():
        if isinstance(v, list):
            return v
    return []

def _flatten_simple_fields(obj: Dict[str, Any], *, prefix: str = "") -> Dict[str, Any]:
    """
    Keep only scalar-like fields from a dict; ignore nested dicts/lists.
    Optionally add a prefix to keys.
    """
    out: Dict[str, Any] = {}
    for k, v in obj.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[f"{prefix}{k}"] = v
    return out

class SurfCSV:
    """
    Flatten /kbyg/spots/forecasts/surf payloads.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = data.get("surf") or _first_list_in_data(data)

        for point in series or []:
            row: Dict[str, Any] = {}
            # Core fields
            row["timestamp"] = point.get("timestamp")
            surf_block = point.get("surf") or {}
            if isinstance(surf_block, dict):
                row["surf_min"] = surf_block.get("min")
                row["surf_max"] = surf_block.get("max")
                row["surf_humanRelation"] = surf_block.get("humanRelation")
            # Include any other simple fields at top level
            row.update(_flatten_simple_fields(point))
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

class SwellsCSV:
    """
    Flatten /kbyg/spots/forecasts/swells payloads.

    Assumes data is shaped like:
      data.swells: [ { timestamp, swells: [ {...}, {...}, ... ] }, ... ]
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = data.get("swells") or _first_list_in_data(data)

        for point in series or []:
            ts = point.get("timestamp")
            comps = point.get("swells") or []
            if not isinstance(comps, list):
                comps = []
            for idx, swell in enumerate(comps, start=1):
                row: Dict[str, Any] = {"timestamp": ts, "component_index": idx}
                row.update(_flatten_simple_fields(swell, prefix="swell_"))
                out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

class RatingCSV:
    """
    Flatten /kbyg/spots/forecasts/rating payloads.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = (
            data.get("rating")
            or data.get("ratings")
            or _first_list_in_data(data)
        )

        for point in series or []:
            row: Dict[str, Any] = {}
            row["timestamp"] = point.get("timestamp")
            # rating might itself be a scalar or dict
            rating_val = point.get("rating")
            if isinstance(rating_val, dict):
                row.update(_flatten_simple_fields(rating_val, prefix="rating_"))
            elif rating_val is not None:
                row["rating_value"] = rating_val

            row.update(_flatten_simple_fields(point))
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

class SpectraCSV:
    """
    Flatten /kbyg/spots/forecasts/spectra payloads.

    This is highly structured data; here we make a reasonable guess:
      - One row per (timestamp, bin_index)
      - Each bin's scalar fields are prefixed with "bin_".
    """

class SpectraCSV:
    """
    Flatten /kbyg/spots/forecasts/spectra payloads.

    This is highly structured data; here we make a reasonable guess:
      - One row per (timestamp, bin_index)
      - Each bin's scalar fields are prefixed with "bin_".
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        # Ensure "data" is a dict; if it's something else (int, list, etc.), bail out
        data = (json_obj or {}).get("data", {})
        if not isinstance(data, dict):
            return out

        series = data.get("spectra") or _first_list_in_data(data)
        if not isinstance(series, list):
            return out

        for point in series:
            if not isinstance(point, dict):
                continue
            ts = point.get("timestamp")
            # Guess that spectral bins are under "bins" or "data"
            bins = point.get("bins") or point.get("data") or []
            if not isinstance(bins, list):
                continue
            for idx, b in enumerate(bins, start=1):
                if not isinstance(b, dict):
                    continue
                row: Dict[str, Any] = {
                    "timestamp": ts,
                    "bin_index": idx,
                }
                row.update(_flatten_simple_fields(b, prefix="bin_"))
                out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

class SunlightCSV:
    """
    Flatten /kbyg/spots/forecasts/sunlight payloads.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = data.get("sunlight") or _first_list_in_data(data)

        for point in series or []:
            row: Dict[str, Any] = {}
            row["timestamp"] = point.get("timestamp")
            row.update(_flatten_simple_fields(point))
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

class WindCSV:
    """
    Flatten /kbyg/spots/forecasts/wind payloads.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = data.get("wind") or data.get("windModel") or _first_list_in_data(data)

        for point in series or []:
            row: Dict[str, Any] = {
                "timestamp": point.get("timestamp"),
            }
            row.update(_flatten_simple_fields(point))
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

class TidesCSV:
    """
    Flatten /kbyg/spots/forecasts/tides payloads.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        series = (json_obj or {}).get("data", {}).get("tides", []) or []
        for t in series:
            row = {
                "timestamp": t.get("timestamp"),
                "type": t.get("type"),
                "height": t.get("height"),
            }
            row.update(_flatten_simple_fields(t))
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

class RegionConditionsCSV:
    """
    Flatten /kbyg/regions/forecasts/conditions payloads.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = data.get("conditions") or _first_list_in_data(data)

        for c in series or []:
            row: Dict[str, Any] = {
                "timestamp": c.get("timestamp"),
            }
            row.update(_flatten_simple_fields(c))
            # Common nested weather block
            weather = c.get("weather") or {}
            if isinstance(weather, dict):
                row.update(_flatten_simple_fields(weather, prefix="weather_"))
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        _write_dict_rows(rows, path)

# ---------------------- Spot name / slug helpers ---------------------- #

def extract_spot_name_from_surf(json_obj: Dict[str, Any], spot_id: str) -> str:
    if not isinstance(json_obj, dict):
        return f"spot_{spot_id}"
    associated = (json_obj.get("associated") or {})
    spot = associated.get("spot") or json_obj.get("spot") or {}
    name = None
    if isinstance(spot, dict):
        name = spot.get("name")
    if not name:
        name = associated.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return f"spot_{spot_id}"

def slugify_name(name: str) -> str:
    name = name.strip()
    out_chars = []
    prev_sep = False
    for ch in name:
        if ch.isalnum():
            out_chars.append(ch)
            prev_sep = False
        else:
            if not prev_sep:
                out_chars.append("_")
                prev_sep = True
    slug = "".join(out_chars).strip("_")
    return slug or "spot"