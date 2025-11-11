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
import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests


# ----------------------------- Configuration ----------------------------- #

BASE_WAVE = "https://services.surfline.com/kbyg/spots/forecasts/wave"
BASE_TIDES = "https://services.surfline.com/kbyg/spots/forecasts/tides"
BASE_WIND = "https://services.surfline.com/kbyg/spots/forecasts/wind"
BASE_CONDITIONS = "https://services.surfline.com/kbyg/regions/forecasts/conditions"
BASE_TAXONOMY = "https://services.surfline.com/kbyg/regions/forecasts/taxonomy"
BASE_LOGIN = "https://services.surfline.com/trusted/token"

# Static client authorization string used by Surfline's app (documented publicly in surflinef).
# Ref: https://pkg.go.dev/github.com/mhelmetag/surflinef/v2 (Login payload)
DEFAULT_AUTHZ_B64 = "Basic NWM1OWU3YzNmMGI2Y2IxYWQwMmJhZjY2OnNrX1FxWEpkbjZOeTVzTVJ1MjdBbWcz"


# ------------------------------- Data Types ------------------------------- #

@dataclass
class WaveQuery:
    spot_id: str
    days: int = 6
    interval_hours: int = 1
    max_heights: bool = False
    access_token: Optional[str] = None


@dataclass
class TidesQuery:
    spot_id: str
    days: int = 6
    access_token: Optional[str] = None


@dataclass
class LoginOptions:
    username: str
    password: str
    is_short_lived: bool = False
    authz_b64: str = DEFAULT_AUTHZ_B64


# ------------------------------ Core Client ------------------------------- #

class SurflineAPI:
    """
    Minimal OOP wrapper over Surfline v2 “kbyg” endpoints.

    Typical use:
        api = SurflineAPI()
        api.login("email", "password")  # optional
        wave_json = api.get_wave(spot_id="...", days=6, interval_hours=1)
        tide_json = api.get_tides(spot_id="...", days=6)
    """

    def __init__(self, session: Optional[requests.Session] = None, timeout: int = 30) -> None:
        self.session = session or requests.Session()
        self.timeout = timeout
        self._access_token: Optional[str] = None

    # --------- Authentication --------- #
    def login(self, username: str, password: str, *, is_short_lived: bool = False,
              authz_b64: str = DEFAULT_AUTHZ_B64) -> str:
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

    # --------- Fetchers --------- #
    def get_wave(self, spot_id: str, *, days: int = 6, interval_hours: int = 1,
                 max_heights: bool = False, access_token: Optional[str] = None) -> Dict[str, Any]:
        params = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
            "maxHeights": str(max_heights).lower(),
        }
        token = access_token or self._access_token
        if token:
            params["accessToken"] = token
        r = self.session.get(BASE_WAVE, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def get_tides(self, spot_id: str, *, days: int = 6,
                  access_token: Optional[str] = None) -> Dict[str, Any]:
        params = {
            "spotId": spot_id,
            "days": days,
        }
        token = access_token or self._access_token
        if token:
            params["accessToken"] = token
        r = self.session.get(BASE_TIDES, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()
    
    def get_wind(self, spot_id: str, *, days: int = 6, interval_hours: int = 1,
             access_token: Optional[str] = None) -> Dict[str, Any]:
        params = {
            "spotId": spot_id,
            "days": days,
            "intervalHours": interval_hours,
        }
        token = access_token or self._access_token
        if token:
            params["accessToken"] = token
        r = self.session.get(BASE_WIND, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    # (Optional) Example of how you would extend:
    def get_conditions(self, subregion_id: str, *, days: int = 6,
                       access_token: Optional[str] = None) -> Dict[str, Any]:
        params = {"subregionId": subregion_id, "days": days}
        token = access_token or self._access_token
        if token:
            params["accessToken"] = token
        r = self.session.get(BASE_CONDITIONS, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    # You could also add a taxonomy helper here to discover spot/region IDs.


# ------------------------------ CSV Helpers ------------------------------- #

class WaveCSV:
    """
    Utilities for flattening wave JSON to rows and writing CSVs.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Create one row per forecast timestamp with:
          - timestamp (unix seconds, UTC-based)
          - surf_min, surf_max
          - up to 6 swell components (height_m, period_s, direction_deg)
        """
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = data.get("wave") or data.get("waveModel") or []

        for point in series:
            row: Dict[str, Any] = {
                "timestamp": point.get("timestamp"),
                "surf_min": (point.get("surf") or {}).get("min"),
                "surf_max": (point.get("surf") or {}).get("max"),
            }
            swells = point.get("swells", []) or []
            for idx, swell in enumerate(swells[:6], start=1):
                row[f"swell{idx}_height_m"] = swell.get("height")
                row[f"swell{idx}_period_s"] = swell.get("period")
                row[f"swell{idx}_direction_deg"] = swell.get("direction")
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        WaveCSV._write_dict_rows(rows, path)

    @staticmethod
    def _write_dict_rows(rows: List[Dict[str, Any]], path: str) -> None:
        if not rows:
            print(f"[warn] no rows to write for {path}", file=sys.stderr)
            return
        # union of all keys across rows to keep sparse columns
        fieldnames: List[str] = []
        seen = set()
        for r in rows:
            for k in r.keys():
                if k not in seen:
                    seen.add(k)
                    fieldnames.append(k)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"[ok] wrote {len(rows)} rows → {path}")


class TidesCSV:
    """
    Utilities for flattening tide JSON to rows and writing CSVs.
    """

    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        series = (json_obj or {}).get("data", {}).get("tides", []) or []
        for t in series:
            out.append(
                {
                    "timestamp": t.get("timestamp"),
                    "type": t.get("type"),
                    "height": t.get("height"),  # units depend on Surfline settings (often ft for US)
                }
            )
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        WaveCSV._write_dict_rows(rows, path)  # reuse the same writer


class WindCSV:
    """
    Flatten wind JSON to rows and write CSV.
    """
    @staticmethod
    def flatten(json_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        data = (json_obj or {}).get("data", {})
        series = data.get("wind") or data.get("windModel") or []

        for point in series:
            # Common fields seen in Surfline wind payloads
            row = {
                "timestamp": point.get("timestamp"),
                "speed": point.get("speed"),                 # numeric (units depend on Surfline settings)
                "direction_deg": point.get("direction"),     # meteorological degrees (0..360)
                "gust": point.get("gust"),                   # gust speed if present
            }
            # Some payloads also include textual direction or unit info
            # Keep them if available without assuming presence
            txt = point.get("directionType") or point.get("compassDirection")
            if txt is not None:
                row["direction_text"] = txt
            unit = point.get("unit") or (data.get("units") or {}).get("wind")
            if unit is not None:
                row["unit"] = unit
            out.append(row)
        return out

    @staticmethod
    def write(rows: List[Dict[str, Any]], path: str) -> None:
        WaveCSV._write_dict_rows(rows, path)  # reuse the same writer