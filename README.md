# 🏄‍♂️ SurflineFetcher — Surfline Historic Forecast Harvester

Surflie is a Python utility for downloading **historic Surfline forecasts** for any surf spot, including **all major v2 API endpoints**, and saving them as organized CSV files.

The tool reconstructs *what Surfline predicted on each past day* by repeatedly calling the Surfline **historic forecast API** using the `start` parameter.

Use cases include:

- Forecast drift analysis  
- Swell model accuracy comparisons  
- Machine learning datasets for surf prediction  
- Long-term archive of Surfline forecasts  
- Spot quality and conditions modeling  

> ⚠️ This is **not** an official Surfline product. Endpoint schemas may change.

---

## Features

This tool pulls and flattens **all major Surfline v2 forecast endpoints**:

### Spot-level endpoints

| Endpoint     | Output CSV                        | Description                             |
|--------------|-----------------------------------|-----------------------------------------|
| `/surf`      | `*_surf_YYYYMMDD.csv`             | Surf height ranges, surf relation       |
| `/swells`    | `*_swells_YYYYMMDD.csv`           | Primary/secondary swell components      |
| `/rating`    | `*_rating_YYYYMMDD.csv`           | Surfline quality rating                 |
| `/spectra`   | `*_spectra_YYYYMMDD.csv`          | Wave energy frequency spectra           |
| `/sunlight`  | `*_sunlight_YYYYMMDD.csv`         | Sunrise/sunset & daylight windows       |
| `/wind`      | `*_wind_YYYYMMDD.csv`             | Wind speed, direction, and gusts        |
| `/tides`     | `*_tides_YYYYMMDD.csv`            | Tide heights and tide events            |

### Regional endpoint
| Endpoint        | Output CSV                      | Description               |
|-----------------|---------------------------------|---------------------------|
| `/conditions`   | `*_conditions_YYYYMMDD.csv`     | Weather + regional surf   |

---

## Output Folder Structure

CSV files are saved under:

forecasts/<spot_slug>/
<spot_slug>_surf_YYYYMMDD.csv
<spot_slug>_swells_YYYYMMDD.csv
<spot_slug>_rating_YYYYMMDD.csv
<spot_slug>_spectra_YYYYMMDD.csv
<spot_slug>_sunlight_YYYYMMDD.csv
<spot_slug>_wind_YYYYMMDD.csv
<spot_slug>_tides_YYYYMMDD.csv
<spot_slug>_conditions_YYYYMMDD.csv

yaml
Copy code

Each `YYYYMMDD` is the **forecast issuance date**.

---

## Installation

Clone the repo:

git clone https://github.com/WeaverC20/surflie.git
cd surflie

yaml
Copy code

Install dependencies:

pip install -r requirements.txt

yaml
Copy code

---

## Authentication (.env)

Create a `.env` file in the project root:

SURFLINE_USER=your_email@example.com
SURFLINE_PASS=your_password

OR use an access token instead:
SURFLINE_ACCESS_TOKEN=abcdef123456...
yaml
Copy code

Auth priority:

1. `SURFLINE_ACCESS_TOKEN`
2. Username + password
3. Anonymous (limited; historic forecasts usually blocked)

---

## How to Run

Basic usage:

python fetch_forecast_history.py
--spot-id <spotId>
--subregion-id <subregionId>
--start-date YYYY-MM-DD
--end-date YYYY-MM-DD
--days-ahead 16
--interval-hours 1

scss
Copy code

Example (Huntington Pier):

python fetch_forecast_history.py
--spot-id 5842041f4e65fad6a7708827
--subregion-id 58581a836630e24c44878fd6
--start-date 2023-12-01
--end-date 2024-01-01
--days-ahead 16
--interval-hours 1

yaml
Copy code

---

## Pulling 2 Years of Forecasts (today + 15 days ahead)

If today is `2025-11-18`, run:

python fetch_forecast_history.py
--spot-id 5842041f4e65fad6a7708827
--subregion-id 58581a836630e24c44878fd6
--start-date 2023-11-18
--end-date 2025-11-18
--days-ahead 16
--interval-hours 1

yaml
Copy code

This generates ~730 issuance dates × 8 endpoints × 16-day forecast horizons.

---

## Rate Limiting (HTTP 429)

Surfline will rate-limit heavy scraping.

This tool includes:

- Automatic retry for 429 responses  
- Exponential backoff  
- Defensive JSON parsing  
- Optional per-day sleep (throttle)

Adjust throttle in `fetch_forecast_history.py`:

SLEEP_BETWEEN_DAYS = 10 # seconds

yaml
Copy code

Increase to 20–30 seconds for multi-year runs.

---

## Notes

- Surfline APIs are **reverse-engineered**, not official.  
- Endpoint schemas can change without notice.  
- Parsers skip malformed items gracefully.  

---