# Train Tracker

A live UK train status + reliability dashboard, built to eventually run on a small
always-on display showing the trains you care about and how likely they
are to actually be on time, letting you can decide which one to catch.

It pulls real-time train data from National Rail's Darwin Kafka push port and
the Rail Data Marketplae weekly/daily CIF schedule from an S3 bucket, keeps a Postgres database
of schedules + live running data + computed reliability stats, and serves it neatly over
a small HTTP API.

Handing over to Claude for the rest of the readme. **Do not run without checking**. This will be updated once I have checked over everything.
## Contents

- [Architecture](#architecture)
- [Project layout](#project-layout)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [AWS credentials (S3 schedule feed)](#aws-credentials-s3-schedule-feed)
- [Darwin Kafka credentials](#darwin-kafka-credentials)
- [Running it](#running-it)
- [API reference](#api-reference)
- [Managing your watchlist](#managing-your-watchlist)
- [Airflow DAGs](#airflow-dags)
- [Reliability stats](#reliability-stats)
- [Database schema](#database-schema)
- [Troubleshooting](#troubleshooting)
- [Known limitations / provisional decisions](#known-limitations--provisional-decisions)

## Architecture

Three independent pieces, all reading/writing the same Postgres database. None of
them share in-process state with each other — that's deliberate, so any one can be
restarted without taking the others down.

```
                                    ┌─────────────────────────┐
   S3 (weekly + daily CIF) ───────▶ │   Airflow (Docker)      │
                                    │   nightly_schedule_download │──▶ schedules, schedule_locations, tiplocs
                                    │   nightly_stats          │──▶ daily_stats
                                    └─────────────────────────┘
                                                                            ▲
   Darwin Kafka (real-time) ─────▶ live ingest (main.py) ──────────────────┤  live_journeys,
                                    always-on, native process               │  live_journey_events
                                                                            │
                                    Postgres (Docker) ◀─────────────────────┘
                                                │
                                                ▼
                                    API service (services/api.py)
                                    always-on, native process
                                                │
                                                ▼
                                        GET /status  (→ eventually, an ESP32)
```

- **Live ingest** (`main.py`) — an always-on process that consumes the Darwin Kafka
  feed, filters it down to whatever's on your watchlist, and persists observed
  stop times into Postgres. Refreshes its watchlist from the database every couple
  of minutes, so changes made via the API take effect without a restart.
- **API service** (`services/api.py`, FastAPI) — an always-on process serving live
  status + watchlist management over HTTP. Reads Postgres directly; has no shared
  memory with the ingest service.
- **Airflow** (Docker) — batch-only. `nightly_schedule_download` pulls the weekly
  full CIF schedule and daily incremental update from S3 and upserts them into
  Postgres; `nightly_stats` computes reliability metrics from the previous day's
  observed running data. Airflow never manages the two always-on services above.

Postgres and Airflow run in Docker (`docker-compose.yml`); the live ingest and API
services run natively via `uv run` (they're not containerized yet — see
[Running it](#running-it)).

## Project layout

```
main.py                          Live ingest entrypoint (always-on)
services/
  api.py                         FastAPI service (always-on)
  database.py                    All Postgres access - schema + queries
  models.py                      Pydantic/dataclass models (CIF records, Darwin messages, Watchlist)
  observation.py                 Kafka message parsing + the Observer consume loop
  load_data.py                   CIF file parser (used by both main.py's schema-init path and the DAG)
  s3_downloader.py                S3 download/extract helpers
dags/
  nightly_schedule_download.py   Fetches + upserts the weekly/daily CIF schedule
  nightly_stats.py               Computes reliability stats from the previous day
stats/
  __init__.py                    Metric auto-discovery + runner (see Reliability stats)
  _util.py                       Shared time-parsing helpers
  on_time_percentage.py          Metric: % of departures within 5 min of schedule
  average_delay.py               Metric: average departure delay in minutes
docker-compose.yml                Postgres + Airflow
docker/postgres-init/             Creates the separate `airflow` metadata database on first boot
```

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (Python 3.14, per `pyproject.toml`)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (for Postgres + Airflow)
- A Darwin Kafka push port subscription (username/password, bootstrap servers, topic)
- An AWS account with read access to an S3 bucket containing your CIF schedule files

## Setup

1. **Install dependencies**

   ```
   uv sync
   ```

2. **Copy the env file and fill it in**

   ```
   cp .env.example .env
   ```

   Fill in the Kafka (`CONSUMER_*`, `KAFKA_*`, `JSON_TOPIC`) and AWS/S3 sections -
   see [AWS credentials](#aws-credentials-s3-schedule-feed) and
   [Darwin Kafka credentials](#darwin-kafka-credentials) below. The `POSTGRES_*`
   and `DB_PATH` defaults already match `docker-compose.yml` and don't need
   changing unless you want different credentials.

3. **Start Postgres + Airflow**

   ```
   docker compose up -d postgres airflow
   ```

   First boot installs a few extra Python packages into the Airflow image
   (`_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yml`), so give it a minute.
   Check it's healthy:

   ```
   docker compose ps
   ```

   > **Port conflict?** If you have a native PostgreSQL installed on Windows, it
   > may already own port 5432 and shadow Docker's own port mapping. This repo's
   > `docker-compose.yml` already avoids that by publishing Postgres on **5433**
   > instead - if you changed that mapping, update `DB_PATH` in `.env` to match.

4. **Initialise the database schema**

   ```
   uv run python -c "from dotenv import load_dotenv; load_dotenv(); import os; from services.database import Database; Database(os.getenv('DB_PATH')).initialise_schema()"
   ```

   (`main.py` and the Airflow DAGs also call `initialise_schema()` on startup, so
   this step is really just a sanity check that `DB_PATH` in `.env` is correct.)

5. **Airflow login**

   Airflow runs in `standalone` mode and creates an admin user on first boot -
   the generated password is printed in its logs:

   ```
   docker compose logs airflow | grep -A2 "Password for user 'admin'"
   ```

   Then open [http://localhost:8080](http://localhost:8080).

## AWS credentials (S3 schedule feed)

The Airflow DAGs need read access to the S3 bucket holding your CIF schedule
files (a weekly full extract + a daily incremental update, per National Rail's
usual distribution).

**1. Get an access key.** In the AWS Console: **IAM → Users → your user → Security
credentials → Create access key**. You need a key with at least
`s3:GetObject` and `s3:HeadObject` on the bucket/prefix your schedule files live
under (`s3:ListBucket` too if you want to browse the bucket).

**2. Put the credentials in `.env`:**

```
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
AWS_REGION=eu-west-2                # match your bucket's region
S3_BUCKET_NAME=your-bucket-name
S3_SCHEDULE_PREFIX=/                # prefix within the bucket, if any
S3_WEEKLY_SCHEDULE=timetable_full.zip
S3_DAILY_UPDATE=timetable_update.zip
```

**3. Test the connection** before relying on the DAG to tell you it's broken.
This checks both that the credentials are valid *and* that the two configured
object keys actually exist and are readable:

```
uv run python -c "
from dotenv import load_dotenv
load_dotenv()
import os, boto3
from botocore.exceptions import ClientError

client = boto3.client('s3', region_name=os.environ.get('AWS_REGION'))
bucket = os.environ['S3_BUCKET_NAME']

for key_env in ('S3_WEEKLY_SCHEDULE', 'S3_DAILY_UPDATE'):
    key = os.environ[key_env]
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        print(f'{key_env}={key}: OK, size={head[\"ContentLength\"]:,} bytes, last_modified={head[\"LastModified\"]}')
    except ClientError as e:
        print(f'{key_env}={key}: FAILED - {e}')
"
```

Reading the error tells you what to fix:

| Error | Likely cause |
| --- | --- |
| `SignatureDoesNotMatch` | The secret key itself is wrong - mistyped, truncated on copy-paste, or rotated since you saved it. Re-copy from IAM or generate a new key pair. |
| `403 Forbidden` (clean, no signature error) | The key is valid but lacks permission on that bucket/key - check the IAM policy attached to the user. |
| `NoSuchKey` / `404` | The key name (`S3_WEEKLY_SCHEDULE`/`S3_DAILY_UPDATE`) doesn't match what's actually in the bucket - check for typos or a different prefix. |
| `NoSuchBucket` | `S3_BUCKET_NAME` or `AWS_REGION` is wrong. |
| `EndpointConnectionError` / timeout | Network/firewall issue, or `AWS_REGION` doesn't match where the bucket actually lives. |

Once that passes, the same credentials are what `dags/nightly_schedule_download.py`
uses (passed through as container env vars in `docker-compose.yml`) - no separate
setup needed for Airflow itself. If you change `.env`, recreate the container to
pick it up:

```
docker compose up -d airflow --force-recreate
```

## Darwin Kafka credentials

Fill in the Kafka section of `.env`:

```
CONSUMER_GROUP=
CONSUMER_USERNAME=
CONSUMER_PASSWORD=
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SECURITY_PROTOCOL=SASL_SSL
JSON_TOPIC=
```

These come from your Darwin/National Rail data feed subscription. There's no
built-in connection test for this one, but a quick way to check it works is to
add something to your watchlist (see below) and run `main.py` - if the
credentials are wrong you'll see a Kafka authentication error in `logs/app.log`
within a few seconds of startup.

## Running it

Postgres and Airflow are containerized; the live ingest and API services run
natively:

```
# Terminal 1 - live ingest (always-on)
uv run python main.py

# Terminal 2 - API service (always-on)
uv run uvicorn services.api:app --port 8000
```

The API is then at `http://127.0.0.1:8000` (interactive docs at `/docs`).

## API reference

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/status` | Everything for the display: upcoming departures for each watched station (with live status + reliability where available) and current status for each pinned train. |
| `GET` | `/stations/search?q=` | Look up a station by name/CRS code/tiploc, to find what to pass to the watchlist endpoints below. |
| `GET` | `/watchlist/stations` | List current station-board watches. |
| `POST` | `/watchlist/stations` | Add a station watch. Body: `{"tiploc": "...", "destination_tiploc": "..."}` (destination optional). |
| `DELETE` | `/watchlist/stations/{id}` | Remove a station watch. |
| `GET` | `/watchlist/trains` | List current pinned-train watches. |
| `POST` | `/watchlist/trains` | Pin a specific recurring train. Body: `{"uid": "...", "origin_tiploc": "..."}`. |
| `DELETE` | `/watchlist/trains/{id}` | Unpin a train. |

## Managing your watchlist

There are two ways to watch something:

- **A station board** - every upcoming departure from a station, optionally
  narrowed to services heading toward a particular destination. Good for "what
  can I catch from here right now".
- **A pinned train** - a specific recurring service (by CIF `uid`), identified
  from a particular origin station. Good for "my usual commute train".

Example: watch Surbiton departures toward Waterloo, and pin a specific known
service:

```
# find the tiploc for your station
curl "http://127.0.0.1:8000/stations/search?q=surbiton"

# watch every departure from Surbiton heading to Waterloo
curl -X POST http://127.0.0.1:8000/watchlist/stations \
  -H "Content-Type: application/json" \
  -d '{"tiploc": "SURBITN", "destination_tiploc": "WATRLOO"}'

# pin a specific train (uid) from Surbiton
curl -X POST http://127.0.0.1:8000/watchlist/trains \
  -H "Content-Type: application/json" \
  -d '{"uid": "A12345", "origin_tiploc": "SURBITN"}'
```

Changes take effect in the live ingest service within its refresh interval
(`WATCHLIST_REFRESH_SECONDS` in `services/observation.py`, default 2 minutes) -
no restart needed.

## Airflow DAGs

Both DAGs start paused (Airflow's default for newly-registered DAGs) - unpause
them in the UI or with `airflow dags unpause <dag_id>` once you're ready for
them to run on schedule.

- **`nightly_schedule_download`** (`0 2 * * *`) - downloads the daily CIF
  update every run, and the full weekly schedule only when S3's `LastModified`
  has actually changed since last time (tracked via a local marker file, since
  the full extract is tens of MB and doesn't change nightly). Both get upserted
  into `schedules`/`schedule_locations`/`tiplocs` via the same CIF parser
  (`load_timetable`), which handles full extracts and incremental deltas
  identically. Retries for ~3 hours (6 retries, 30 min apart) before giving up.
- **`nightly_stats`** (`15 2 * * *`) - computes every registered metric under
  `stats/` for the previous day's observed running data. No hard dependency on
  the schedule DAG - it only reads `live_journeys`/`live_journey_events`, which
  the always-on ingest service writes continuously regardless of when the
  schedule was last refreshed.

Run either manually to test:

```
docker compose exec airflow airflow dags test nightly_schedule_download 2026-08-24
docker compose exec airflow airflow dags test nightly_stats 2026-08-24
```

## Reliability stats

Metrics are modular by design - `stats/__init__.py` auto-discovers every
sibling module (skipping ones starting with `_`) and runs it. **Adding a new
metric is "add a file", nothing else**:

```python
# stats/cancellation_rate.py
METRIC_NAME = "cancellation_rate"

def compute(db, stat_date: str) -> list[dict]:
    # return [{"scope_type": "uid", "scope_value": "...", "value": ...}, ...]
    ...
```

No DAG changes, no schema changes, no registration step - `nightly_stats`
picks it up automatically. `stats/on_time_percentage.py` and
`stats/average_delay.py` are the two shipped examples, both reading the same
underlying observations (`Database.get_departure_observations`) via pandas.

`on_time_percentage`'s metric name (`on_time_pct`) is what
`services/database.py`'s live-status queries display as the reliability
figure for each departure/pinned train - see `RELIABILITY_METRIC` in that
file if you want to swap which metric is shown there.

## Database schema

| Table | Purpose |
| --- | --- |
| `tiplocs` | Reference data: location codes → names/CRS codes, from CIF `TI` records. |
| `schedules` / `schedule_locations` | The base weekly timetable (CIF `BS`/`LO`/`LI`/`LT` records). |
| `watchlist_stations` / `watchlist_trains` | What you're watching - managed via the API, not edited directly. |
| `live_journeys` | One row per real-time journey instance (Darwin `rid`), linking a day's running to a `uid`. |
| `live_journey_events` | Observed planned/estimated/actual times per stop, written continuously by the live ingest service. |
| `daily_stats` | Computed reliability metrics - generic `(metric_name, scope_type, scope_value, stat_date, value)` shape so new metrics never need a schema change. |

## Troubleshooting

**`psycopg2` import fails with a `pg_config` build error** - the dependency
resolved to source-build `psycopg2` instead of a wheel. This repo already
pins `psycopg2-binary` in `pyproject.toml`; run `uv sync` again.

**Port 5432 already in use / can't reach Postgres** - see the port-conflict
note in [Setup](#setup) above. Check what's actually bound:

```
docker compose ps
```

**`ModuleNotFoundError: services` when a DAG loads** - Airflow's container
needs `services`/`stats` on its `PYTHONPATH` (already set in
`docker-compose.yml`) and the corresponding bind mounts. Verify directly:

```
docker compose exec airflow python -c "import services, stats; print('ok')"
```

**A DAG's own import (`boto3`, `pandas`, ...) fails inside Airflow but works
locally** - it's missing from `_PIP_ADDITIONAL_REQUIREMENTS` in
`docker-compose.yml`. Add it there, then:

```
docker compose up -d --force-recreate airflow
```

**Containers exited unexpectedly** - Docker Desktop can restart its backend
on its own (resource limits, WSL2 cycling, updates). Just bring them back up:

```
docker compose up -d
```

**DAG changes don't show up in the Airflow UI/CLI** - force a reparse:

```
docker compose exec airflow airflow dags reserialize
```

## Known limitations / provisional decisions

A few things were implemented against reasonable assumptions rather than
confirmed real Darwin message samples, since this was built without a live
feed to test against end-to-end. Worth revisiting once you've watched some
real traffic:

- **`TrainUpdate.ssd`** (schedule date, used to link a `rid` back to a
  specific day's `schedules` row) is read from the parent Darwin `uR` record
  if present, falling back to "today" otherwise. Confirm this against a real
  message and adjust `services/observation.py`'s `MessageParser` if Darwin
  actually carries it elsewhere.
- **Departure vs. arrival field mapping** in `TrainLocation` (`services/models.py`)
  only captures arrival-side estimated/actual times (`et`/`at`) - Darwin's real
  keys for departure estimates/actuals (commonly `etd`/`atd`) aren't parsed yet,
  so `est_dep`/`act_dep` in `live_journey_events` stay `None` until that's added.
- **Cancellations** aren't detected from `TS` messages - `live_journeys.cancelled`
  exists in the schema but nothing sets it `True` yet.
- **`nightly_stats`'s "yesterday"** is computed in UTC, not the UK-local traffic
  day - only matters for a handful of trains running past midnight during BST.
- **Reliability stats currently exist for pinned/watched trains and stations
  only** - the live ingest service only persists events for things on your
  watchlist, so `daily_stats` will only ever have data for what you've watched.
