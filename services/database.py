import logging
from datetime import datetime

import psycopg2

from services.models import Schedule, TiplocInsert

logger = logging.getLogger(__name__)

# Canonical name for the primary reliability metric surfaced in the API,
# matching whatever the (not yet built) nightly stats job writes to
# daily_stats under metric_name='on_time_pct'.
RELIABILITY_METRIC = "on_time_pct"


class Database:
    """Thin wrapper around a Postgres connection for the timetable schema."""

    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn)

    def close(self) -> None:
        self.conn.close()

    def initialise_schema(self) -> None:
        with self.conn.cursor() as cursor:
            # Reference data from TI records - locations schedules can point to
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tiplocs (
                    tiploc VARCHAR(7) PRIMARY KEY,
                    nlc VARCHAR(6),
                    nlc_check_char VARCHAR(1),
                    tps_description VARCHAR(26),
                    stanox VARCHAR(5),
                    crs_code VARCHAR(3),
                    nlc_description VARCHAR(16)
                )
                """)

            # One row per BS record. Natural key per CIF docs is (uid, start_date,
            # stp_indicator) - uid alone isn't unique because STP overlays reuse it.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schedules (
                    id BIGSERIAL PRIMARY KEY,
                    uid VARCHAR(6) NOT NULL,
                    start_date VARCHAR(6) NOT NULL,
                    end_date VARCHAR(6) NOT NULL,
                    stp_indicator VARCHAR(1) NOT NULL,
                    transaction_type VARCHAR(1),
                    days_run VARCHAR(7),
                    bank_holiday_running VARCHAR(1),
                    train_status VARCHAR(1),
                    train_category VARCHAR(2),
                    train_identity VARCHAR(4),
                    headcode VARCHAR(4),
                    train_service_code VARCHAR(8),
                    portion_id VARCHAR(1),
                    power_type VARCHAR(3),
                    timing_load VARCHAR(4),
                    speed VARCHAR(3),
                    operating_characteristics VARCHAR(6),
                    seating_class VARCHAR(1),
                    sleeping_car VARCHAR(1),
                    reservations VARCHAR(1),
                    catering_code VARCHAR(4),
                    service_branding VARCHAR(4),
                    UNIQUE (uid, start_date, stp_indicator)
                )
                """)

            # One row per LO/LI/LT record, linked back to its schedule. stop_sequence
            # preserves journey order; location_type distinguishes which CIF record
            # produced the row, since LO/LI/LT each populate a different subset of
            # the arrival/departure/pass columns.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schedule_locations (
                    id BIGSERIAL PRIMARY KEY,
                    schedule_id BIGINT NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
                    stop_sequence SMALLINT NOT NULL,
                    location_type VARCHAR(2) NOT NULL CHECK (location_type IN ('LO', 'LI', 'LT')),
                    tiploc VARCHAR(7) NOT NULL REFERENCES tiplocs(tiploc),
                    scheduled_arrival VARCHAR(5),
                    scheduled_departure VARCHAR(5),
                    scheduled_pass VARCHAR(5),
                    public_arrival VARCHAR(4),
                    public_departure VARCHAR(4),
                    platform VARCHAR(3),
                    line VARCHAR(3),
                    path VARCHAR(3),
                    activity VARCHAR(12),
                    engineering_allowance VARCHAR(2),
                    pathing_allowance VARCHAR(2),
                    performance_allowance VARCHAR(2),
                    UNIQUE (schedule_id, stop_sequence)
                )
                """)

            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_schedules_uid ON schedules (uid)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_schedule_locations_tiploc ON schedule_locations (tiploc)"
            )

            # User-managed watchlists (replaces the old UserSettings JSON file).
            # A station watch is a board: every departure from `tiploc`,
            # optionally narrowed to services calling at `destination_tiploc`.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist_stations (
                    id BIGSERIAL PRIMARY KEY,
                    tiploc VARCHAR(7) NOT NULL REFERENCES tiplocs(tiploc),
                    destination_tiploc VARCHAR(7) REFERENCES tiplocs(tiploc),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """)

            # A pinned train is a specific recurring service, identified by
            # its CIF uid plus the station it's watched from (a uid can call
            # at many locations, so origin_tiploc disambiguates which leg).
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist_trains (
                    id BIGSERIAL PRIMARY KEY,
                    uid VARCHAR(6) NOT NULL,
                    origin_tiploc VARCHAR(7) NOT NULL REFERENCES tiplocs(tiploc),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """)

            # One row per real-time journey instance (Darwin's rid is unique
            # per service per day, unlike uid which recurs). schedule_id is
            # nullable - resolving rid/uid/ssd back to a specific schedules
            # row can fail (schedule not loaded yet, VSTP-only service).
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS live_journeys (
                    rid VARCHAR(16) PRIMARY KEY,
                    uid VARCHAR(6) NOT NULL,
                    ssd VARCHAR(10),
                    schedule_id BIGINT REFERENCES schedules(id) ON DELETE SET NULL,
                    cancelled BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """)

            # Observed times per stop, upserted on every relevant Kafka
            # message. This is the source of truth for both the live-status
            # API and the nightly reliability stats.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS live_journey_events (
                    id BIGSERIAL PRIMARY KEY,
                    rid VARCHAR(16) NOT NULL REFERENCES live_journeys(rid) ON DELETE CASCADE,
                    tiploc VARCHAR(7) NOT NULL REFERENCES tiplocs(tiploc),
                    planned_arr VARCHAR(8),
                    planned_dep VARCHAR(8),
                    est_arr VARCHAR(8),
                    est_dep VARCHAR(8),
                    act_arr VARCHAR(8),
                    act_dep VARCHAR(8),
                    platform VARCHAR(3),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (rid, tiploc)
                )
                """)

            # Modular stats storage - adding a new metric is "insert rows
            # with a new metric_name", never a schema change. scope_type is
            # e.g. 'station' or 'uid'; scope_value is the tiploc/uid itself.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id BIGSERIAL PRIMARY KEY,
                    metric_name VARCHAR(64) NOT NULL,
                    scope_type VARCHAR(32) NOT NULL,
                    scope_value VARCHAR(64) NOT NULL,
                    stat_date DATE NOT NULL,
                    value NUMERIC NOT NULL,
                    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (metric_name, scope_type, scope_value, stat_date)
                )
                """)

            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_live_journeys_uid_ssd ON live_journeys (uid, ssd)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_live_journey_events_tiploc ON live_journey_events (tiploc)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_daily_stats_lookup ON daily_stats (metric_name, scope_type, scope_value, stat_date)"
            )
        self.conn.commit()

    def upsert_tiploc(self, tiploc: TiplocInsert) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO tiplocs (
                    tiploc, nlc, nlc_check_char, tps_description, stanox,
                    crs_code, nlc_description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tiploc) DO UPDATE SET
                    nlc = EXCLUDED.nlc,
                    nlc_check_char = EXCLUDED.nlc_check_char,
                    tps_description = EXCLUDED.tps_description,
                    stanox = EXCLUDED.stanox,
                    crs_code = EXCLUDED.crs_code,
                    nlc_description = EXCLUDED.nlc_description
                """,
                (
                    tiploc.tiploc,
                    str(tiploc.nlc),
                    tiploc.nlc_check_char,
                    tiploc.tps_description,
                    str(tiploc.stanox),
                    tiploc.crs_code,
                    tiploc.nlc_description,
                ),
            )
        self.conn.commit()

    def delete_tiploc(self, tiploc: str) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM tiplocs WHERE tiploc = %s", (tiploc,))
        self.conn.commit()

    def upsert_schedule(self, schedule: Schedule) -> None:
        """Writes a schedule and its locations as a single unit.

        CIF 'D' (delete) transactions remove the schedule outright. 'N' (new)
        and 'R' (revise) upsert the schedule row keyed on (uid, start_date,
        stp_indicator), then fully replace its locations - CIF gives no
        per-location key to merge against, so delete-and-reinsert is the
        simplest correct way to handle a revision.
        """
        with self.conn.cursor() as cursor:
            if schedule.transaction_type == "D":
                cursor.execute(
                    """
                    DELETE FROM schedules
                    WHERE uid = %s AND start_date = %s AND stp_indicator = %s
                    """,
                    (schedule.uid, schedule.start_date, schedule.stp_indicator),
                )
                self.conn.commit()
                return

            cursor.execute(
                """
                INSERT INTO schedules (
                    uid, start_date, end_date, stp_indicator, transaction_type,
                    days_run, bank_holiday_running, train_status, train_category,
                    train_identity, headcode, train_service_code, portion_id,
                    power_type, timing_load, speed, operating_characteristics,
                    seating_class, sleeping_car, reservations, catering_code,
                    service_branding
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (uid, start_date, stp_indicator) DO UPDATE SET
                    end_date = EXCLUDED.end_date,
                    transaction_type = EXCLUDED.transaction_type,
                    days_run = EXCLUDED.days_run,
                    bank_holiday_running = EXCLUDED.bank_holiday_running,
                    train_status = EXCLUDED.train_status,
                    train_category = EXCLUDED.train_category,
                    train_identity = EXCLUDED.train_identity,
                    headcode = EXCLUDED.headcode,
                    train_service_code = EXCLUDED.train_service_code,
                    portion_id = EXCLUDED.portion_id,
                    power_type = EXCLUDED.power_type,
                    timing_load = EXCLUDED.timing_load,
                    speed = EXCLUDED.speed,
                    operating_characteristics = EXCLUDED.operating_characteristics,
                    seating_class = EXCLUDED.seating_class,
                    sleeping_car = EXCLUDED.sleeping_car,
                    reservations = EXCLUDED.reservations,
                    catering_code = EXCLUDED.catering_code,
                    service_branding = EXCLUDED.service_branding
                RETURNING id
                """,
                (
                    schedule.uid,
                    schedule.start_date,
                    schedule.end_date,
                    schedule.stp_indicator,
                    schedule.transaction_type,
                    schedule.days_run,
                    schedule.bank_holiday_running,
                    schedule.train_status,
                    schedule.train_category,
                    schedule.train_identity,
                    schedule.headcode,
                    schedule.train_service_code,
                    schedule.portion_id,
                    schedule.power_type,
                    schedule.timing_load,
                    schedule.speed,
                    schedule.operating_characteristics,
                    schedule.seating_class,
                    schedule.sleeping_car,
                    schedule.reservations,
                    schedule.catering_code,
                    schedule.service_branding,
                ),
            )
            schedule_id = cursor.fetchone()[0]

            cursor.execute(
                "DELETE FROM schedule_locations WHERE schedule_id = %s",
                (schedule_id,),
            )

            rows = [
                (
                    schedule_id,
                    0,
                    "LO",
                    schedule.start_location.tiploc,
                    None,
                    schedule.start_location.scheduled_departure,
                    None,
                    None,
                    schedule.start_location.public_departure,
                    schedule.start_location.platform,
                    schedule.start_location.line,
                    None,
                    schedule.start_location.activity,
                    schedule.start_location.engineering_allowance,
                    schedule.start_location.pathing_allowance,
                    schedule.start_location.performance_allowance,
                )
            ]
            for sequence, stop in enumerate(schedule.stops, start=1):
                rows.append(
                    (
                        schedule_id,
                        sequence,
                        "LI",
                        stop.tiploc,
                        stop.scheduled_arrival,
                        stop.scheduled_departure,
                        stop.scheduled_pass,
                        stop.public_arrival,
                        stop.public_departure,
                        stop.platform,
                        stop.line,
                        stop.path,
                        stop.activity,
                        stop.engineering_allowance,
                        stop.pathing_allowance,
                        stop.performance_allowance,
                    )
                )
            rows.append(
                (
                    schedule_id,
                    len(schedule.stops) + 1,
                    "LT",
                    schedule.end_location.tiploc,
                    schedule.end_location.scheduled_arrival,
                    None,
                    None,
                    schedule.end_location.public_arrival,
                    None,
                    schedule.end_location.platform,
                    schedule.end_location.line,
                    None,
                    schedule.end_location.activity,
                    None,
                    None,
                    None,
                )
            )

            cursor.executemany(
                """
                INSERT INTO schedule_locations (
                    schedule_id, stop_sequence, location_type, tiploc,
                    scheduled_arrival, scheduled_departure, scheduled_pass,
                    public_arrival, public_departure, platform, line, path,
                    activity, engineering_allowance, pathing_allowance,
                    performance_allowance
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        self.conn.commit()

    @staticmethod
    def _rows_as_dicts(cursor) -> list[dict]:
        columns = [col.name for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # --- Watchlist management -------------------------------------------------

    def add_watchlist_station(
        self, tiploc: str, destination_tiploc: str | None = None
    ) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO watchlist_stations (tiploc, destination_tiploc)
                VALUES (%s, %s)
                RETURNING id
                """,
                (tiploc, destination_tiploc),
            )
            watch_id = cursor.fetchone()[0]
        self.conn.commit()
        return watch_id

    def remove_watchlist_station(self, watch_id: int) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM watchlist_stations WHERE id = %s", (watch_id,))
        self.conn.commit()

    def list_watchlist_stations(self) -> list[dict]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, tiploc, destination_tiploc, created_at FROM watchlist_stations"
            )
            return self._rows_as_dicts(cursor)

    def add_watchlist_train(self, uid: str, origin_tiploc: str) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO watchlist_trains (uid, origin_tiploc)
                VALUES (%s, %s)
                RETURNING id
                """,
                (uid, origin_tiploc),
            )
            watch_id = cursor.fetchone()[0]
        self.conn.commit()
        return watch_id

    def remove_watchlist_train(self, watch_id: int) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute("DELETE FROM watchlist_trains WHERE id = %s", (watch_id,))
        self.conn.commit()

    def list_watchlist_trains(self) -> list[dict]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, uid, origin_tiploc, created_at FROM watchlist_trains"
            )
            return self._rows_as_dicts(cursor)

    # --- Live journey ingest ---------------------------------------------------

    def upsert_live_journey(
        self,
        rid: str,
        uid: str,
        ssd: str | None = None,
        schedule_id: int | None = None,
        cancelled: bool = False,
    ) -> bool:
        """Returns False (and logs) instead of raising on a bad Kafka message.

        This is called from an always-on streaming loop with no client
        waiting on the result - one malformed/oversized field from the feed
        should never be allowed to poison the long-lived connection and
        silently kill ingestion until the process is restarted.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO live_journeys (rid, uid, ssd, schedule_id, cancelled, updated_at)
                    VALUES (%s, %s, %s, %s, %s, now())
                    ON CONFLICT (rid) DO UPDATE SET
                        ssd = COALESCE(EXCLUDED.ssd, live_journeys.ssd),
                        schedule_id = COALESCE(EXCLUDED.schedule_id, live_journeys.schedule_id),
                        cancelled = EXCLUDED.cancelled,
                        updated_at = now()
                    """,
                    (rid, uid, ssd, schedule_id, cancelled),
                )
            self.conn.commit()
            return True
        except psycopg2.Error:
            self.conn.rollback()
            logger.warning(
                "Failed to upsert live_journey rid=%r uid=%r", rid, uid, exc_info=True
            )
            return False

    def upsert_live_journey_event(
        self,
        rid: str,
        tiploc: str,
        planned_arr: str | None = None,
        planned_dep: str | None = None,
        est_arr: str | None = None,
        est_dep: str | None = None,
        act_arr: str | None = None,
        act_dep: str | None = None,
        platform: str | None = None,
    ) -> bool:
        """Returns False (and logs) instead of raising - see upsert_live_journey."""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO live_journey_events (
                        rid, tiploc, planned_arr, planned_dep, est_arr, est_dep,
                        act_arr, act_dep, platform, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (rid, tiploc) DO UPDATE SET
                        planned_arr = COALESCE(EXCLUDED.planned_arr, live_journey_events.planned_arr),
                        planned_dep = COALESCE(EXCLUDED.planned_dep, live_journey_events.planned_dep),
                        est_arr = COALESCE(EXCLUDED.est_arr, live_journey_events.est_arr),
                        est_dep = COALESCE(EXCLUDED.est_dep, live_journey_events.est_dep),
                        act_arr = COALESCE(EXCLUDED.act_arr, live_journey_events.act_arr),
                        act_dep = COALESCE(EXCLUDED.act_dep, live_journey_events.act_dep),
                        platform = COALESCE(EXCLUDED.platform, live_journey_events.platform),
                        updated_at = now()
                    """,
                    (
                        rid,
                        tiploc,
                        planned_arr,
                        planned_dep,
                        est_arr,
                        est_dep,
                        act_arr,
                        act_dep,
                        platform,
                    ),
                )
            self.conn.commit()
            return True
        except psycopg2.Error:
            self.conn.rollback()
            logger.warning(
                "Failed to upsert live_journey_event rid=%r tiploc=%r",
                rid,
                tiploc,
                exc_info=True,
            )
            return False

    # --- Stats -------------------------------------------------------------

    def upsert_daily_stat(
        self,
        metric_name: str,
        scope_type: str,
        scope_value: str,
        stat_date: str,
        value: float,
    ) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO daily_stats (metric_name, scope_type, scope_value, stat_date, value, computed_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (metric_name, scope_type, scope_value, stat_date) DO UPDATE SET
                    value = EXCLUDED.value,
                    computed_at = now()
                """,
                (metric_name, scope_type, scope_value, stat_date, value),
            )
        self.conn.commit()

    def get_departure_observations(self, stat_date: str) -> list[dict]:
        """Every observed stop on `stat_date` (ssd) that had a scheduled
        departure - the raw material stats modules aggregate into metrics
        like on_time_pct. One row per (rid, tiploc).
        """
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT lj.uid, lje.tiploc, lje.planned_dep, lje.est_dep, lje.act_dep
                FROM live_journeys lj
                JOIN live_journey_events lje ON lje.rid = lj.rid
                WHERE lj.ssd = %s AND lje.planned_dep IS NOT NULL
                """,
                (stat_date,),
            )
            return self._rows_as_dicts(cursor)

    # --- Live status reads (for the API service) -------------------------------

    def search_stations(self, query: str, limit: int = 10) -> list[dict]:
        """Looks up tiplocs by name/CRS code, for picking a station to watch."""
        pattern = f"%{query}%"
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT tiploc, crs_code, tps_description
                FROM tiplocs
                WHERE tps_description ILIKE %s OR crs_code ILIKE %s OR tiploc ILIKE %s
                ORDER BY tps_description
                LIMIT %s
                """,
                (pattern, pattern, pattern, limit),
            )
            return self._rows_as_dicts(cursor)

    def get_upcoming_departures(
        self,
        tiploc: str,
        destination_tiploc: str | None = None,
        limit: int = 5,
    ) -> list[dict]:
        """Next scheduled departures from `tiploc` today, overlaid with live data.

        Picks the CIF schedule row for each uid that's actually in effect
        today (date range + day-of-week match, STP overlay/cancellation
        takes precedence over the permanent schedule per CIF rules), then
        joins in live_journeys/live_journey_events (matched on uid+ssd) and
        the most recent daily_stats reliability figure for the station, if
        any exist yet.
        """
        now = datetime.now()
        today_yymmdd = now.strftime("%y%m%d")
        today_iso = now.strftime("%Y-%m-%d")
        weekday_position = (
            now.isoweekday()
        )  # Monday=1 .. Sunday=7, matches CIF days_run order
        now_hhmm = now.strftime("%H%M")

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                WITH day_schedules AS (
                    SELECT DISTINCT ON (s.uid)
                        s.id, s.uid, s.stp_indicator
                    FROM schedules s
                    WHERE s.start_date <= %(today_yymmdd)s
                      AND s.end_date >= %(today_yymmdd)s
                      AND substring(s.days_run FROM %(weekday)s FOR 1) = '1'
                    ORDER BY s.uid,
                        CASE s.stp_indicator
                            WHEN 'C' THEN 0
                            WHEN 'O' THEN 1
                            WHEN 'N' THEN 2
                            WHEN 'P' THEN 3
                            ELSE 4
                        END
                ),
                departures AS (
                    SELECT
                        ds.id AS schedule_id, ds.uid, sl.stop_sequence,
                        substring(sl.scheduled_departure FROM 1 FOR 4) AS scheduled_departure,
                        sl.platform AS scheduled_platform
                    FROM day_schedules ds
                    JOIN schedule_locations sl ON sl.schedule_id = ds.id
                    WHERE ds.stp_indicator <> 'C'
                      AND sl.tiploc = %(tiploc)s
                      AND sl.scheduled_departure IS NOT NULL
                      AND substring(sl.scheduled_departure FROM 1 FOR 4) >= %(now_hhmm)s
                )
                SELECT
                    d.uid, d.scheduled_departure, d.scheduled_platform,
                    lj.rid, lj.cancelled,
                    lje.est_dep, lje.act_dep, lje.platform AS live_platform,
                    rel.value AS reliability_pct
                FROM departures d
                LEFT JOIN live_journeys lj ON lj.uid = d.uid AND lj.ssd = %(today_iso)s
                LEFT JOIN live_journey_events lje ON lje.rid = lj.rid AND lje.tiploc = %(tiploc)s
                LEFT JOIN LATERAL (
                    SELECT value FROM daily_stats
                    WHERE metric_name = %(reliability_metric)s
                      AND scope_type = 'uid' AND scope_value = d.uid
                    ORDER BY stat_date DESC LIMIT 1
                ) rel ON TRUE
                WHERE %(destination_tiploc)s IS NULL OR EXISTS (
                    SELECT 1 FROM schedule_locations sl2
                    WHERE sl2.schedule_id = d.schedule_id
                      AND sl2.stop_sequence > d.stop_sequence
                      AND sl2.tiploc = %(destination_tiploc)s
                )
                ORDER BY d.scheduled_departure
                LIMIT %(limit)s
                """,
                {
                    "today_yymmdd": today_yymmdd,
                    "today_iso": today_iso,
                    "weekday": weekday_position,
                    "now_hhmm": now_hhmm,
                    "tiploc": tiploc,
                    "destination_tiploc": destination_tiploc,
                    "reliability_metric": RELIABILITY_METRIC,
                    "limit": limit,
                },
            )
            return self._rows_as_dicts(cursor)

    def get_train_status(self, uid: str, origin_tiploc: str) -> dict | None:
        """Today's status for a single pinned train, or None if it doesn't run today."""
        now = datetime.now()
        today_yymmdd = now.strftime("%y%m%d")
        today_iso = now.strftime("%Y-%m-%d")
        weekday_position = now.isoweekday()

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                WITH day_schedule AS (
                    SELECT s.id, s.uid, s.stp_indicator
                    FROM schedules s
                    WHERE s.uid = %(uid)s
                      AND s.start_date <= %(today_yymmdd)s
                      AND s.end_date >= %(today_yymmdd)s
                      AND substring(s.days_run FROM %(weekday)s FOR 1) = '1'
                    ORDER BY
                        CASE s.stp_indicator
                            WHEN 'C' THEN 0
                            WHEN 'O' THEN 1
                            WHEN 'N' THEN 2
                            WHEN 'P' THEN 3
                            ELSE 4
                        END
                    LIMIT 1
                )
                SELECT
                    ds.uid, ds.stp_indicator,
                    substring(sl.scheduled_departure FROM 1 FOR 4) AS scheduled_departure,
                    sl.platform AS scheduled_platform,
                    lj.rid, lj.cancelled,
                    lje.est_dep, lje.act_dep, lje.platform AS live_platform,
                    rel.value AS reliability_pct
                FROM day_schedule ds
                JOIN schedule_locations sl ON sl.schedule_id = ds.id AND sl.tiploc = %(origin_tiploc)s
                LEFT JOIN live_journeys lj ON lj.uid = ds.uid AND lj.ssd = %(today_iso)s
                LEFT JOIN live_journey_events lje ON lje.rid = lj.rid AND lje.tiploc = %(origin_tiploc)s
                LEFT JOIN LATERAL (
                    SELECT value FROM daily_stats
                    WHERE metric_name = %(reliability_metric)s
                      AND scope_type = 'uid' AND scope_value = ds.uid
                    ORDER BY stat_date DESC LIMIT 1
                ) rel ON TRUE
                LIMIT 1
                """,
                {
                    "uid": uid,
                    "origin_tiploc": origin_tiploc,
                    "today_yymmdd": today_yymmdd,
                    "today_iso": today_iso,
                    "weekday": weekday_position,
                    "reliability_metric": RELIABILITY_METRIC,
                },
            )
            rows = self._rows_as_dicts(cursor)
            return rows[0] if rows else None
