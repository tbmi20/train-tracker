import psycopg2

from services.models import Schedule, TiplocInsert


class Database:
    """Thin wrapper around a Postgres connection for the timetable schema."""

    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn)

    def close(self) -> None:
        self.conn.close()

    def initialise_schema(self) -> None:
        with self.conn.cursor() as cursor:
            # Reference data from TI records - locations schedules can point to
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tiplocs (
                    tiploc VARCHAR(7) PRIMARY KEY,
                    nlc VARCHAR(6),
                    nlc_check_char VARCHAR(1),
                    tps_description VARCHAR(26),
                    stanox VARCHAR(5),
                    crs_code VARCHAR(3),
                    nlc_description VARCHAR(16)
                )
                """
            )

            # One row per BS record. Natural key per CIF docs is (uid, start_date,
            # stp_indicator) - uid alone isn't unique because STP overlays reuse it.
            cursor.execute(
                """
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
                """
            )

            # One row per LO/LI/LT record, linked back to its schedule. stop_sequence
            # preserves journey order; location_type distinguishes which CIF record
            # produced the row, since LO/LI/LT each populate a different subset of
            # the arrival/departure/pass columns.
            cursor.execute(
                """
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
                """
            )

            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_schedules_uid ON schedules (uid)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_schedule_locations_tiploc ON schedule_locations (tiploc)"
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
                    tiploc._3_alpha_code,
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
