import json
import logging
from services.models import (
    UserSettings,
    Schedule,
    Header,
    LocationOrigin,
    LocationIntermediate,
    LocationTermination,
    TiplocInsert,
)
from services.database import Database

logger = logging.getLogger(__name__)


def load_user_settings(file_path: str) -> UserSettings:
    """Loads user settings from a JSON file."""
    try:
        with open(file_path, "r") as f:
            settings = json.load(f)
        return UserSettings.from_dict(settings)

    except FileNotFoundError:
        raise FileNotFoundError(
            f"Settings file not found at {file_path}. Using default settings."
        )
    except json.JSONDecodeError:
        raise ValueError(
            f"Error decoding JSON from {file_path}. Using default settings."
        )


def _write_schedule(schedule: Schedule, db: Database) -> bool:
    """Upserts a schedule if it's actually complete.

    A well-formed CIF file always closes a BS block with an LT record
    before the next BS/EOF, but a truncated download (a real risk for an
    automated nightly S3 pull) can leave the last schedule in the file
    missing its end_location - upsert_schedule would otherwise crash on
    that with an unhelpful AttributeError. transaction_type 'D' (delete)
    schedules never carry locations at all, so they're exempt from this
    check.
    """
    if schedule.transaction_type != "D" and schedule.end_location is None:
        logger.warning(
            "Skipping incomplete schedule uid=%s (missing LT record - "
            "likely a truncated file)",
            schedule.uid,
        )
        return False
    db.upsert_schedule(schedule)
    return True


def load_timetable(file_path: str, db: Database) -> int:
    """Parses a CIF timetable file, writing tiplocs and schedules to the database.

    Returns the number of schedules written.
    """
    schedule_count = 0
    complete_schedule = None

    with open(file_path, "r") as f:

        for line in f:
            if line.startswith("HD"):  # Header record
                header = Header.from_cif_line(line)

            # BRANCH A: Reference Data (tiploc lookup table)
            elif line.startswith("TI"):
                db.upsert_tiploc(TiplocInsert.from_cif_line(line))

            elif line.startswith("TD"):
                tiploc_code = line[2:9].strip()
                db.delete_tiploc(tiploc_code)

            # BRANCH B: Start of a new train schedule
            elif line.startswith("BS"):
                # If a train was already being built, write it before moving to the next
                if complete_schedule and _write_schedule(complete_schedule, db):
                    schedule_count += 1
                complete_schedule = Schedule.from_cif_line(line)

            # BRANCH C: Route Detail lines for the active train
            elif line.startswith(("LO", "LT", "LI")) and complete_schedule:
                rec_type = line[0:2]

                # Extract the time based on line type
                if rec_type == "LO":
                    complete_schedule.start_location = LocationOrigin.from_cif_line(
                        line
                    )
                elif rec_type == "LI":
                    location = LocationIntermediate.from_cif_line(line)
                    complete_schedule.stops.append(location)
                elif rec_type == "LT":
                    complete_schedule.end_location = LocationTermination.from_cif_line(
                        line
                    )

        # Write the final train at the end of the file loop
        if complete_schedule and _write_schedule(complete_schedule, db):
            schedule_count += 1

    return schedule_count
