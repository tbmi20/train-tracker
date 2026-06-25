import json
from services.models import UserSettings, Schedule
from services.database import Database


def load_user_settings(file_path: str) -> UserSettings:
    """Loads user settings from a JSON file."""
    try:
        with open(file_path, "r") as f:
            settings = json.load(f)
        return UserSettings.from_dict(settings)
        
    except FileNotFoundError:
        raise FileNotFoundError(f"Settings file not found at {file_path}. Using default settings.")
    except json.JSONDecodeError:
        raise ValueError(f"Error decoding JSON from {file_path}. Using default settings.")


def load_timetable(file_path: str, db: Database) -> None:

    complete_schedule = None

    with open(file_path, "r") as f:

        for line in f:
            if line.startswith("HD"): # Header record
                
                

            # BRANCH A: Reference Data (Build the dictionary on the fly)
            if line.startswith("TI"):
                tiploc_code = line[2:9].strip()
                full_name = line[18:44].strip()
                formatted_tiploc_insert = 
                db.insert_station(tiploc_code, full_name)
                continue

            # BRANCH B: Start of a new train schedule
            elif line.startswith("BS"):
                # If a train was already being built, index it before moving to the next
                if complete_schedule:
                    db.insert_schedule(complete_schedule, conn=conn)

                uid = line[3:9].strip()
                days_run = line[21:28].strip()
                complete_schedule = Schedule(uid=uid, days_run=days_run)

            # BRANCH C: Route Detail lines for the active train
            elif (
                line.startswith(("LO", "LI", "LT"))
                and complete_schedule is not None
            ):
                rec_type = line[0:2]
                tiploc = line[2:9].strip()

                # Extract the time based on line type
                time_str = None
                if rec_type == "LO":
                    time_str = line[15:19].strip()
                elif rec_type == "LT":
                    time_str = line[11:15].strip()
                elif rec_type == "LI":
                    time_str = line[53:57].strip()

                # Create location and attach the station name if we've already parsed its TI record
                location = TrainLocation(
                    tpl=tiploc,
                )
                complete_schedule.stops.append(location)

        # Clean up the final train at the end of the file loop
        if complete_schedule:
            db.insert_schedule(complete_schedule, conn=conn)

        db.mark_timetable_loaded(conn)
        conn.commit()
