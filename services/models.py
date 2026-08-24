"""Models for structuring data from both internal and external sources."""

from pydantic import BaseModel, Field, field_validator
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Union
import json
import logging

logger = logging.getLogger(__name__)


# Internal models
@dataclass
class UserSettings:
    """Represents user settings for saved trains and favourite stations."""

    saved_trains: list[SavedTrain] = field(default_factory=list)
    favourite_stations: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> UserSettings:
        return cls(
            saved_trains=[
                SavedTrain(**train) for train in data.get("saved_trains", [])
            ],
            favourite_stations=data.get("favourite_stations", []),
        )

    @classmethod
    def to_json(cls, settings: UserSettings) -> str:
        return json.dumps(
            {
                "saved_trains": [train.to_dict() for train in settings.saved_trains],
                "favourite_stations": settings.favourite_stations,
            }
        )


@dataclass
class SavedTrain:
    """Represents a train that a user has saved for tracking."""

    # TODO: the uid doesn't specify a train so might need more info
    tiploc: str
    uid: str

    def to_dict(self) -> dict[str, str]:
        return {"tiploc": self.tiploc, "uid": self.uid}


# Pydantic models for Kafka messages
class TrainLocation(BaseModel):
    # Mapping the short Darwin keys to readable names
    tiploc: str = Field(alias="tpl")
    planned_arr: Optional[str] = Field(alias="pta", default=None)
    planned_dep: Optional[str] = Field(alias="ptd", default=None)

    # Reality check: actual times (at) take priority over estimated (et)
    est_arr: Optional[str] = Field(alias="et", default=None)
    act_arr: Optional[str] = Field(alias="at", default=None)

    platform_dict: Optional[Union[dict, str]] = Field(alias="plat", default=None)
    source: Optional[str] = Field(alias="src", default="Unknown")

    @property
    def current_status(self) -> str:
        if self.act_arr:
            return f"Arrived at {self.act_arr}"
        if self.est_arr:
            return f"Expected at {self.est_arr}"
        return "On time"

    @property
    def platform(self) -> Optional[str]:
        if self.platform_dict:
            if isinstance(self.platform_dict, dict):
                return self.platform_dict.get(
                    "", None
                )  # Darwin sometimes sends platform as {"": "1"}, which is a bit odd
            return self.platform_dict
        return None

    @property
    def platform_source(self) -> Optional[str]:
        if self.platform_dict:
            if isinstance(self.platform_dict, dict):
                return self.platform_dict.get("platsrc", None)
        return None

    @property
    def platform_confidence(self) -> Optional[str]:
        if self.platform_dict:
            if isinstance(self.platform_dict, dict):
                return self.platform_dict.get("conf", None)
        return None


class TrainLateReason(BaseModel):
    rid: str = Field(alias="rid")
    uid: str = Field(alias="uid")
    reason: str = Field(alias="LateReason")


class TrainUpdate(BaseModel):
    rid: str
    uid: str
    late_reason: Optional[str] = Field(alias="LateReason", default=None)
    # Handling the 'Location is sometimes a list, sometimes a dict' issue
    location: List[TrainLocation] = Field(alias="Location", default_factory=list)

    @field_validator(
        "location", mode="before"
    )  # Location can sometimes miss a tiploc if it's a LateReason update
    @classmethod
    def ensure_list(cls, v):
        if v is None:
            return []
        if isinstance(v, dict):
            if "tpl" in v:
                return [v]
            return []
        if isinstance(v, list):
            if len(v) > 1:
                logger.warning(
                    f"Received multiple Locations for train {v[0].get('rid', 'unknown')}"
                )
            return [entry for entry in v if isinstance(entry, dict) and "tpl" in entry]
        return v

    @field_validator("late_reason", mode="before")
    @classmethod
    def normalize_late_reason(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, dict):
            # Darwin can send LateReason as {"tiploc": "INCE", "": "574"}
            if "" in v and v[""] is not None:
                return str(v[""])
            if "LateReason" in v and v["LateReason"] is not None:
                return str(v["LateReason"])
            logger.warning("Received LateReason dict without a reason code")
            return None
        return str(v)


@dataclass
class Journey:
    uid: str
    rid: str
    location_history: list[TrainLocation]


class EventType(Enum):
    """Enum for train event types.

    PASS: Train is passing through the location without stopping.
    STOP: Train is stopping at the location.
    """

    PASS = "PASS"
    STOP = "STOP"


class Watchlist:
    """Maintains watchlists for user subscriptions."""

    def __init__(
        self,
        subscribed_trains: list[str],
        favourite_stations: Optional[list[str]] = None,
    ):
        favourite_stations = favourite_stations or []
        self.station_watchlist: dict[str, list[TrainUpdate]] = {
            station: [] for station in favourite_stations
        }
        self.train_watchlist: dict[str, Journey] = {
            uid: Journey(uid=uid, rid="", location_history=[])
            for uid in subscribed_trains
        }

    def update_watchlists(self, train_update: TrainUpdate):
        """Updates the watchlists with the latest train update information."""

        if train_update.uid in self.train_watchlist:  # live train update
            # Update existing entry or create a new one
            self.train_watchlist[train_update.uid].rid = train_update.rid
            self.train_watchlist[train_update.uid].location_history.extend(
                train_update.location
            )  # Append new location info
            return self.train_watchlist[
                train_update.uid
            ]  # Read-only Journey object for external use

        stations_intersect = set([loc.tiploc for loc in train_update.location]) & set(
            self.station_watchlist.keys()
        )  # station update for a train we're not tracking, but we care about the station
        if stations_intersect:
            for loc in stations_intersect:
                self.station_watchlist[loc].append(train_update)
                print(
                    f"Train {train_update.rid} ({train_update.uid}) has an update for station {loc}"
                )


# Pydantic data models for timetable schedule
class Schedule(BaseModel):
    transaction_type: str
    uid: str
    start_date: str  # YYMMDD
    end_date: str  # YYMMDD
    days_run: str
    bank_holiday_running: Optional[str]
    train_status: str
    train_category: str
    train_identity: str
    headcode: Optional[str]
    train_service_code: str
    portion_id: Optional[str]
    power_type: str
    timing_load: Optional[str]
    speed: str
    operating_characteristics: Optional[str]
    seating_class: Optional[str]
    sleeping_car: Optional[str]
    reservations: Optional[str]
    catering_code: Optional[str]
    service_branding: Optional[str]
    stp_indicator: str

    # Populated incrementally as LO/LI/LT lines following the BS record are parsed
    start_location: Optional[LocationOrigin] = None
    stops: list[LocationIntermediate] = field(default_factory=list)
    end_location: Optional[LocationTermination] = None

    @classmethod
    def from_cif_line(cls, line: str) -> Schedule:
        if not line.startswith("BS"):
            raise ValueError("Line does not start with 'BS' record identifier.")
        return cls(
            transaction_type=line[2:3].strip(),
            uid=line[3:9].strip(),
            start_date=line[9:15].strip(),
            end_date=line[15:21].strip(),
            days_run=line[21:28].strip(),
            bank_holiday_running=line[28:29].strip(),
            train_status=line[29:30].strip(),
            train_category=line[30:32].strip(),
            train_identity=line[32:36].strip(),
            headcode=line[36:40].strip(),
            train_service_code=line[41:49].strip(),
            portion_id=line[49:50].strip(),
            power_type=line[50:53].strip(),
            timing_load=line[53:57].strip(),
            speed=line[57:60].strip(),
            operating_characteristics=line[60:66].strip(),
            seating_class=line[66:67].strip(),
            sleeping_car=line[67:68].strip(),
            reservations=line[68:69].strip(),
            catering_code=line[69:73].strip(),
            service_branding=line[73:77].strip(),
            stp_indicator=line[78:79].strip(),
        )


class LocationOrigin(BaseModel):
    record_identity: str  # 	2	LO	Constant value 'LO'
    tiploc: str  # 	8	MLCHSTR	Timing Point Location code
    scheduled_departure: str  # 	5	0000	Scheduled departure time (HHMM)
    public_departure: str  # 	4	0000	Public departure time (HHMM)
    platform: str  # 	3	00	Platform number
    line: str  # 	3	00	Line code
    engineering_allowance: str  # 	2	00	Engineering allowance (minutes)
    pathing_allowance: str  # 	2	00	Pathing allowance (minutes)
    activity: str  # 	12
    performance_allowance: str  # 	2	00	Performance allowance (minutes)

    @classmethod
    def from_cif_line(cls, line: str) -> LocationOrigin:
        if not line.startswith("LO"):
            raise ValueError("Line does not start with 'LO' record identifier.")
        return cls(
            record_identity=line[0:2].strip(),
            tiploc=line[2:10].strip(),
            scheduled_departure=line[10:15].strip(),
            public_departure=line[15:19].strip(),
            platform=line[19:22].strip(),
            line=line[22:25].strip(),
            engineering_allowance=line[25:27].strip(),
            pathing_allowance=line[27:29].strip(),
            activity=line[29:41].strip(),
            performance_allowance=line[41:43].strip(),
        )


class LocationIntermediate(BaseModel):
    record_identity: str  # 	2	LI	Constant value 'LI'
    tiploc: str  # 	8	MLCHSTR	Timing Point Location code
    scheduled_arrival: str  # 	5	0000	Scheduled arrival time (HHMM)
    scheduled_departure: str  # 	5	0000	Scheduled departure time (HHMM)
    scheduled_pass: str  # 	5	0000	Scheduled pass time (HHMM)
    public_arrival: str  # 	4	0000	Public arrival time (HHMM)
    public_departure: str  # 	4	0000	Public departure time (HHMM)
    platform: str  # 	3	00	Platform number
    line: str  # 	3	00	Line code
    path: str  # 	3	00	Path code
    activity: str  # 	12
    engineering_allowance: str  # 	2	00	Engineering allowance (minutes)
    pathing_allowance: str  # 	2	00	Pathing allowance (minutes)
    performance_allowance: str  # 	2	00	Performance allowance (minutes)

    @classmethod
    def from_cif_line(cls, line: str) -> LocationIntermediate:
        if not line.startswith("LI"):
            raise ValueError("Line does not start with 'LI' record identifier.")
        return cls(
            record_identity=line[0:2].strip(),
            tiploc=line[2:10].strip(),
            scheduled_arrival=line[10:15].strip(),
            scheduled_departure=line[15:20].strip(),
            scheduled_pass=line[20:25].strip(),
            public_arrival=line[25:29].strip(),
            public_departure=line[29:33].strip(),
            platform=line[33:36].strip(),
            line=line[36:39].strip(),
            path=line[39:42].strip(),
            activity=line[42:54].strip(),
            engineering_allowance=line[54:56].strip(),
            pathing_allowance=line[56:58].strip(),
            performance_allowance=line[58:60].strip(),
        )


class LocationTermination(BaseModel):
    record_identity: str  # 	2	LT	Constant value 'LT'
    tiploc: str  # 	8	MLCHSTR	Timing Point Location code
    scheduled_arrival: str  # 	5	0000	Scheduled arrival time (HHMM)
    public_arrival: str  # 	4	0000	Public arrival time (HHMM)
    platform: str  # 	3	00	Platform number
    line: str  # 	3	00	Line code
    activity: str  # 	12

    @classmethod
    def from_cif_line(cls, line: str) -> LocationTermination:
        if not line.startswith("LT"):
            raise ValueError("Line does not start with 'LT' record identifier.")
        return cls(
            record_identity=line[0:2].strip(),
            tiploc=line[2:10].strip(),
            scheduled_arrival=line[10:15].strip(),
            public_arrival=line[15:19].strip(),
            platform=line[19:22].strip(),
            line=line[22:25].strip(),
            activity=line[25:37].strip(),
        )


class Header(BaseModel):
    record_identity: str = Field(default="HD")  # always "HD"
    file_mainframe_identity: str
    date_of_extract: str  # DDMMYY
    time_of_extract: str  # HHMM
    current_file_reference: str
    last_file_reference: str
    update_indicator: str
    version: str
    user_start_date: str  # DDMMYY
    user_end_date: str  # DDMMYY

    @classmethod
    def from_cif_line(cls, line: str) -> Header:
        if not line.startswith("HD"):
            raise ValueError("Line does not start with 'HD' header record identifier.")
        return cls(
            file_mainframe_identity=line[2:22].strip(),
            date_of_extract=line[22:28].strip(),
            time_of_extract=line[28:32].strip(),
            current_file_reference=line[32:39].strip(),
            last_file_reference=line[39:46].strip(),
            update_indicator=line[46:47].strip(),
            version=line[47:48].strip(),
            user_start_date=line[48:54].strip(),
            user_end_date=line[54:60].strip(),
            spare=line[60:].strip(),
        )


class TiplocInsert(BaseModel):
    record_identity: str  # 	2	TI	Always TI.
    tiploc: str  # 	7	BLTNODR	Timing Point Location Code.
    capitals_identification: int  # 	2	24	Not used, but may still contain historic data. Was to define capitalisation of TIPLOC.
    nlc: int  # 	6	853600	National Location Code.
    nlc_check_char: str  # 	1	D	NLC check character.
    tps_description: str  # 	26	BOLTON-UPON-DEARNE	Description of location.
    stanox: int  # 	5	24011	5 character TOPS location code.
    po_mcp_code: int  # 	4	0	Not used, but may still contain historic data. Was 4 character Post Office Location Code.
    crs_code: str  # 	3	BTD	CRS code.
    nlc_description: str  # 	16	BOLTON ON DEARNE	16 character description used in CAPRI.
    spare: str  # 	8

    @classmethod
    def from_cif_line(cls, line: str) -> TiplocInsert:
        if not line.startswith("TI"):
            raise ValueError("Line does not start with 'TI' record identifier.")
        return cls(
            record_identity=line[0:2].strip(),
            tiploc=line[2:9].strip(),
            capitals_identification=int(line[9:11].strip()),
            nlc=int(line[11:17].strip()),
            nlc_check_char=line[17:18].strip(),
            tps_description=line[18:44].strip(),
            stanox=int(line[44:49].strip()),
            po_mcp_code=int(line[49:53].strip()),
            crs_code=line[53:56].strip(),
            nlc_description=line[56:72].strip(),
            spare=line[72:].strip(),
        )


class TiplocAmend(BaseModel):
    record_identity: str  # 	2	TA	Always TA
    tiploc: str  # 	7	MBRK942	Timing Point Location
    capitals_identification: int  # 	2	00	Defines capitalisation of TIPLOC
    nlc: int  # 	6	590970	National Location Code
    nlc_check_char: str  # 	1	A
    tps_description: str  # 	26	MILLBROOK SIG E942	Description of location
    stanox: int  # 	5	86536
    po_mcp_code: int  # 	4	0
    crs_code: str  # 	3
    nlc_description: str  # 	16
    new_tiploc: str  # 	7		Only present if TIPLOC change
    spare: str  # 	1	(empty space)

    @classmethod
    def from_cif_line(cls, line: str) -> TiplocAmend:
        if not line.startswith("TA"):
            raise ValueError("Line does not start with 'TA' record identifier.")
        return cls(
            record_identity=line[0:2].strip(),
            tiploc=line[2:9].strip(),
            capitals_identification=int(line[9:11].strip()),
            nlc=int(line[11:17].strip()),
            nlc_check_char=line[17:18].strip(),
            tps_description=line[18:44].strip(),
            stanox=int(line[44:49].strip()),
            po_mcp_code=int(line[49:53].strip()),
            crs_code=line[53:56].strip(),
            nlc_description=line[56:72].strip(),
            new_tiploc=line[72:79].strip(),
            spare=line[79:].strip(),
        )


class TiplocDelete(BaseModel):
    record_identity: str  # 	2	TD	Constant value 'TD'
    tiploc: str  # 	7	MLCHSTR	Timing Point Location code
    spare: str  # 	71	(empty space)

    @classmethod
    def from_cif_line(cls, line: str) -> TiplocDelete:
        if not line.startswith("TD"):
            raise ValueError("Line does not start with 'TD' record identifier.")
        return cls(
            record_identity=line[0:2].strip(),
            tiploc=line[2:9].strip(),
            spare=line[9:].strip(),
        )
