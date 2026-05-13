"""Models for train updates and locations from Kafka messages."""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Union
import logging

logger = logging.getLogger(__name__)


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
