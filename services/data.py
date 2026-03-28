from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Union


class TrainLocation(BaseModel):
    # Mapping the short Darwin keys to readable names
    tiploc: str = Field(alias="tpl")
    planned_arr: Optional[str] = Field(alias="pta", default=None)
    planned_dep: Optional[str] = Field(alias="ptd", default=None)

    # Reality check: actual times (at) take priority over estimated (et)
    est_arr: Optional[str] = Field(alias="et", default=None)
    act_arr: Optional[str] = Field(alias="at", default=None)

    platform: Optional[str] = Field(alias="plat", default="?")
    source: Optional[str] = Field(alias="src", default="Unknown")

    @property
    def current_status(self) -> str:
        if self.act_arr:
            return f"Arrived at {self.act_arr}"
        if self.est_arr:
            return f"Expected at {self.est_arr}"
        return "On time"


class TrainUpdate(BaseModel):
    rid: str
    uid: str
    # Handling the 'Location is sometimes a list, sometimes a dict' issue
    locations: List[TrainLocation] = Field(alias="Location")

    @field_validator("locations", mode="before")
    @classmethod
    def ensure_list(cls, v):
        if isinstance(v, dict):
            return [v]
        return v
