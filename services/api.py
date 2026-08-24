"""FastAPI service exposing live train status + watchlist management.

Reads/writes Postgres directly - deliberately has no shared state with the
live ingest service (main.py), since they run as independent processes.
Run with: uv run uvicorn services.api:app --reload
"""

import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.database import Database

load_dotenv()

app = FastAPI(title="Train Status API")

_db: Optional[Database] = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(os.environ["DB_PATH"])
    return _db


class StationWatchIn(BaseModel):
    tiploc: str
    destination_tiploc: Optional[str] = None


class TrainWatchIn(BaseModel):
    uid: str
    origin_tiploc: str


@app.get("/stations/search")
def search_stations(q: str) -> list[dict]:
    return get_db().search_stations(q)


@app.get("/watchlist/stations")
def list_station_watches() -> list[dict]:
    return get_db().list_watchlist_stations()


@app.post("/watchlist/stations", status_code=201)
def add_station_watch(body: StationWatchIn) -> dict:
    db = get_db()
    try:
        watch_id = db.add_watchlist_station(body.tiploc, body.destination_tiploc)
    except psycopg2.Error:
        db.conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Unknown tiploc/destination_tiploc - check /stations/search",
        )
    return {"id": watch_id}


@app.delete("/watchlist/stations/{watch_id}", status_code=204)
def delete_station_watch(watch_id: int) -> None:
    get_db().remove_watchlist_station(watch_id)


@app.get("/watchlist/trains")
def list_train_watches() -> list[dict]:
    return get_db().list_watchlist_trains()


@app.post("/watchlist/trains", status_code=201)
def add_train_watch(body: TrainWatchIn) -> dict:
    db = get_db()
    try:
        watch_id = db.add_watchlist_train(body.uid, body.origin_tiploc)
    except psycopg2.Error:
        db.conn.rollback()
        raise HTTPException(
            status_code=400,
            detail="Unknown origin_tiploc - check /stations/search",
        )
    return {"id": watch_id}


@app.delete("/watchlist/trains/{watch_id}", status_code=204)
def delete_train_watch(watch_id: int) -> None:
    get_db().remove_watchlist_train(watch_id)


@app.get("/status")
def status() -> dict:
    db = get_db()

    station_boards = []
    for watch in db.list_watchlist_stations():
        departures = db.get_upcoming_departures(
            tiploc=watch["tiploc"], destination_tiploc=watch["destination_tiploc"]
        )
        station_boards.append(
            {
                "tiploc": watch["tiploc"],
                "destination_tiploc": watch["destination_tiploc"],
                "departures": departures,
            }
        )

    pinned_trains = []
    for watch in db.list_watchlist_trains():
        train_status = db.get_train_status(
            uid=watch["uid"], origin_tiploc=watch["origin_tiploc"]
        )
        if train_status is not None:
            pinned_trains.append(train_status)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stations": station_boards,
        "trains": pinned_trains,
    }
