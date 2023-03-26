from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from datetime import date, datetime
import csv
import pytz
from typing import Tuple

## FLASK APP
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///loop_app.db"

## DATABASE
db = SQLAlchemy()
db.init_app(app)


## MODELS
# store hours model (during what time the store is operable)
class StoreHours(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String)
    day_of_week = db.Column(db.Integer)
    start_time = db.Column(db.String)
    end_time = db.Column(db.String)

    def __repr__(self) -> str:
        return f"StoreHoursObject{{store_id: {self.store_id}, day_of_week: {self.day_of_week}, start_time: {self.start_time}, end_time: {self.end_time}}}"


# store status model (whether the store is active or inactive)
class StoreStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String)
    status = db.Column(db.String)
    timestamp = db.Column(db.String)

    def __repr__(self) -> str:
        return f"StoreStatusObject{{store_id: {self.store_id}, status: {self.status}, timestamp: {self.timestamp}}}"


# timezone model (store's local timezone)
class Timezone(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String)
    timezone = db.Column(db.String)

    def __repr__(self) -> str:
        return f"TimezoneObject{{store_id: {self.store_id}, timezone: {self.timezone}}}"


## HELPERS
def get_store_time(store_id: str, day_of_week: int) -> str:
    store_data = (
        StoreHours.query.filter(
            (StoreHours.store_id == store_id) & (StoreHours.day_of_week == day_of_week)
        )
        .order_by("start_time", "end_time")
        .all()
    )

    times = []
    for store in store_data:
        times.append((store.start_time, store.end_time))

    return times


def get_datetime_from_ts(timestamp: str) -> datetime:
    # timestamp format: 2023-01-25 11:09:27.334577 UTC
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f %Z")


def get_time_in_local_tz(store_id: str, timestamp: str):
    dt = get_datetime_from_ts(timestamp)

    local_tz = Timezone.query.filter(Timezone.store_id == store_id).first().timezone
    dt = dt.astimezone(pytz.timezone(local_tz or "America/Chicago"))

    return dt


## ROUTES
# index route, to check whether the server is running or not
@app.route("/")
def index_route():
    return "Server running"


# add csv data to db (temporary route)
@app.route("/add_data_to_db")
def add_data_to_db():
    with open("store_hours.csv", mode="r") as store_hours_file:
        file = list(csv.reader(store_hours_file))[1:]

        for line in file:
            store = StoreHours(
                store_id=line[0],
                day_of_week=line[1],
                start_time=line[2],
                end_time=line[3],
            )
            db.session.add(store)

    with open("store_status.csv", mode="r") as store_status_file:
        file = list(csv.reader(store_status_file))[1:]

        for line in file:
            store = StoreStatus(store_id=line[0], status=line[1], timestamp=line[2])
            db.session.add(store)

    with open("timezone.csv", mode="r") as timezone_file:
        file = list(csv.reader(timezone_file))[1:]

        for line in file:
            store = Timezone(store_id=line[0], timezone=line[1] or "America/Chicago")
            db.session.add(store)

    db.session.commit()

    return "Done"


# test
@app.route("/test")
def test_route():
    store_id = "3068127015204330700"
    # store_data = get_store_time(store_id, 0)
    # print(store_data)

    store_status = (
        StoreStatus.query.filter((StoreStatus.store_id == store_id))
        .order_by("timestamp")
        .all()
    )
    # print(store_status)
    for item in store_status:
        local_dt = get_time_in_local_tz(store_id, item.timestamp)
        store_time = get_store_time(store_id, local_dt.weekday())
        print(local_dt.strftime("%H:%M:%S.%f"), item.status, store_time)

    return "Check console"


## RUN
if __name__ == "__main__":
    app.run(debug=True)
