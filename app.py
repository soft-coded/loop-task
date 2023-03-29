from flask import Flask, jsonify, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import desc
from datetime import datetime
from typing import List, Literal, Tuple, TypedDict, Union
from threading import Thread
import random
import string
import csv
import pytz
import time

## FLASK APP
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///loop_app.db"

## DATABASE
db = SQLAlchemy()
db.init_app(app)

# GLOBALS
time_format = "%H:%M:%S"
timestamp_format = "%Y-%m-%d %H:%M:%S.%f %Z"
WeekdayType = Literal[0, 1, 2, 3, 4, 5, 6]
StatusType = Literal["active", "inactive"]


## MODELS
# store hours model (during what time the store is operable)
class StoreHours(db.Model):
    id: int = db.Column(db.Integer, primary_key=True)
    store_id: str = db.Column(db.String)
    day_of_week: WeekdayType = db.Column(db.Integer)
    start_time: datetime = db.Column(db.DateTime)
    end_time: datetime = db.Column(db.DateTime)


# store status model (whether the store is active or inactive)
class StoreStatus(db.Model):
    id: int = db.Column(db.Integer, primary_key=True)
    store_id: str = db.Column(db.String)
    status: StatusType = db.Column(db.String)
    timestamp: datetime = db.Column(db.DateTime)


# timezone model (store's local timezone)
class Timezone(db.Model):
    id: int = db.Column(db.Integer, primary_key=True)
    store_id: str = db.Column(db.String)
    timezone: str = db.Column(db.String)


# reports model for keeping track of reports
class Report(db.Model):
    report_id: str = db.Column(db.String, primary_key=True)
    status: Literal["Running", "Completed"] = db.Column(db.String, default="Running")
    # time taken to generate the report (in minutes)
    time_taken: float = db.Column(db.Float)


## HELPERS
# get the operating business hours of a store on a particular weekday
def get_store_time(store_id: str, day_of_week: int) -> List[Tuple[datetime, datetime]]:
    store_data: List[StoreHours] = (
        StoreHours.query.filter(
            (StoreHours.store_id == store_id) & (StoreHours.day_of_week == day_of_week)
        )
        .order_by("start_time", "end_time")
        .all()
    )

    times = []
    for store in store_data:
        times.append((store.start_time, store.end_time))

    # Assumption: If no data was found, assume the store to be open 24*7
    if not times:
        times.append((datetime(1900, 1, 1, 0, 0, 0), datetime(1900, 1, 1, 23, 59, 59)))

    return times


# convert a formatted string to a datetime object
def get_datetime_from_ts(timestamp: str, only_time=False):
    # time format: 12:24:54
    if only_time:
        return datetime.strptime(timestamp, time_format)

    # timestamp format: 2023-01-25 11:09:27.334577 UTC
    try:
        return datetime.strptime(timestamp, timestamp_format)
    except ValueError:
        # if timestamp has no milliseconds part
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z")


# get local timezone of a store
def get_local_tz(store_id: str):
    local_tz = Timezone.query.filter(Timezone.store_id == store_id).first().timezone
    local_tz = pytz.timezone(local_tz)
    return local_tz


# type class for typing the return type of the following function
class InitialVars(TypedDict):
    local_tz: pytz.BaseTzInfo
    last_day_ts: datetime
    business_hours: List[Tuple[datetime, datetime]]
    store_status: List[StoreStatus]


# helper function to initialise the getters below
def get_initial_vars(store_id: str) -> Union[InitialVars, None]:
    # get timestamps in descending order to get the last day records
    store_status: List[StoreStatus] = (
        StoreStatus.query.filter((StoreStatus.store_id == store_id))
        .order_by(desc("timestamp"))
        .all()
    )

    # if no polls are available for a store, return None
    if not store_status:
        return None

    # first entry is the last day, last time in UTC
    local_tz = get_local_tz(store_id)
    last_day_ts = store_status[0].timestamp.astimezone(local_tz)
    print(
        f"Last poll for store: {last_day_ts.strftime(timestamp_format)}, weekday:",
        last_day_ts.weekday(),
    )

    # get the store's business hours for this weekday
    business_hours = get_store_time(store_id, last_day_ts.weekday())
    print("Store's business hours on this weekday:")

    for hours in business_hours:
        print(hours[0].strftime(time_format), "to", hours[1].strftime(time_format))

    return {
        "business_hours": business_hours,
        "last_day_ts": last_day_ts,
        "store_status": store_status,
        "local_tz": local_tz,
    }


# type class for the return type of the following function
class AllTimes(TypedDict):
    up_weekly: int  # in hours
    down_weekly: int  # in hours
    up_daily: int  # in hours
    down_daily: int  # in hours
    up_hourly: int  # in minutes
    down_hourly: int  # in minutes


# get up and down times for a store
def get_times(store_id: str) -> AllTimes:
    print(f"\nCalculating uptime for store with id {store_id}")

    times: AllTimes = {
        "up_weekly": 0,  # in hours
        "down_weekly": 0,  # in hours
        "up_daily": 0,  # in hours
        "down_daily": 0,  # in hours
        "up_hourly": 0,  # in minutes
        "down_hourly": 0,  # in minutes
    }
    # get initial variables for this store
    initial_vars = get_initial_vars(store_id)
    # if no polls are available for a store, return
    if initial_vars is None:
        return times

    store_status = initial_vars["store_status"]
    local_tz = initial_vars["local_tz"]
    last_day_ts = initial_vars["last_day_ts"]
    business_hours = initial_vars["business_hours"]

    last_day_date = last_day_ts.date()
    did_hit_last_hour = False
    did_hit_last_day = False
    did_hit_last_week = False

    print("Beginning uptime count")
    for status in store_status:
        status_dt = status.timestamp.astimezone(local_tz)
        print(
            f"Current poll: {status_dt.strftime(timestamp_format)}, status:",
            status.status,
        )

        if not did_hit_last_day and status_dt.date() != last_day_date:
            # passed the last day if the code reached here
            print("Passed last day")
            did_hit_last_day = True

        if not did_hit_last_week and (
            status_dt.date() != last_day_date
            and status_dt.weekday() == last_day_ts.weekday()
        ):
            # if reached the same weekday on a different date, then completed calculating for an entire week
            print("Passed last week")
            did_hit_last_week = True

        # if hit all three time intervals, done for this store
        if did_hit_last_week and did_hit_last_day and did_hit_last_hour:
            print(f"Done calculating for store with id {store_id}")
            break

        business_hours = get_store_time(store_id, status_dt.weekday())
        for hours in business_hours:
            poll_time = status_dt.time()
            # if the poll was made outside the business hours of the store, ignore it
            # less than the start_time or greater than the end_time
            if poll_time < hours[0].time() or poll_time > hours[1].time():
                continue

            # Assumption: Polls are made every hour, so taking every poll as one whole hour
            if status.status == "active":
                print("Adding to uptime")

                if not did_hit_last_hour:
                    times["up_hourly"] += 60  # in minutes
                if not did_hit_last_day:
                    times["up_daily"] += 1
                if not did_hit_last_week:
                    times["up_weekly"] += 1
            else:
                print("Adding to downtime")

                if not did_hit_last_hour:
                    times["down_hourly"] += 60  # in minutes
                if not did_hit_last_day:
                    times["down_daily"] += 1
                if not did_hit_last_week:
                    times["down_weekly"] += 1

            # if reached here then the last hour has been hit
            if not did_hit_last_hour:
                print("Passed last hour")
                did_hit_last_hour = True

    print("Final values:", times, "\n")
    return times


# generate the csv file
def generate_report(report_id: str):
    with app.app_context():
        st = time.time()
        all_stores: List[Timezone] = []

        all_stores = Timezone.query.order_by("store_id").limit(100).all()

        with open(f"./reports/{report_id}.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "store_id",
                    "uptime_last_hour(in minutes)",
                    "uptime_last_day(in hours)",
                    "uptime_last_week(in hours)",
                    "downtime_last_hour(in minutes)",
                    "downtime_last_day(in hours)",
                    "downtime_last_week(in hours)",
                ]
            )

            for store in all_stores:
                times = get_times(store.store_id)
                writer.writerow(
                    [
                        store.store_id,
                        times["up_hourly"],
                        times["up_daily"],
                        times["up_weekly"],
                        times["down_hourly"],
                        times["down_daily"],
                        times["down_weekly"],
                    ]
                )

        time_taken = (time.time() - st) / 60  # in minutes

        report: Report = Report.query.filter(Report.report_id == report_id).first()
        report.status = "Completed"
        report.time_taken = time_taken
        db.session.commit()

        print("\nTime taken:", time_taken)


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
                start_time=get_datetime_from_ts(line[2], True),
                end_time=get_datetime_from_ts(line[3], True),
            )
            db.session.add(store)

    with open("store_status.csv", mode="r") as store_status_file:
        file = list(csv.reader(store_status_file))[1:]

        for line in file:
            store = StoreStatus(
                store_id=line[0],
                status=line[1],
                timestamp=get_datetime_from_ts(line[2]),
            )
            db.session.add(store)

    with open("timezone.csv", mode="r") as timezone_file:
        file = list(csv.reader(timezone_file))[1:]

        for line in file:
            store = Timezone(store_id=line[0], timezone=line[1] or "America/Chicago")
            db.session.add(store)

    db.session.commit()

    return "Done"


@app.route("/trigger_report")
def trigger_report():
    # random string of 10 characters, to be used as an id and filename
    report_id = "".join(random.choices(string.ascii_letters + string.digits, k=10))

    # add report_id to database
    report = Report(report_id=report_id)
    db.session.add(report)
    db.session.commit()

    # generate the report on a separate thread
    Thread(target=generate_report, kwargs={"report_id": report_id}).start()

    return jsonify({"report_id": report_id})


@app.route("/get_report/<report_id>")
def get_report(report_id: str):
    report: Report = Report.query.filter(Report.report_id == report_id).first()

    if report.status == "Running":
        return jsonify({"status": "Running"})

    return send_file(f"./reports/{report_id}.csv")


## RUN
if __name__ == "__main__":
    app.run(debug=True)
