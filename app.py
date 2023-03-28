from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import desc
from datetime import datetime
from typing import List, Literal, Tuple, TypedDict
import csv
import pytz

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


# type class for typing the return value of the following function
class InitialVars(TypedDict):
    local_tz: pytz.BaseTzInfo
    last_day_ts: datetime
    business_hours: List[Tuple[datetime, datetime]]
    store_status: List[StoreStatus]


# helper function to initialise the getters below
def get_initial_vars(store_id: str) -> InitialVars:
    # get timestamps in descending order to get the last day records
    store_status: List[StoreStatus] = (
        StoreStatus.query.filter((StoreStatus.store_id == store_id))
        .order_by(desc("timestamp"))
        .all()
    )

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


# get up and down times for the last hour
def get_uptime_last_hour(store_id: str) -> Tuple[int, int]:
    print(f"Getting uptime (last hour) for store with id {store_id}")

    # get initial variables for this store
    initial_vars = get_initial_vars(store_id)
    store_status = initial_vars["store_status"]
    local_tz = initial_vars["local_tz"]
    business_hours = initial_vars["business_hours"]

    uptime = downtime = 0  # in minutes

    # calculate for the last business hour
    did_hit_last_hour = False

    print("Beginning uptime count")
    for status in store_status:
        if did_hit_last_hour:
            break

        status_dt = status.timestamp.astimezone(local_tz)
        print(
            f"Current poll: {status_dt.strftime(timestamp_format)}, status:",
            status.status,
        )

        for hours in business_hours:
            # if the poll was made outside the business hours of the store, ignore it
            poll_time = status_dt.time()
            # less than the start_time or greater than the end_time
            if poll_time < hours[0].time() or poll_time > hours[1].time():
                continue

            # Assumption: Polls are made every hour, so taking every poll as one whole hour
            if status.status == "active":
                print("Adding to uptime")
                uptime += 60
            else:
                print("Adding to downtime")
                downtime += 60

            # if reached here then the last hour has been hit
            did_hit_last_hour = True

    print(f"Total uptime: {uptime}, total downtime:", downtime)
    return (uptime, downtime)


# get up and down times for the last day
def get_uptime_last_day(store_id: str) -> Tuple[int, int]:
    print(f"Getting uptime (last day) for store with id {store_id}")

    # get initial variables for this store
    initial_vars = get_initial_vars(store_id)
    store_status = initial_vars["store_status"]
    local_tz = initial_vars["local_tz"]
    last_day_ts = initial_vars["last_day_ts"]
    business_hours = initial_vars["business_hours"]

    uptime = downtime = 0  # in hours

    print("Beginning uptime count")
    for status in store_status:
        status_dt = status.timestamp.astimezone(local_tz)
        print(
            f"Current poll: {status_dt.strftime(timestamp_format)}, status:",
            status.status,
        )

        if status_dt.date() != last_day_ts.date():
            print("Passed last day")
            break  # passed the last day if the code reached here

        for hours in business_hours:
            # if the poll was made outside the business hours of the store, ignore it
            poll_time = status_dt.time()
            # less than the start_time or greater than the end_time
            if poll_time < hours[0].time() or poll_time > hours[1].time():
                continue

            # Assumption: Polls are made every hour, so taking every poll as one whole hour
            if status.status == "active":
                print("Adding to uptime")
                uptime += 1
            else:
                print("Adding to downtime")
                downtime += 1

    print(f"Total uptime: {uptime}, total downtime:", downtime)
    # (uptime, downtime)
    return (uptime, downtime)


# get up and down times for the last week
def get_uptime_last_week(store_id: str) -> Tuple[int, int]:
    print(f"Getting uptime (last week) for store with id {store_id}")
    # get initial variables for this store
    initial_vars = get_initial_vars(store_id)
    store_status = initial_vars["store_status"]
    local_tz = initial_vars["local_tz"]
    last_day_ts = initial_vars["last_day_ts"]
    business_hours = initial_vars["business_hours"]

    uptime = downtime = 0  # in hours

    # calculate for the last week
    print("Beginning uptime count")
    for status in store_status:
        status_dt = status.timestamp.astimezone(local_tz)
        print(
            f"Current poll: {status_dt.strftime(timestamp_format)}, status:",
            status.status,
        )

        # if completed the count for the whole week, break
        if (
            status_dt.date() != last_day_ts.date()
            and status_dt.weekday() == last_day_ts.weekday()
        ):
            # if reached the same weekday on a different date, then completed calculating for an entire week
            print("Passed last week")
            break

        for hours in business_hours:
            # if the poll was made outside the business hours of the store, ignore it
            poll_time = status_dt.time()
            # less than the start_time or greater than the end_time
            if poll_time < hours[0].time() or poll_time > hours[1].time():
                continue

            # Assumption: Polls are made every hour, so taking every poll as one whole hour
            if status.status == "active":
                print("Adding to uptime")
                uptime += 1
            else:
                print("Adding to downtime")
                downtime += 1

    print(f"Total uptime: {uptime}, total downtime:", downtime)
    return (uptime, downtime)


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


# test
@app.route("/test")
def test_route():
    return ""


## RUN
if __name__ == "__main__":
    app.run(debug=True)
