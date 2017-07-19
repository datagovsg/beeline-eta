import glob
import os
import pandas as pd
import pickle
import psycopg2
import pytz
from constants import (
    COLUMN_NAMES_PINGS, COLUMN_NAMES_ROUTES, COLUMN_NAMES_STOPS,
    COLUMN_NAMES_TRIPS, COLUMN_NAMES_TRIPSTOPS
)
from datetime import datetime, timedelta
from os.path import join, dirname
from utility import flatten

DATABASE_URI = os.environ.get("DATABASE_URL")

# Helper function to convert results from SQL SELECT query to pandas dataframe
def to_pandas(column_names, records):
    table = pd.DataFrame(records)
    table.columns = column_names
    return table

# Helper function to do SQL SELECT query
def query(sql, data=(), column_names=[], pandas_format=True):
    conn = psycopg2.connect(DATABASE_URI)
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Singapore';")
    cursor.execute(sql, data)
    records = cursor.fetchall()
    # This returns a list of results (where results is represented as a tuple)
    if not pandas_format:
        return records
    # For empty records, we still want to preserve the relevant column names
    if len(records) == 0:
        return pd.DataFrame(columns=column_names)
    # For error cases with no column names, or mismatch of no. of columns
    if len(column_names) == 0 or len(records[0]) != len(column_names):
        return pd.DataFrame()
    return to_pandas(column_names, records).set_index('id')

def get_routes():
    sql = """
          SELECT
              id, path
          FROM
              routes
          """
    records = query(sql, column_names=COLUMN_NAMES_ROUTES)
    return records

def get_trips(trip_id=None):
    sql = """
          SELECT
              id, date, "routeId"
          FROM
              trips
          {}
          """ \
          .format('' if trip_id == None else 'WHERE id = %(trip_id)s')
    data = {'trip_id': 0 if trip_id == None else int(trip_id)}
    records = query(sql, data=data, column_names=COLUMN_NAMES_TRIPS)
    return records

def get_stops(stop_id=None):
    sql = """
          SELECT
              id, ST_X(coordinates), ST_Y(coordinates)
          FROM
              stops
          {}
          """ \
          .format('' if stop_id == None else 'WHERE id = %(stop_id)s')
    data = {'stop_id': 0 if stop_id == None else int(stop_id)}
    records = query(sql, data=data, column_names=COLUMN_NAMES_STOPS)
    return records

def get_tripstops(tripstop_id=None, trip_id=None):
    sql = """
          SELECT
              ts.id, "tripId", "stopId", "canBoard", "canAlight", time,
              ST_X(s.coordinates), ST_Y(s.coordinates)
          FROM
              stops AS s
              INNER JOIN "tripStops" AS ts ON s.id = "stopId"
          WHERE
              "canBoard" = %(can_board)s {} {}
          ORDER BY
              time
          """ \
          .format('' if tripstop_id == None else 'AND ts.id = %(tripstop_id)s',
                  '' if trip_id == None else 'AND "tripId" = %(trip_id)s')
    data = {'tripstop_id': 0 if tripstop_id == None else int(tripstop_id),
            'trip_id': 0 if trip_id == None else int(trip_id),
            'can_board': True}
    records = query(sql,
                    data=data,
                    column_names=COLUMN_NAMES_TRIPSTOPS)
    return records


def get_pings(ping_id=None, trip_id=None, newest_datetime=datetime.now()):
    sql = """
          SELECT
              id, ST_X(coordinates), ST_Y(coordinates), time, "tripId"
          FROM
              pings
          WHERE
              time <= %(newest_datetime)s {} {}
          ORDER BY
              time
          """ \
          .format('' if ping_id == None else 'AND id = %(ping_id)s',
                  '' if trip_id == None else 'AND "tripId" = %(trip_id)s')
    data = {'ping_id': 0 if ping_id == None else int(ping_id),
            'trip_id': 0 if trip_id == None else int(trip_id),
            'newest_datetime': newest_datetime}
    records = query(sql,
                    data=data,
                    column_names=COLUMN_NAMES_PINGS)
    return records

def get_past_trips_of_route(route_id, before_date=datetime.now()):
    after_date = before_date - timedelta(days=30)
    sql = """
          SELECT
              id, date, "routeId"
          FROM
              trips
          WHERE
              "routeId" = %(route_id)s
              AND date > %(after_date)s
              AND date < %(before_date)s
          ORDER BY
              date DESC
          """
    data = {'route_id': int(route_id),
            'after_date': after_date,
            'before_date': before_date}
    records = query(sql, data=data, column_names=COLUMN_NAMES_TRIPS)
    return records

def get_operating_trip_ids(date_time=datetime.now()):
    sql = """
          SELECT
              "tripId"
          FROM
              "tripStops"
          GROUP BY
              "tripId"
          HAVING
              %(date_time)s >= MIN(time) - INTERVAL '15 minutes'
              AND %(date_time)s <= MAX(time) + INTERVAL '15 minutes'
          """
    data = {'date_time': date_time}
    records = query(sql, data=data, column_names=['tripId'], pandas_format=False)
    return flatten(records)

def get_offset(minutes=20):
    sql = """
          SELECT
              MAX(time)
          FROM
              pings
          """
    records = query(sql, column_names=['time'], pandas_format=False)
    latest_known_datetime = records[0][0]
    time_diff = datetime.now(pytz.timezone('Singapore')) - latest_known_datetime
    return time_diff + timedelta(minutes=minutes)

def destroy_predictions():
    files = glob.glob('results/*')
    for f in files:
        if f.endswith('gitkeep'):
          continue
        os.remove(f)
