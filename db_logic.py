import global_data
import os
import pandas as pd
import pickle
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from os.path import join, dirname
from utility import flatten

DATA_FILENAME_ROUTES = 'preprocessed/routes.pkl'
DATA_FILENAME_TRIPS = 'preprocessed/trips.pkl'
DATA_FILENAME_TRIPSTOPS = 'preprocessed/tripstops.pkl'
DATA_FILENAME_PINGS = 'preprocessed/pings.pkl'

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

DATABASE_URI = os.environ.get("DATABASE_URI")

conn = psycopg2.connect(DATABASE_URI)
cursor = conn.cursor()
cursor.execute("SET TIME ZONE 'Singapore';")

# Helper function to convert results from SQL SELECT query to pandas dataframe
def to_pandas(column_names, records):
    table = pd.DataFrame(records)
    table.columns = column_names
    return table

# Helper function to do SQL SELECT query
def query(sql, data=(), column_names=[]):
    cursor.execute(sql, data)
    records = cursor.fetchall()
    if len(column_names) == 0 or len(records) == 0 or len(records[0]) != len(column_names):
        return pd.DataFrame()
    return to_pandas(column_names, records).set_index('id')

def get_routes():
    try:
        return pd.read_pickle(DATA_FILENAME_ROUTES)
    except:
        sql = """
              SELECT
                  id, path
              FROM
                  routes
              """
        records = query(sql, column_names=['id', 'path'])
        records.to_pickle(DATA_FILENAME_ROUTES)
        return records

def get_trips():
    try:
        return pd.read_pickle(DATA_FILENAME_TRIPS)
    except:
        sql = """
              SELECT
                  id, date, "routeId"
              FROM
                  trips
              """
        records = query(sql, column_names=['id', 'date', 'routeId'])
        records.to_pickle(DATA_FILENAME_TRIPS)
        return records

def get_tripstops(trip_id=None):
    try:
        tripstops = pd.read_pickle(DATA_FILENAME_TRIPSTOPS)
        return tripstops if trip_id == None else tripstops[tripstops['tripId'] == trip_id]
    except:
        sql = """
              SELECT
                  ts.id, "tripId", "stopId", "canBoard", "canAlight", time, ST_X(s.coordinates), ST_Y(s.coordinates)
              FROM
                  stops AS s
                  INNER JOIN "tripStops" AS ts ON s.id = "stopId"
              WHERE
                  "canBoard" = %(can_board)s {}
              ORDER BY
                  time
              """ \
              .format('' if trip_id == None else 'AND "tripId" = %(trip_id)s')
        data = {'trip_id': trip_id,
                'can_board': True}
        records = query(sql,
                        data=data,
                        column_names=['id', 'tripId', 'stopId', 'canBoard', 'canAlight', 'time', 'lng', 'lat'])

        if trip_id == None:
            records.to_pickle(DATA_FILENAME_TRIPSTOPS)
        return records


def get_pings(trip_id=None, newest_datetime=datetime.now()):
    try:
        pings = pd.read_pickle(DATA_FILENAME_PINGS)
        if trip_id == None:
            return pings
        return pings[(pings['tripId'] == trip_id) & (pings['time'] <= newest_datetime)]
    except:
        sql = """
              SELECT
                  id, ST_X(coordinates), ST_Y(coordinates), time, "tripId"
              FROM
                  pings
              WHERE
                  time <= %(newest_datetime)s {}
              ORDER BY
                  time
              """ \
              .format('' if trip_id == None else 'AND "tripId" = %(trip_id)s')
        data = {'trip_id': trip_id,
                'newest_datetime': newest_datetime}
        records = query(sql,
                        data=data,
                        column_names=['id', 'lng', 'lat', 'time', 'tripId'])

        if trip_id == None or datetime.now() - newest_datetime < timedelta(hours=1):
            records.to_pickle(DATA_FILENAME_PINGS)
        return records

def get_recent_pings(current_datetime=datetime.now(), interval_minutes=2):
    sql = """
          SELECT
              id, ST_X(coordinates), ST_Y(coordinates), time, "tripId"
          FROM
              pings
          WHERE
              time >= %(current_datetime)s - INTERVAL '%(interval_minutes)s minutes'
              AND time <= %(current_datetime)s
          ORDER BY
              time
          """
    data = {'current_datetime': current_datetime,
            'interval_minutes': interval_minutes}
    records = query(sql,
                    data=data,
                    column_names=['id', 'lng', 'lat', 'time', 'tripId'])
    return records

def get_operating_trip_ids(date_time=datetime.now()):
    sql = """
          SELECT "tripId"
          FROM "tripStops"
          GROUP BY "tripId"
          HAVING %(date_time)s >= MIN(time) - INTERVAL '15 minutes'
             AND %(date_time)s <= MAX(time) + INTERVAL '15 minutes'
          """
    cursor.execute(sql, {'date_time': date_time})
    return flatten(cursor.fetchall())

def setup_data():
    global_data.routes = get_routes()
    global_data.trips = get_trips()
    global_data.tripstops = get_tripstops()
    global_data.pings = get_pings()

# For routes, trips, tripstops, this function will be called every day
# We first destroy the 3 files, then we regenerate re-get all the routes, trips and tripstops
def destroy_file(filename):
    os.remove(filename)

def destroy_and_recreate():
    destroy_and_recreate_routes()
    destroy_and_recreate_trips()
    destroy_and_recreate_tripstops()

def destroy_and_recreate_routes():
    # Destroy file first so the get operation will always go to exception clause.
    destroy_file(DATA_FILENAME_ROUTES)
    global_data.routes = get_routes()
    return routes

def destroy_and_recreate_trips():
    # Destroy file first so the get operation will always go to exception clause.
    destroy_file(DATA_FILENAME_TRIPS)
    global_data.trips = get_trips()
    return trips

def destroy_and_recreate_tripstops():
    # Destroy file first so the get operation will always go to exception clause.
    destroy_file(DATA_FILENAME_TRIPSTOPS)
    global_data.tripstops = get_tripstops()
    return tripstops


# For pings, this function will be called every minute
# We will take the past 3 mins pings and attempt to add it to the existing pings
def add_row(df, row):
    # Assume key is at the first column (row[0])
    df.loc[row[0]] = row[1:]

def update_df(df1, df2):
    for row in df2.itertuples():
        add_row(df1, row)

def update_pings(current_datetime=datetime.now()):
    new_pings = get_recent_pings(current_datetime=current_datetime)
    update_df(global_data.pings, new_pings)
    global_data.pings.to_pickle(DATA_FILENAME_PINGS)
