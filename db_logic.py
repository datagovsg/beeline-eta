import os
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from os.path import join, dirname
from utility import flatten

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
def query(select_str, from_str, where_str='', order_by_str='', column_names=[]):
    cursor.execute('{} {} {} {}'.format(select_str, from_str, where_str, order_by_str))
    records = cursor.fetchall()
    if len(column_names) == 0 or len(records) == 0 or len(records[0]) != len(column_names):
        return pd.DataFrame()
    return to_pandas(column_names, records)

def get_routes():
    return query('SELECT id, path',
                 'FROM public.routes',
                 column_names=['id', 'path'])

def get_trips():
    return query('SELECT id, date, "routeId"',
                 'FROM public.trips',
                 column_names=['id', 'date', 'routeId'])

def get_tripstops(trip_id=None):
    where_str_part = 'AND "tripId" = {}'.format(trip_id) if trip_id else ''
    return query('SELECT ts.id, "tripId", "stopId", "canBoard", "canAlight", time, ' +
                         'ST_X(s.coordinates), ST_Y(s.coordinates)',
                 'FROM public.stops AS s INNER JOIN "tripStops" AS ts ON s.id = "stopId"',
                 where_str='WHERE "canBoard" = True {}'.format(where_str_part),
                 order_by_str="ORDER BY time",
                 column_names=['id', 'tripId', 'stopId', 'canBoard', 'canAlight', 'time', 'lng', 'lat'])

def get_pings(trip_id=None, recent=False):
    where_str_part_1 = '"tripId" = {}'.format(trip_id) if trip_id else ''
    where_str_part_2 = "time >= NOW() - INTERVAL '3 minutes'" if recent else ''
    where_str = ''
    if trip_id and recent:
        where_str = 'WHERE {} AND {}'.format(where_str_part_1, where_str_part_2)
    elif trip_id:
        where_str = 'WHERE {}'.format(where_str_part_1)
    elif recent:
        where_str = 'WHERE {}'.format(where_str_part_2)
    return query('SELECT id, ST_X(coordinates), ST_Y(coordinates), time, "tripId"',
                 'FROM public.pings',
                 where_str=where_str,
                 order_by_str="ORDER BY time",
                 column_names=['id', 'lng', 'lat', 'time', 'tripId'])

def get_operating_trip_ids(date_time=datetime.now()):
    sql = 'SELECT "tripId" ' \
        + 'FROM "tripStops" ' \
        + 'GROUP BY "tripId" ' \
        + "HAVING (%s) >= MIN(time) - INTERVAL '15 minutes'" \
        + "   AND (%s) <= MAX(time) + INTERVAL '15 minutes'"
    cursor.execute(sql, (date_time, date_time))
    return flatten(cursor.fetchall())

# Sanity checks:
# Singapore Latitude (Y): 1.29, Longitude (X): 103.85
# routes = get_routes()
# trips = get_trips()
# tripstops = get_tripstops()
# pings = get_pings()
# print(get_operating_trip_ids())
