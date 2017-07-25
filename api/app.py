#!flask/bin/python
from flask import Flask, abort, jsonify
from flask_cors import CORS, cross_origin

from datetime import datetime
import glob
import pickle
import psycopg2

import boto
import boto.s3.connection
import os
from boto.s3.key import Key

app = Flask(__name__)
CORS(app)

get_filename = lambda trip_id: 'results/prediction-{}.pickle'.format(trip_id)

"""
This section handles Bucketeer and reading of saved files.
"""
AWS_ACCESS_KEY_ID = os.environ.get("BUCKETEER_AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("BUCKETEER_AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.environ.get("BUCKETEER_BUCKET_NAME")

def get_connection_and_bucket():
    conn = boto.s3.connect_to_region(
               'us-east-1',
               aws_access_key_id=AWS_ACCESS_KEY_ID,
               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
               is_secure=True,
               calling_format = boto.s3.connection.OrdinaryCallingFormat(),
           )
    bucket = conn.get_bucket(BUCKET_NAME)
    return conn, bucket

def split_filename(full_filename):
    if '/' not in full_filename:
        return full_filename
    filename_parts = full_filename.split('/')
    path, filename = '/'.join(filename_parts[:-1]), filename_parts[-1]
    return path, filename

def download_file(full_filename):
    path, filename = split_filename(full_filename)
    conn, bucket = get_connection_and_bucket()
    full_key_name = os.path.join(path, filename)
    print("Attempting to download from {}".format(full_key_name))
    k = Key(bucket)
    k.key = full_key_name
    k.get_contents_to_filename(filename)

def read_from_pickle(filename, from_bucketeer=False):
    if from_bucketeer:
        download_file(filename)
        path, filename = split_filename(filename)

    with open(filename, 'rb') as f:
        return pickle.load(f)

"""
Database logic
"""
DATABASE_URL = os.environ.get("DATABASE_URL")

# Helper function to do SQL SELECT query
def query(sql, data=(), column_names=[]):
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SET TIME ZONE 'Singapore';")
    cursor.execute(sql, data)
    records = cursor.fetchall()
    # This returns a list of results (where results is represented as a tuple)
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
    records = query(sql, data=data, column_names=['tripId'])
    return [record[0] for record in records]

"""
Helper methods
"""
def stringify_predictions(predictions):
    result = {}
    for stop_id, date_time in predictions.items():
        if type(date_time) == datetime:
            result[str(stop_id)] = date_time.strftime('%Y-%m-%dT%H:%M:%S+0800')
        else:
            result[str(stop_id)] = date_time
    return result

"""
API
"""
@app.route('/api/v1.0/', methods=['GET'])
def get_all_predictions():
    try:
        trip_ids = get_operating_trip_ids()
        predictions_per_trip = {}
        for trip_id in trip_ids:
            try:
                predictions = read_from_pickle(get_filename(trip_id), from_bucketeer=True)
                predictions = stringify_predictions(predictions)
                predictions_per_trip[str(trip_id)] = predictions
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)
        abort(500)

    return jsonify(predictions_per_trip)

@app.route('/api/v1.0/<int:trip_id>', methods=['GET'])
def get_predictions(trip_id):
    try:
        predictions = read_from_pickle(get_filename(trip_id), from_bucketeer=True)
    except Exception as e:
        print(e)
        abort(404)
    
    predictions = stringify_predictions(predictions)
    return jsonify(predictions)
    

@app.route('/api/v1.0/<int:trip_id>/<int:stop_id>', methods=['GET'])
def get_prediction(trip_id, stop_id):
    try:
        predictions = read_from_pickle(get_filename(trip_id), from_bucketeer=True)
    except Exception as e:
        print(e)
        abort(404)
    
    if not stop_id in predictions:
        abort(404)
    
    predictions = stringify_predictions(predictions)
    return jsonify(predictions[str(stop_id)])

if __name__ == '__main__':
    app.run(debug=True)