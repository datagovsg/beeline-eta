#!flask/bin/python
from flask import Flask, abort, jsonify

from datetime import datetime
import glob
import pickle

app = Flask(__name__)

get_filename = lambda trip_id: 'results/prediction-{}.pickle'.format(trip_id)

import boto
import boto.s3.connection
import os
from boto.s3.key import Key

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

def upload_file(full_filename):
    path, filename = split_filename(full_filename)
    conn, bucket = get_connection_and_bucket()
    full_key_name = os.path.join(path, filename)
    k = bucket.new_key(full_key_name)
    k.set_contents_from_filename(filename)

def download_file(full_filename):
    path, filename = split_filename(full_filename)
    conn, bucket = get_connection_and_bucket()
    full_key_name = os.path.join(path, filename)
    k = bucket.new_key(full_key_name)
    k.get_contents_from_filename(filename)

def list_files(path):
    conn, bucket = get_connection_and_bucket()
    return [key.name for key in bucket]

def read_from_pickle(filename, from_bucketeer=False):
    if from_bucketeer:
        download_file(filename)

    with open(filename, 'rb') as f:
        return pickle.load(f)

def stringify_predictions(predictions):
    result = {}
    for stop_id, date_time in predictions.items():
        if type(date_time) == datetime:
            result[str(stop_id)] = date_time.strftime('%Y-%m-%dT%H:%M:%S+0800')
        else:
            result[str(stop_id)] = date_time
    return result

"""
@app.route('/api/v1.0/', methods=['GET'])
def get_all_predictions():
    try:
        predictions_per_trip = {}
        files = glob.glob('../main/results/*')
        for f in files:
            if f.endswith('gitkeep'):
                continue
            trip_id = [int(s) for s in f.replace('.', '-').split('-') if s.isdigit()][0]
            predictions_per_trip[str(trip_id)] = stringify_predictions(pickle.load(open(f, 'rb')))
    except:
        abort(500)

    return jsonify(predictions_per_trip)
"""

@app.route('/api/v1.0/<int:trip_id>', methods=['GET'])
def get_predictions(trip_id):
    try:
        predictions = pickle.load(read_from_pickle(get_filename(trip_id), to_bucketeer=True))
    except:
        abort(404)
    
    predictions = stringify_predictions(predictions)
    return jsonify(predictions)
    

@app.route('/api/v1.0/<int:trip_id>/<int:stop_id>', methods=['GET'])
def get_prediction(trip_id, stop_id):
    try:
        predictions = pickle.load(read_from_pickle(get_filename(trip_id), to_bucketeer=True))
    except:
        abort(404)
    
    if not stop_id in predictions:
        abort(404)
    
    predictions = stringify_predictions(predictions)
    return jsonify(predictions[str(stop_id)])

if __name__ == '__main__':
    app.run(debug=True)