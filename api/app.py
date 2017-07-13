#!flask/bin/python
from flask import Flask, abort, jsonify

from datetime import datetime
import pickle

app = Flask(__name__)

get_filename = lambda trip_id: '../main/results/prediction-{}.pickle'.format(trip_id)

def stringify_predictions(predictions):
    result = {}
    for stop_id, date_time in predictions.items():
        if type(date_time) == datetime:
            result[str(stop_id)] = date_time.strftime('%Y-%m-%dT%H:%M:%S+0800')
        else:
            result[str(stop_id)] = date_time
    return result


@app.route('/api/v1.0/<int:trip_id>', methods=['GET'])
def get_predictions(trip_id):
    try:
        predictions = pickle.load(open(get_filename(trip_id), 'rb'))
    except:
        abort(404)
    
    predictions = stringify_predictions(predictions)
    return jsonify(predictions)
    

@app.route('/api/v1.0/<int:trip_id>/<int:stop_id>', methods=['GET'])
def get_prediction(trip_id, stop_id):
    try:
        predictions = pickle.load(open(get_filename(trip_id), 'rb'))
    except:
        abort(404)
    
    if not stop_id in predictions:
        abort(404)
    
    predictions = stringify_predictions(predictions)
    return jsonify(predictions[str(stop_id)])

if __name__ == '__main__':
    app.run(debug=True)