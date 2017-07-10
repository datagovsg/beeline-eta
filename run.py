import threading
from constants import API_RESULT_FILE
from datetime import datetime
from save_and_load_variables import write_to_pickle
from trip_helper import get_operating_trip_ids
from trip_predictor import update_timings_for_trip

def run(date_time):
    api_result = {}
    
    operating_trip_ids = get_operating_trip_ids(date_time)

    threads = []
    for trip_id in operating_trip_ids:
        # Do multithread for each trip to update timings.
        t = threading.Thread(target=update_timings_for_trip,
                             args=(api_result, date_time, trip_id))
        threads.append(t)
    for t in threads: # Start all threads
        t.start()
    for t in threads: # Wait for all threads to finish
        t.join()
    
    # Rewrite api-result.pickle
    write_to_pickle(API_RESULT_FILE, api_result)
    return api_result