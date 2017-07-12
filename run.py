import multiprocessing
from datetime import datetime
from db_logic import get_operating_trip_ids
from trip_predictor import update_timings_for_trip

def run(date_time):
    operating_trip_ids = get_operating_trip_ids(date_time)

    for trip_id in operating_trip_ids:
        # Do an independent process for each trip to update timings.
        p = multiprocessing.Process(target=update_timings_for_trip,
                                    args=(date_time, trip_id))
        p.start()