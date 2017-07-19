import multiprocessing
from datetime import datetime
from db_logic import get_operating_trip_ids
from trip_predictor import update_timings_for_trip

def run(date_time):
    operating_trip_ids = get_operating_trip_ids(date_time)
    print('Operating trips: ' + str(operating_trip_ids))

    with multiprocessing.Pool(5) as pool:
        pool.map(
            lambda trip_id: update_timings_for_trip(date_time, trip_id),
            operating_trip_ids
        )
