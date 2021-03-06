import unittest
from clean_data import get_cleaned_trip_pings, is_sharp_turn
from datetime import datetime, timedelta
from db_logic import get_offset, get_pings, get_stops, get_tripstops
from run import update_timings_for_trip
from ping_locator import list_of_nearest_pings_to_stops, list_of_nearest_pings_to_tripstops
from trip_helper import get_bearings, is_circular_trip
from utility import is_nan

def get_predicted_arrival_timing(trip_id, stop_id, date_time):
    stop_ids_to_predicted_arrival_timings = update_timings_for_trip(date_time, trip_id, to_bucketeer=False)
    if not stop_ids_to_predicted_arrival_timings:
        return None
    next_stop_predicted_arrival_timing = stop_ids_to_predicted_arrival_timings[stop_id]
    return next_stop_predicted_arrival_timing

def get_actual_arrival_timing(trip_id, stop_id, date_time):
    # Use default current datetime, so all pings are considered for actual arrival timings
    if is_circular_trip(trip_id):
        nearest_pings_id_per_stop_id = list_of_nearest_pings_to_stops(trip_id)
        if not nearest_pings_id_per_stop_id:
            return None

        # Filter pings that have wrong heading first (relevant for circular routes with potential U-turns)
        trip_pings = get_pings(trip_id=trip_id) # Actual timing just take the whole set of pings
        cleaned_trip_pings = get_cleaned_trip_pings(trip_pings)
        trip_bearings = get_bearings(cleaned_trip_pings)
        trip_bearings.append(trip_bearings[-1]) # To keep the length of trip_bearings the same as number of pings.

        stop = get_stops(stop_id=stop_id).iloc[0]
        stop_heading = stop.heading

        nearest_ping_ids = [ping_ids
                            for each_stop_id, ping_ids in nearest_pings_id_per_stop_id
                            if each_stop_id == stop_id][0]
        nearest_ping_ids = [ping_id
                            for i, ping_id in enumerate(nearest_ping_ids)
                            if not is_sharp_turn(trip_bearings[i], trip_bearings[i] if is_nan(stop_heading) else stop_heading)]
        next_stop_actual_arrival_timings = [get_pings(ping_id=ping_id).iloc[0].time
                                            for ping_id in nearest_ping_ids]
        arrival_timings_after_date_time = [timing
                                           for timing in next_stop_actual_arrival_timings
                                           if timing.replace(tzinfo=None) > date_time]
        if not arrival_timings_after_date_time:
            return None
        return min(arrival_timings_after_date_time)
    else:
        nearest_ping_id_per_tripstop_id = list_of_nearest_pings_to_tripstops(trip_id)
        if not nearest_ping_id_per_tripstop_id:
            return None
        nearest_ping_id = [ping_id
                           for tripstop_id, ping_id in nearest_ping_id_per_tripstop_id
                           if get_tripstops(tripstop_id=tripstop_id).iloc[0].stopId == stop_id][0]
        next_stop_actual_arrival_timing = get_pings(ping_id=nearest_ping_id).iloc[0].time
        return next_stop_actual_arrival_timing

def get_residual_time(trip_id, stop_id, date_time):
    predicted_arrival_timing = get_predicted_arrival_timing(trip_id, stop_id, date_time)
    actual_arrival_timing = get_actual_arrival_timing(trip_id, stop_id, date_time)
    return (predicted_arrival_timing - actual_arrival_timing).total_seconds()

# Returns True if residual_time is < 10% of (actual_time - date_time)
def is_within_allowable_margin(trip_id, stop_id, date_time, allowable_margin):
    residual_timing = get_residual_time(trip_id, stop_id, date_time)
    return abs(residual_timing) <= allowable_margin

class TestPredictions(unittest.TestCase):

    def test_normal_trip_next_stop(self):
        date_time = datetime(2017, 7, 6, 9, 28, 42)
        self.assertTrue(is_within_allowable_margin(18258, 4442, date_time, 60))

    def test_normal_trip_next_next_stop(self):
        date_time = datetime(2017, 7, 6, 9, 28, 42)
        self.assertTrue(is_within_allowable_margin(18258, 4385, date_time, 120))

    def test_normal_trip_next_next_next_stop(self):
        date_time = datetime(2017, 7, 6, 9, 28, 42)
        self.assertTrue(is_within_allowable_margin(18258, 4468, date_time, 180))

    #def test_circular_trip_next_stop(self):
    #    date_time = datetime(2017, 6, 15, 14, 25, 0)
    #    self.assertTrue(is_within_allowable_margin(15335, 957, date_time, 60))

    #def test_circular_trip_next_next_stop(self):
    #    date_time = datetime(2017, 6, 15, 14, 25, 0)
    #    self.assertTrue(is_within_allowable_margin(15335, 1147, date_time, 120))

    #def test_circular_trip_just_past_next_next_stop(self):
    #    date_time = datetime(2017, 6, 15, 14, 29, 0)
    #    self.assertTrue(is_within_allowable_margin(15335, 1147, date_time, 600)) # 10 mins since its ~45mins away

    def test_trip_without_data(self):
        date_time = datetime.now() - get_offset(minutes=8 * 60 + 10)
        predicted_arrival_timing = get_predicted_arrival_timing(15220, 5713, date_time)

        # Assert no predicted arrival timing
        self.assertIsNone(predicted_arrival_timing)

if __name__ == '__main__':
    unittest.main()