import unittest
from datetime import datetime, timedelta
from db_logic import get_offset, get_pings, get_tripstops
from run import update_timings_for_trip
from ping_locator import list_of_nearest_pings_to_tripstops

def get_predicted_arrival_timing(trip_id, stop_id, date_time):
    stop_ids_to_predicted_arrival_timings = update_timings_for_trip(date_time, trip_id)
    if not stop_ids_to_predicted_arrival_timings:
        return None
    next_stop_predicted_arrival_timing = stop_ids_to_predicted_arrival_timings[stop_id]
    return next_stop_predicted_arrival_timing

def get_actual_arrival_timing(trip_id, stop_id, date_time):
    # Use default current datetime, so all pings are considered for actual arrival timings
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

class TestPredictions(unittest.TestCase):

    def test_immediate_next_stop(self):
        date_time = datetime.now() - get_offset(minutes=8 * 60 + 10)
        allowable_error = 60
        self.assertTrue(abs(get_residual_time(18258, 4442, date_time)) <= allowable_error)

    def test_next_next_stop(self):
        date_time = datetime.now() - get_offset(minutes=8 * 60 + 10)
        allowable_error = 60
        self.assertTrue(abs(get_residual_time(18258, 4385, date_time)) <= allowable_error)

    def test_next_next_next_stop(self):
        date_time = datetime.now() - get_offset(minutes=8 * 60 + 10)
        allowable_error = 120
        self.assertTrue(abs(get_residual_time(18258, 4468, date_time)) <= allowable_error)

    def test_without_data(self):
        date_time = datetime.now() - get_offset(minutes=8 * 60 + 10)
        predicted_arrival_timing = get_predicted_arrival_timing(15220, 5713, date_time)

        # Assert no predicted arrival timing
        self.assertIsNone(predicted_arrival_timing)

if __name__ == '__main__':
    unittest.main()