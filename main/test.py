import unittest
from datetime import datetime, timedelta
from db_logic import get_offset, get_pings, get_tripstops
from run import update_timings_for_trip
from ping_locator import list_of_nearest_pings_to_tripstops

def get_predicted_arrival_timing(trip_id, next_stop_id):
    date_time = datetime.now() - get_offset(minutes=8 * 60 + 10)
    stop_ids_to_predicted_arrival_timings = update_timings_for_trip(date_time, trip_id)
    if not stop_ids_to_predicted_arrival_timings:
        return None
    next_stop_predicted_arrival_timing = stop_ids_to_predicted_arrival_timings[next_stop_id]
    return next_stop_predicted_arrival_timing

def get_actual_arrival_timing(trip_id, next_stop_id):
    date_time = datetime.now() - get_offset(minutes=8 * 60 + 10)
    # Use default current datetime, so all pings are considered for actual arrival timings
    nearest_ping_id_per_tripstop_id = list_of_nearest_pings_to_tripstops(trip_id)
    if not nearest_ping_id_per_tripstop_id:
        return None
    nearest_ping_id = [ping_id
                       for tripstop_id, ping_id in nearest_ping_id_per_tripstop_id
                       if get_tripstops(tripstop_id=tripstop_id).iloc[0].stopId == next_stop_id][0]
    next_stop_actual_arrival_timing = get_pings(ping_id=nearest_ping_id).iloc[0].time
    return next_stop_actual_arrival_timing

class TestPredictions(unittest.TestCase):

    def test_immediate_next_stop(self):
        self.test_with_data(trip_id=18258, stop_id=4442, allowable_error=60)

    def test_next_next_stop(self):
        self.test_with_data(trip_id=18258, stop_id=4385, allowable_error=60)

    # This is the helper function, but it also test_next_next_next_stop with the default values.
    def test_with_data(self, trip_id=18258, stop_id=4468, allowable_error=120):
        predicted_arrival_timing = get_predicted_arrival_timing(trip_id, stop_id)
        actual_arrival_timing = get_actual_arrival_timing(trip_id, stop_id)

        # Assert predicted and actual arrival time is close enough
        residual_time = (predicted_arrival_timing - actual_arrival_timing).total_seconds()

        self.assertTrue(abs(residual_time) < allowable_error)

    def test_without_data(self):
        predicted_arrival_timing = get_predicted_arrival_timing(15220, 5713)

        # Assert no predicted arrival timing
        self.assertIsNone(predicted_arrival_timing)

if __name__ == '__main__':
    unittest.main()