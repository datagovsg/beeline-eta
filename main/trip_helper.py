import pandas
from constants import DATE_FORMAT
from datetime import datetime, timedelta
from db_logic import get_tripstops, get_pings
from utility import latlng_bearing, latlng_distance

# Get pings from first trip ping time to current date_time,
# sorted starting from the most recent ping.
def get_most_recent_pings(trip_id, date_time):
    return get_pings(trip_id=trip_id, newest_datetime=date_time) \
           .sort_values('time', ascending=False)

# Get the list of distances between each consecutive ping pairs
get_distances = lambda trip_pings: \
    [latlng_distance((ping1.lat, ping1.lng), (ping2.lat, ping2.lng))
     for ping1, ping2 in list(zip(trip_pings, trip_pings[1:]))]

# Get the list of bearings from each ping to its respective next ping
get_bearings = lambda trip_pings: \
    [latlng_bearing((ping1.lat, ping1.lng), (ping2.lat, ping2.lng))
     for ping1, ping2 in list(zip(trip_pings, trip_pings[1:]))]

# Get the list of time elapsed between each consecutive ping pairs
get_intervals = lambda trip_pings: \
    [(ping2.time - ping1.time).total_seconds()
     for ping1, ping2 in list(zip(trip_pings, trip_pings[1:]))]

# Get the list of speeds from each ping to its respective next ping
get_speeds = lambda trip_pings: \
    [distance/timing if timing > 0 else 0
     for distance, timing in list(zip(get_distances(trip_pings), get_intervals(trip_pings)))]

# Determine if a trip's route is circular
# The heuristics here is checking if any tripstop is repeated more than once.
is_circular_trip = lambda trip_id: any([count > 1 
                                        for count in 
                                        get_tripstops(trip_id=trip_id)
                                        .groupby('stopId')
                                        .size()])

# Given a list of stops, return the cycle length of the stops
def get_cycle_length(stop_ids):
    first_stop_id = stop_ids[0]
    for i in range(1, len(stop_ids)):
        if stop_ids[i] == first_stop_id:
            return i
    return -1

# Given a list of stops (circular), return the cycle of the stops
def get_trip_cycle(stop_ids):
    cycle_length = get_cycle_length(stop_ids)
    return stop_ids[:cycle_length]
