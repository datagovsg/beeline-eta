import pandas
from constants import DATE_FORMAT
from datetime import datetime, timedelta
from trip_data import pings, routes, trips, tripstops 
from utility import bearing, distance

# Given a trip id, returns the boarding tripstops, sorted in ascending time
get_trip_tripstops = lambda trip_id: tripstops[(tripstops['tripId'] == trip_id) 
                                             & (tripstops['canBoard'] == 't')] \
                                              .sort_values('time')

# Given a trip id, returns the pings, sorted in ascending time
get_trip_pings = lambda trip_id: pings[pings['tripId'] == trip_id] \
                                 .sort_values('time')

# Get pings from first trip ping time to current date_time,
# sorted starting from the most recent ping.
def get_most_recent_pings(trip_id, date_time):
    return pings[(pings['tripId'] == trip_id)
               & (pings['time'] <= date_time)] \
                .sort_values('time', ascending=False)

# Get the list of distances between each consecutive ping pairs
get_distances = lambda trip_pings: \
    [distance(ping1.lat, ping1.lng, ping2.lat, ping2.lng)
     for ping1, ping2 in list(zip(trip_pings, trip_pings[1:]))]

# Get the list of bearings from each ping to its respective next ping
get_bearings = lambda trip_pings: \
    [bearing(ping1.lat, ping1.lng, ping2.lat, ping2.lng)
     for ping1, ping2 in list(zip(trip_pings, trip_pings[1:]))]

# Get the list of time elapsed between each consecutive ping pairs
get_timings = lambda trip_pings: \
    [(ping2.time - ping1.time).total_seconds()
     for ping1, ping2 in list(zip(trip_pings, trip_pings[1:]))]

# Get the list of speeds from each ping to its respective next ping
get_speeds = lambda trip_pings: \
    [distance/timing if timing > 0 else 0
     for distance, timing in list(zip(get_distances(trip_pings), get_timings(trip_pings)))]

# Determine if a trip's route is circular
# The heuristics here is checking if any tripstop is repeated more than once.
is_circular_trip = lambda trip_id: any([count > 1 
                                        for count in 
                                        get_trip_tripstops(trip_id)
                                        .groupby('stopId')
                                        .size()])

# Input: date_time (datetime object)
# Output: List of trip_ids that are operating at stated date_time.
def get_operating_trip_ids(date_time):
    date_string = datetime.strftime(date_time, DATE_FORMAT)
    trip_ids = trips[trips['date'] == date_string].index.tolist()
    
    # Reduce the search space for all_tripstops later in the for-loop
    filtered_tripstops = tripstops[tripstops['tripId'].isin(trip_ids)]
    
    # Collect a list of trip_ids that are operational 
    # within (start - 20) min and (end + 20) mins of date_time
    operating_trip_ids = []
    for trip_id in trip_ids:
        trip_tripstops = filtered_tripstops[filtered_tripstops['tripId'] == trip_id] \
                         .sort_values('time')
        if trip_tripstops.iloc[0].time - timedelta(minutes=20) < date_time \
        and date_time < trip_tripstops.iloc[-1].time + timedelta(minutes=20):
            operating_trip_ids.append(trip_id)
    return operating_trip_ids