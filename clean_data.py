import pandas
from datetime import datetime, timedelta
from trip_helper import get_bearings, get_distances, get_speeds, get_timings, get_trip_pings
from utility import flatten

# From trip_pings, return Trip pings with dirty pings removed
get_cleaned_trip_pings = lambda trip_pings: flatten(clean_rep(trip_pings))

# Compute the smallest difference from bearing 1 to bearing 2
def delta_angle(bearing_1, bearing_2):
    delta = bearing_2 - bearing_1
    return delta - 360 if delta > 180 else delta + 360 if delta < -180 else delta

# Check if the turn is a sharp turn
def is_sharp_turn(bearing_1, bearing_2):
    return abs(delta_angle(bearing_1, bearing_2)) > 120

# Takes a trip_pings_parts, split it further if a criteria is above the threshold
def split_trip_by_criteria(trip_pings_parts, criteria_function, threshold_value):
    cleaned_trip_pings_parts = []
    for j in range(len(trip_pings_parts)):
        trip_pings_part = trip_pings_parts[j]
        criteria_part = criteria_function(trip_pings_part)
        
        start_index = 0
        i = 0
        while i < len(criteria_part):
            if criteria_part[i] >= threshold_value:
                cleaned_trip_pings_parts.append(trip_pings_part[start_index:i+1])
                start_index = i + 1
            i += 1
        
        cleaned_trip_pings_parts.append(trip_pings_part[start_index:])
    return [part for part in cleaned_trip_pings_parts if len(part) > 2]

# Split trip if edge is more than 30s.
MAX_TIME = 30
split_trip_by_time = lambda trip_pings_parts: \
    split_trip_by_criteria(trip_pings_parts, get_timings, MAX_TIME)

# Split trip if edge is more than 28m/s (~= 100km/h, impossible in Singapore)
MAX_SPEED = 28
split_trip_by_speed = lambda trip_pings_parts: \
    split_trip_by_criteria(trip_pings_parts, get_speeds, MAX_SPEED)

# For each part in trip_pings_parts, 
# remove points that cause consecutive sharp turns to occur
def smoothen_trip(trip_pings_parts):
    for j in range(len(trip_pings_parts)):
        trip_pings_part = trip_pings_parts[j]
        
        # if i-3, i-2, i-1 bearing is a sharp turn (abs(delta_angle) > 120),
        # remove i-2 and/or i-1 ping from the list to smoothen trip.
        bearings = get_bearings(trip_pings_part)
        cleaned_trip_pings_part = []
        i = 3
        while i < len(trip_pings_part):
            if is_sharp_turn(bearings[i-3], bearings[i-2]) \
            and is_sharp_turn(bearings[i-2], bearings[i-1]):
                trip_pings_removed_second = [trip_pings_part[i-3], 
                                             trip_pings_part[i-1], 
                                             trip_pings_part[i]]
                bearings_removed_second = get_bearings(trip_pings_removed_second)
                abs_delta_angle_removed_second = abs(delta_angle(bearings_removed_second[0], 
                                                                 bearings_removed_second[1]))
                
                trip_pings_removed_third = [trip_pings_part[i-3], 
                                            trip_pings_part[i-2], 
                                            trip_pings_part[i]]
                bearings_removed_third = get_bearings(trip_pings_removed_third)
                abs_delta_angle_removed_third = abs(delta_angle(bearings_removed_third[0], 
                                                                bearings_removed_third[1]))
            
                # Remove the 2nd or 3rd ping (or both) 
                # depending on which reduces the sharp turn best 
                # while not removing too much data.
                if abs_delta_angle_removed_third < min(abs_delta_angle_removed_second, 90):
                    trip_pings_part = trip_pings_part[:i-1] + trip_pings_part[i:]
                elif abs_delta_angle_removed_second < min(abs_delta_angle_removed_third, 90):
                    trip_pings_part = trip_pings_part[:i-2] + trip_pings_part[i-1:]
                else:
                    trip_pings_part = trip_pings_part[:i-2] + trip_pings_part[i:]
            i += 1
        
        trip_pings_parts[j] = trip_pings_part
    return trip_pings_parts

# Take trip pings and remove anomalous pings.
def clean_rep(trip_pings):
    if len(trip_pings) == 0:
        return []
    
    trip_pings_parts = \
        [[trip_ping for trip_ping in trip_pings.sort_values('time').itertuples()]]
    
    # Use multiple passes to iteratively smoothen trip parts, 
    # and split trip by speed and time
    for i in range(5):
        trip_pings_parts = smoothen_trip(trip_pings_parts)
        trip_pings_parts = split_trip_by_speed(trip_pings_parts)
        trip_pings_parts = split_trip_by_time(trip_pings_parts)

    return trip_pings_parts

# Check if the pings by a trip violates any condition for prediction.
def check_rep(trip_id, date_time=datetime.now()):
    trip_pings = get_trip_pings(trip_id)
    trip_pings_parts = clean_rep(trip_pings)
    
    if len(trip_pings_parts) == 0:
        return 'No prediction: No trip pings at all'
    
    if len(trip_pings_parts[-1]) < 3:
        return 'No prediction: Insufficient latest trip pings for prediction'
    
    if date_time - trip_pings_parts[-1][-1].time >= timedelta(minutes=1):
        return 'No prediction: The latest ping is more than 1 minute from now; prediction will be inaccurate'
    
    return 'Can predict'
