import numpy as np
import pandas
from clean_data import check_rep, get_cleaned_trip_pings, is_sharp_turn
from constants import DATETIME_FORMAT, DATE_FORMAT
from datetime import datetime, timedelta
from db_logic import (
    get_pings, get_trips, get_tripstops, get_past_trips_of_route
)
from ping_locator import (
    get_kd_tree, list_of_nearest_pings_to_tripstops, list_of_nearest_pings_to_stops, p
)
from save_and_load_variables import write_to_pickle
from trip_helper import (
    is_circular_trip, get_bearings,
    get_most_recent_pings, get_trip_cycle
)
from utility import is_sorted, latlng_distance, transpose

def update_timings_for_trip(date_time, trip_id):
    trip_tripstops = get_tripstops(trip_id=trip_id)
    trip_pings = get_pings(trip_id=trip_id, newest_datetime=date_time)

    stop_ids = trip_tripstops.stopId.tolist()
    if is_circular_trip(trip_id):
        stop_ids = get_trip_cycle(stop_ids)

    tripstop_arrival_times = [datetime.strptime(str(date_time), DATETIME_FORMAT + '+08:00')
                              for date_time in trip_tripstops.time.tolist()]

    # If the pings for trip_id fails check_rep, we show them the error message
    message = check_rep(trip_id, date_time=date_time)
    if message.startswith('No prediction'):
        update_prediction(trip_id, stop_ids, [message] * len(trip_tripstops))
        return

    predicted_arrival_times = []

    if is_circular_trip(trip_id):
        predicted_arrival_times = predict_arrival_times_for_circular_trips(trip_id, date_time)
    else:
        predicted_arrival_times = predict_arrival_times_for_normal_trips(trip_id, date_time)

    if not predicted_arrival_times:
        message = 'No prediction: Insufficient historical data for prediction.'
        update_prediction(trip_id, stop_ids, [message] * len(trip_tripstops))
        return

    # Save and overwrite prediction
    update_prediction(trip_id, stop_ids, predicted_arrival_times)

    return dict(zip(stop_ids, predicted_arrival_times))


def update_prediction(trip_id, stop_ids, predicted_arrival_times):
    filename = 'results/prediction-{}.pickle'.format(str(trip_id))
    write_to_pickle(filename, dict(zip(stop_ids, predicted_arrival_times)))

def predict_arrival_times_for_normal_trips(main_trip_id, date_time):
    threshold_distance = 20
    
    most_recent_pings = get_most_recent_pings(main_trip_id, date_time)
    if len(most_recent_pings) == 0:
        return
    most_recent_ping = most_recent_pings.iloc[0]
    most_recent_ping_lat_lng = (most_recent_ping.lat,
                                most_recent_ping.lng)
    most_recent_ping_x_y = p(most_recent_ping_lat_lng[1],
                             most_recent_ping_lat_lng[0])
    
    # Get alternative past trip_ids for the same route as main_trip_id 
    route_id = get_trips(trip_id=main_trip_id).iloc[0].routeId

    trip_ids = get_past_trips_of_route(route_id, date_time).index
    trip_ids = [trip_id for trip_id in trip_ids if trip_id != main_trip_id]

    main_trip_tripstops = get_tripstops(trip_id=main_trip_id)

    list_of_trip_tripstop_durations = []
    for trip_id in trip_ids:
        if len(list_of_trip_tripstop_durations) >= 5:
            break

        trip_tripstops = get_tripstops(trip_id=trip_id)
        if trip_tripstops.stopId.tolist() != main_trip_tripstops.stopId.tolist():
            continue
        
        # Find the trip ping that is closest to most_recent_ping (<20m)
        cleaned_trip_pings = get_cleaned_trip_pings(get_pings(trip_id=trip_id)) # Past trip, just use full trip pings
        kd_tree = get_kd_tree(trip_id, date_time=date_time)
        if not kd_tree:
            continue
        nearest_ping_indices = kd_tree.query_ball_point(most_recent_ping_x_y,
                                                        r=threshold_distance)
        time_differences = [abs(cleaned_trip_pings[i].time - most_recent_ping.time).total_seconds()
                            for i in nearest_ping_indices]
        # Only time difference is used as metric since all the distances are below 20m anyway.
        indices_ordered = \
            [index for time_difference, index in
             sorted(list(zip(time_differences, nearest_ping_indices)))]
        if len(indices_ordered) == 0:
            continue
        closest_ping = cleaned_trip_pings[indices_ordered[0]]

        # For the trip ping, take difference in timing from trip ping to each future tripstops
        sorted_tripstop_to_nearest_ping = list_of_nearest_pings_to_tripstops(trip_id, date_time=date_time)

        # Check that there is a nearest ping at every tripstop
        if len(sorted_tripstop_to_nearest_ping) != len(trip_tripstops):
            continue
        
        nearest_ping_timing_per_tripstop = \
            [get_pings(ping_id=ping_id).iloc[0].time
             for tripstop_id, ping_id in sorted_tripstop_to_nearest_ping]
        
        # Check that the timings of the nearest pings is sorted
        if not is_sorted(nearest_ping_timing_per_tripstop):
            continue

        duration_per_tripstop = \
            [(ping_timing - closest_ping.time).total_seconds() 
             for ping_timing in nearest_ping_timing_per_tripstop]
        list_of_trip_tripstop_durations.append(duration_per_tripstop)

    # After getting a few values from prev step, can take mean + S.D.
    mean_sd_of_duration_per_trip = []
    list_of_durations_per_trip = transpose(list_of_trip_tripstop_durations)
    for list_of_durations in list_of_durations_per_trip:
        # If >= 3 datapoints, we remove the top and bottom timing, then take mean + S.D
        sorted_list_of_durations = sorted(list_of_durations)
        if len(sorted_list_of_durations) >= 3:
            sorted_list_of_durations = sorted_list_of_durations[1:-1]
        mean_sd_of_duration_per_trip.append((np.mean(sorted_list_of_durations), 
                                             np.std(sorted_list_of_durations)))
    
    duration_per_trip = [mean_duration 
                         for mean_duration, sd_duration in mean_sd_of_duration_per_trip]
    predicted_arrival_times = \
        [most_recent_pings.iloc[0].time.to_datetime() + timedelta(seconds=duration)
         for duration in duration_per_trip]

    # Handle edge case: When the bus is parking near the first stop.
    if len(predicted_arrival_times) > 0:
        first_stop_planned_time = main_trip_tripstops.iloc[0].time.to_datetime()
        predicted_arrival_times[0] = max(predicted_arrival_times[0], first_stop_planned_time)

    return predicted_arrival_times

def predict_arrival_times_for_circular_trips(main_trip_id, date_time):
    threshold_distance = 20

    most_recent_pings = get_most_recent_pings(main_trip_id, date_time)
    if len(most_recent_pings) == 1:
        return
    most_recent_ping = most_recent_pings.iloc[0]
    most_recent_ping_lat_lng = (most_recent_ping.lat,
                                most_recent_ping.lng)
    most_recent_ping_x_y = p(most_recent_ping_lat_lng[1],
                             most_recent_ping_lat_lng[0])

    # Get bearing from 2nd most recent ping to most recent ping
    main_trip_current_bearing = get_bearings([most_recent_pings.iloc[1], most_recent_pings.iloc[0]])[0]

    # Get alternative past trip_ids for the same route as main_trip_id
    route_id = get_trips(trip_id=main_trip_id).iloc[0].routeId

    trip_ids = get_past_trips_of_route(route_id, date_time).index
    trip_ids = [trip_id for trip_id in trip_ids if trip_id != main_trip_id]

    main_trip_tripstops = get_tripstops(trip_id=main_trip_id)

    list_of_trip_tripstop_durations = []
    for trip_id in trip_ids:
        if len(list_of_trip_tripstop_durations) >= 3:
            break

        trip_tripstops = get_tripstops(trip_id=trip_id)
        if get_trip_cycle(trip_tripstops.stopId.tolist()) \
        != get_trip_cycle(main_trip_tripstops.stopId.tolist()):
            continue

        # Find the trip ping that is closest to most_recent_ping (<20m) and time
        cleaned_trip_pings = get_cleaned_trip_pings(get_pings(trip_id=trip_id)) # Past trip, just use full trip pings
        if not cleaned_trip_pings:
            continue

        kd_tree = get_kd_tree(trip_id, date_time=date_time)
        if not kd_tree:
            continue

        # Filter pings that have wrong heading first (relevant for circular routes with potential U-turns)
        trip_bearings = get_bearings(cleaned_trip_pings)
        trip_bearings.append(trip_bearings[-1]) # To keep the length of trip_bearings the same as number of pings.
        nearest_ping_indices = kd_tree.query_ball_point(most_recent_ping_x_y,
                                                        r=threshold_distance)
        nearest_ping_indices = [i for i in nearest_ping_indices
                                if not is_sharp_turn(trip_bearings[i], main_trip_current_bearing)]

        time_differences = [abs(cleaned_trip_pings[i].time - most_recent_ping.time).total_seconds()
                            for i in nearest_ping_indices]

        # Only time difference is used as metric since all the distances are below 20m anyway.
        indices_ordered = \
            [index for time_difference, index in
             sorted(list(zip(time_differences, nearest_ping_indices)))]
        if len(indices_ordered) == 0:
            continue
        closest_ping_index = indices_ordered[0]
        closest_ping = cleaned_trip_pings[closest_ping_index]

        # (In trip predictor) To find best ping using most recent ping (MRP),
        # take the one that minimises duration to MRP, while still positive
        sorted_stop_to_nearest_pings = list_of_nearest_pings_to_stops(trip_id, date_time=date_time)
        duration_per_stop = []
        for stop_id, ping_ids in sorted_stop_to_nearest_pings:
            nearest_pings_timing_at_stop = [get_pings(ping_id=ping_id).iloc[0].time for ping_id in ping_ids]
            durations_to_stop = [(arrival_time - closest_ping.time).total_seconds()
                                 for arrival_time in nearest_pings_timing_at_stop]
            positive_durations_to_stop = [duration for duration in durations_to_stop if duration > 0]
            smallest_positive_duration_to_stop = 0 if len(positive_durations_to_stop) == 0 else min(positive_durations_to_stop)
            duration_per_stop.append(smallest_positive_duration_to_stop)

        list_of_trip_tripstop_durations.append(duration_per_stop)

    # After getting a few values from prev step, can take mean + S.D.
    median_duration_per_trip = []
    list_of_durations_per_trip = transpose(list_of_trip_tripstop_durations)
    for list_of_durations in list_of_durations_per_trip:
        sorted_list_of_durations = sorted(list_of_durations)
        median_duration_per_trip.append(np.median(sorted_list_of_durations))

    predicted_arrival_times = \
        [most_recent_pings.iloc[0].time.to_datetime() + timedelta(seconds=duration)
         for duration in median_duration_per_trip]

    # Handle edge case: When the bus is parking near the first stop.
    if len(predicted_arrival_times) > 0:
        first_stop_planned_time = main_trip_tripstops.iloc[0].time.to_datetime()
        predicted_arrival_times[0] = max(predicted_arrival_times[0], first_stop_planned_time)

    return predicted_arrival_times