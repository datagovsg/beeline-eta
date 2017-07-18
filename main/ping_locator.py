import pandas
from clean_data import clean_rep
from datetime import datetime
from db_logic import get_pings, get_stops, get_tripstops
from pyproj import Proj
from save_and_load_variables import read_from_pickle, write_to_pickle
from scipy.spatial import cKDTree
from trip_helper import get_trip_cycle
from utility import latlng_distance, flatten

p = Proj(init='epsg:3414')

def get_kd_tree(trip_id, date_time=datetime.now()):
    kd_tree_filename = 'preprocessed/kdtree-pings-{}.pickle'.format(trip_id)
    kd_tree = None
    try:
        kd_tree = read_from_pickle(kd_tree_filename)
    except:
        trip_pings = get_pings(trip_id=trip_id, newest_datetime=date_time)
        cleaned_trip_pings = flatten(clean_rep(trip_pings))
        if len(cleaned_trip_pings) == 0:
            return None
        trip_pings_x_y = [p(ping.lng, ping.lat) for ping in cleaned_trip_pings]
        kd_tree = cKDTree(trip_pings_x_y)

        # Cache data if it is more than 1 day old
        latest_ping_time = trip_pings.iloc[-1].time.replace(tzinfo=None)
        if latest_ping_time < date_time and latest_ping_time.day != date_time.day:
            write_to_pickle(kd_tree_filename, kd_tree)
    return kd_tree

# For each tripstop, find the pings within 50m from it.
# Output format: [tripstop_id: nearest_ping_id]
def list_of_nearest_pings_to_tripstops(trip_id, date_time=datetime.now()):
    filename = 'preprocessed/nearest-pings-to-tripstops-{}.pickle'.format(trip_id)
    try:
        return read_from_pickle(filename)
    except:
        threshold_distance = 50 # If the ping is more than 50m away, it's not 'near' the bus stop
    
        trip_pings = get_pings(trip_id=trip_id, newest_datetime=date_time)
        cleaned_trip_pings = flatten(clean_rep(trip_pings))
        trip_pings_lat_lng = [(ping.lat, ping.lng) for ping in cleaned_trip_pings]
        
        trip_tripstops = get_tripstops(trip_id=trip_id)
        trip_tripstops_lat_lng = [(tripstop.lat, tripstop.lng) for tripstop in trip_tripstops.itertuples()]
        trip_tripstops_x_y = [p(lng, lat) for lat, lng in trip_tripstops_lat_lng]

        kd_tree = get_kd_tree(trip_id, date_time=date_time)
        
        ping_indices_per_tripstop = [kd_tree.query_ball_point(tripstop_x_y, r=threshold_distance) 
                                     for tripstop_x_y in trip_tripstops_x_y]

        sorted_tripstop_to_nearest_ping = []
        for tripstop_index, ping_indices in enumerate(ping_indices_per_tripstop):
            if len(ping_indices) == 0:
                continue
    
            distances = [latlng_distance(trip_tripstops_lat_lng[tripstop_index], trip_pings_lat_lng[ping_index]) 
                         for ping_index in ping_indices]
            nearest_pings_timings = [cleaned_trip_pings[ping_index].time for ping_index in ping_indices]
            
            # Get best ping based on least distance. If tie, get the earlier ping.
            # TODO: Improve heuristics in choosing the best ping (affinity by time, heading)
            nearest_ping_index = sorted(list(zip(distances, nearest_pings_timings, ping_indices)))[0][2]
            nearest_ping_id = cleaned_trip_pings[nearest_ping_index].Index
            tripstop_id = trip_tripstops.iloc[tripstop_index].name
            sorted_tripstop_to_nearest_ping.append((tripstop_id, nearest_ping_id))
        
        # Cache data if it is more than 1 day old
        latest_ping_time = trip_pings.iloc[-1].time.replace(tzinfo=None)
        if latest_ping_time < date_time and latest_ping_time.day != date_time.day:
            write_to_pickle(filename, sorted_tripstop_to_nearest_ping)
        return sorted_tripstop_to_nearest_ping

# This implementation is for circular routes where the stops cycles
# Output format: [stop_id: [nearby_ping_ids]]
def list_of_nearest_pings_to_stops(trip_id, date_time=datetime.now()):
    filename = 'preprocessed/nearest-pings-to-stops-{}.pickle'.format(trip_id)
    try:
        return read_from_pickle(filename)
    except:
        threshold_distance = 50 # If the ping is more than 50m away, it's not 'near' the bus stop

        trip_pings = get_pings(trip_id=trip_id, newest_datetime=date_time)
        cleaned_trip_pings = flatten(clean_rep(trip_pings))
        trip_pings_lat_lng = [(ping.lat, ping.lng) for ping in cleaned_trip_pings]
        
        trip_tripstops = get_tripstops(trip_id=trip_id)
        stop_ids_cycle = get_trip_cycle(trip_tripstops.stopId.tolist())
        stops = [get_stops(stop_id=stop_id) for stop_id in stop_ids_cycle]
        stops_lat_lng = [(stop.iloc[0].lat, stop.iloc[0].lng) for stop in stops]
        stops_x_y = [p(lng, lat) for lat, lng in stops_lat_lng]

        kd_tree = get_kd_tree(trip_id, date_time=date_time)

        ping_indices_per_stop = [kd_tree.query_ball_point(stop_x_y, r=threshold_distance)
                                     for stop_x_y in stops_x_y]

        sorted_stop_to_nearest_pings = [(stops[i].iloc[0].name,
                                         [cleaned_trip_pings[ping_index].Index
                                          for ping_index in ping_indices
                                          if ping_index < len(cleaned_trip_pings)])
                                         for i, ping_indices in enumerate(ping_indices_per_stop)]

        # Cache data if it is more than 1 day old
        latest_ping_time = trip_pings.iloc[-1].time.replace(tzinfo=None)
        if latest_ping_time < date_time and latest_ping_time.day != date_time.day:
            write_to_pickle(filename, sorted_stop_to_nearest_pings)
        return sorted_stop_to_nearest_pings
