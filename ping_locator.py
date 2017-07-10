import pandas
from clean_data import clean_rep
from pyproj import Proj
from save_and_load_variables import read_from_pickle, write_to_pickle
from scipy.spatial import cKDTree
from trip_data import pings, tripstops
from trip_helper import get_trip_pings, get_trip_tripstops
from utility import latlng_distance, flatten

p = Proj(init='epsg:3414')

def get_kd_tree(trip_id):
    kd_tree_filename = 'preprocessed/kdtree-pings-{}.pickle'.format(str(trip_id))
    kd_tree = None
    try:
        kd_tree = read_from_pickle(kd_tree_filename)
    except:
        trip_pings = get_trip_pings(trip_id)
        cleaned_trip_pings = flatten(clean_rep(trip_pings))
        if len(cleaned_trip_pings) == 0:
            return None
        trip_pings_lat_lng = [(ping.lat, ping.lng) for ping in cleaned_trip_pings]
        trip_pings_x_y = [p(lng, lat) for lat, lng in trip_pings_lat_lng]
        kd_tree = cKDTree(trip_pings_x_y)
        write_to_pickle(kd_tree_filename, kd_tree)
    return kd_tree

def get_sorted_tripstop_nearest_pings(nearest_pings_id_per_tripstop_id):
    timings_tripstop_ids_pings = [(tripstops.loc[tripstop_id].time, tripstop_id, pings.loc[ping_id]) 
                                  for tripstop_id, ping_id in nearest_pings_id_per_tripstop_id.items()]
    return [(tripstop_id, ping) for timing, tripstop_id, ping in sorted(timings_tripstop_ids_pings)]

# For each tripstop, find the pings within 50m from it.
def list_of_nearest_pings_to_tripstops(trip_id):
    threshold_distance = 50 # If the ping is more than 50m away, it's not 'near' the bus stop
    
    trip_pings = get_trip_pings(trip_id)
    cleaned_trip_pings = flatten(clean_rep(trip_pings))
    trip_pings_lat_lng = [(ping.lat, ping.lng) for ping in cleaned_trip_pings]
    trip_pings_x_y = [p(lng, lat) for lat, lng in trip_pings_lat_lng]
    
    trip_tripstops = get_trip_tripstops(trip_id)
    trip_tripstops_lat_lng = [(tripstop.lat, tripstop.lng) for tripstop in trip_tripstops.itertuples()]
    trip_tripstops_x_y = [p(lng, lat) for lat, lng in trip_tripstops_lat_lng]

    filename = 'preprocessed/nearest-pings-to-tripstops-{}.pickle'.format(str(trip_id))

    try:
        return read_from_pickle(filename)
    except:
        kd_tree = get_kd_tree(trip_id)
        
        ping_indices_per_tripstop = [kd_tree.query_ball_point(tripstop_x_y, r=threshold_distance) 
                                     for tripstop_x_y in trip_tripstops_x_y]

        nearest_ping_id_per_tripstop_id = {}
        for tripstop_index, ping_indices in enumerate(ping_indices_per_tripstop):
            if len(ping_indices) == 0:
                continue
    
            distances = [latlng_distance(trip_tripstops_lat_lng[tripstop_index], trip_pings_lat_lng[ping_index]) 
                         for ping_index in ping_indices]
            nearest_pings_timings = [cleaned_trip_pings[ping_index].time for ping_index in ping_indices]
            
            # Get best ping based on least distance. If tie, get the earlier ping.
            nearest_ping_index = sorted(list(zip(distances, nearest_pings_timings, ping_indices)))[0][2]
            nearest_ping_id = cleaned_trip_pings[nearest_ping_index].Index
            tripstop_id = trip_tripstops.iloc[tripstop_index].name
            nearest_ping_id_per_tripstop_id[tripstop_id] = nearest_ping_id
        
        write_to_pickle(filename, nearest_ping_id_per_tripstop_id)
        return nearest_ping_id_per_tripstop_id