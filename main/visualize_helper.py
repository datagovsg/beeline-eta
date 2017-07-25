import bisect
import folium
import pandas as pd
import pytz
from clean_data import clean_rep
from constants import TIME_FORMAT
from datetime import datetime, timedelta
from db_logic import get_pings, get_tripstops
from test import get_actual_arrival_timing, get_predicted_arrival_timing
from trip_helper import get_most_recent_pings
from utility import flatten

get_lat_lng_pairs = lambda df: list(zip(df['lat'], df['lng']))

####################
# Usable functions #
####################
# This is for visualization of cleaned pings (discontinuous polyline)
def see_trip_parts(trip_id):
    cleaned_trip_pings_parts = clean_rep(get_pings(trip_id=trip_id))
    m = folium.Map(location=(1.38, 103.8), zoom_start=12)
    for trip_ping_part in cleaned_trip_pings_parts:
        add_polyline(m, [(ping.lat, ping.lng) for ping in trip_ping_part])

    filename = 'visualizations/cleaned-pings-parts-{}.html'.format(trip_id)
    m.save(filename)
    print('Saved file at {}'.format(filename))

# This is for visualization of general pings and tripstops
# Input:
#     trip_id: Can get the trip pings and tripstops automatically
#     date_time: Add a red marker showing where the bus is at this timing
#     next_number_of_minutes: Add green marker showing where the bus is at each minute,
#                             starting from date_time
def see_trip_wrapper(trip_id, date_time=None, stop_id=None, next_number_of_minutes=0):
    pings = get_pings(trip_id=trip_id,
                      newest_datetime=date_time + timedelta(minutes=next_number_of_minutes) if date_time else datetime.now())
    pings['time'] = pings['time'].apply(lambda date_time: date_time.replace(tzinfo=None))
    tripstops = get_tripstops(trip_id=trip_id)
    tripstops['time'] = tripstops['time'].apply(lambda date_time: date_time.replace(tzinfo=None))
    m = see_trip(pings, tripstops, date_time=date_time)

    filename = 'visualizations/trip-{}'.format(trip_id)
    
    if stop_id and date_time and next_number_of_minutes:
        add_marker_per_minute(m, trip_id, stop_id, date_time, next_number_of_minutes)
        filename += '-stop-{}-on-{}'.format(trip_id, date_time)
    elif date_time:
        filename += '-on-{}'.format(date_time)

    filename += '.html'

    m.save(filename)
    print('Saved file at {}'.format(filename))

####################
# Helper functions #
####################
def add_polyline(folium_map, lat_lng_pairs):
    folium_map.add_child(folium.PolyLine(lat_lng_pairs))

def add_marker(folium_map, lat_lng_pair, folium_popup=None, folium_icon=None):
    folium_map.add_child(
        folium.Marker(lat_lng_pair, popup=folium_popup, icon=folium_icon))

def add_markers(folium_map, lat_lng_pairs, stop_ids):
    for i in range(len(lat_lng_pairs)):
        add_marker(folium_map, lat_lng_pairs[i], folium_popup=folium.Popup(
                folium.Html('<b>' + str(i+1) + '</b>. ' + str(stop_ids.iloc[i]), script=True)))

def see_trip(trip_pings, trip_tripstops, date_time=None):
    m = folium.Map(location=(1.38, 103.8), zoom_start=12)

    # Add polyline for pings
    add_polyline(m, get_lat_lng_pairs(trip_pings))

    # Add blue (default) marker for tripstops
    add_markers(m, get_lat_lng_pairs(trip_tripstops), trip_tripstops['stopId'])

    # Add red marker for current location (if date_time is provided)
    if date_time:
        index = min(bisect.bisect(trip_pings.time.tolist(), date_time), len(trip_pings) - 1)
        if index != -1:
            point = (trip_pings.iloc[index].lat, trip_pings.iloc[index].lng)
            add_marker(m, point, folium_icon=folium.Icon(color='red'))
    return m

def add_marker_per_minute(folium_map, trip_id, stop_id, date_time, next_number_of_minutes):
    for i in range(next_number_of_minutes + 1):
        current_datetime = date_time + timedelta(minutes=i)
        recent_pings = get_most_recent_pings(trip_id, current_datetime)
        if len(recent_pings) == 0:
            continue
        point = (recent_pings.iloc[0].lat, recent_pings.iloc[0].lng)
        actual_arrival_time = get_actual_arrival_timing(trip_id, stop_id, current_datetime)
        predicted_arrival_time = get_predicted_arrival_timing(trip_id, stop_id, current_datetime)
        add_marker(folium_map, 
                   point, 
                   folium_icon=folium.Icon(color='green'), 
                   folium_popup=folium.Popup(
                    folium.Html(
                        "<b>Stop id: {}</b><br>Current time: {}<br>Predicted: {}<br>Actual: {}".format(
                            stop_id,
                            current_datetime.strftime(TIME_FORMAT),
                            predicted_arrival_time.strftime(TIME_FORMAT) if predicted_arrival_time else '',
                            actual_arrival_time.strftime(TIME_FORMAT) if actual_arrival_time else ''
                        ), script=True)))

# Usage:
# print(see_trip_parts(18258))
# print(see_trip_wrapper(18258))
# print(see_trip_wrapper(18258, date_time=datetime(2017, 7, 6, 9, 28, 43)))
# print(see_trip_wrapper(18258, date_time=datetime(2017, 7, 6, 9, 28, 43), stop_id=4468, next_number_of_minutes=8))
# print(see_trip_wrapper(15335, date_time=datetime(2017, 6, 15, 14, 25, 0), stop_id=1147, next_number_of_minutes=7))
