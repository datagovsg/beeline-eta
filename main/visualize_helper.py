import folium
import pandas as pd
import bisect

get_lat_lng_pairs = lambda df: list(zip(df['lat'], df['lng']))

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