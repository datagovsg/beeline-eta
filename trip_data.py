import pandas as pd
import shapely.wkb as wkb
from constants import DATETIME_FORMAT
from datetime import datetime, timedelta

decode_coordinates = lambda row: pd.Series(wkb.loads(row['coordinates'], hex=True).coords[0])
get_time = lambda row: datetime.strptime(row['time'] + '00', DATETIME_FORMAT + '+0000') + timedelta(hours=8)

def get_tripstops():
    try:
        return pd.read_pickle("preprocessed/tripstops.pkl")
    except:
        """TODO: Use database instead of CSV."""
        tripstops = pd.DataFrame.from_csv("data/tripStops.csv", sep=",")
        tripstops.columns = ['tripId', 'stopId', 'canBoard', 'canAlight', 
                             'time', 'createdAt', 'updatedAt']
        tripstops = tripstops[['tripId', 'stopId', 'canBoard', 'canAlight', 
                               'time']]

        tripstops['time'] = tripstops.apply(get_time, axis=1)

        stops = pd.DataFrame.from_csv("data/stops.csv", sep=",")
        stops.columns = ['description', 'road', 'label', 'postcode', 'type', 
                         'coordinates', 'viewUrl', 'createdAt', 'updatedAt']
        stops = stops[['coordinates']]

        stops[['lng', 'lat']] = stops.apply(decode_coordinates ,axis=1)
        del stops['coordinates']

        tripstops = pd.merge(stops, tripstops, right_on='stopId', left_index=True)

        tripstops = tripstops.sort_values(by=['tripId','time'])

        tripstops.to_pickle("preprocessed/tripstops.pkl")
        print("Saved tripstops")

        return tripstops

def get_trips():
    try:
        return pd.read_pickle("preprocessed/trips.pkl")
    except:
        """TODO: Use database instead of CSV"""
        trips = pd.DataFrame.from_csv("data/trips.csv", sep=",")
        trips.columns = ['date', 'capacity', 'status', 'price', 
                         'transportCompanyId', 'vehicleId', 'driverId', 'routeId',
                         'createdAt', 'updatedAt', 'bookingInfo', 'seatsAvailable']
        trips = trips[['date', 'routeId']]

        trips.to_pickle("preprocessed/trips.pkl")
        print("Saved trips")

        return trips

def get_routes():
    try:
        return pd.read_pickle("preprocessed/routes.pkl")
    except:
        """TODO: Use database instead of CSV"""
        routes = pd.DataFrame.from_csv("data/routes.csv", sep=",")
        routes.columns = ['name', 'from', 'to', 'path', 'transportCompanyId', 
                          'label', 'schedule', 'createdAt', 'updatedAt',
                          'tags', 'notes', 'features', 'companyTags']
        routes = routes[['name', 'from', 'to', 'path', 'label']]

        routes.to_pickle("preprocessed/routes.pkl")
        print("Saved routes")

        return routes

def get_pings():
    try:
        return pd.read_pickle("preprocessed/pings.pkl")
    except:
        """TODO: Use database instead of CSV"""
        pings = pd.DataFrame.from_csv("data/pings.csv", sep=",")
        pings.columns = ['coordinates', 'time', 'driverId', 'tripId', 
                         'vehicleId', 'status', 'createdAt', 'updatedAt']
        pings = pings[['coordinates', 'time', 'tripId']]

        pings[['lng', 'lat']] = pings.apply(decode_coordinates ,axis=1)
        del pings['coordinates']

        pings['time'] = pings.apply(get_time, axis=1)

        pings = pings.sort_values(by=['tripId','time'])

        pings.to_pickle("preprocessed/pings.pkl") # Because trying to get latlng and adding a time row is particularly nasty.
        print("Saved pings")

        return pings

tripstops = get_tripstops()
trips = get_trips()
routes = get_routes()
pings = get_pings()
