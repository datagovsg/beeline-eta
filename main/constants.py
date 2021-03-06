API_KEY = 'AIzaSyCxvr7c1N-0YuzYkEl9k9Z3n9RCA9tEL4c'
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
DATETIME_FORMAT = ' '.join([DATE_FORMAT, TIME_FORMAT])
MAX_TIME = 900 # Not predicting anything over 10 mins (we declare as >10 mins)

COLUMN_NAMES_ROUTES = ['id', 'path']
COLUMN_NAMES_TRIPS = ['id', 'date', 'routeId']
COLUMN_NAMES_TRIPSTOPS = ['id', 'tripId', 'stopId', 'canBoard', 'canAlight', 'time', 'lng', 'lat']
COLUMN_NAMES_PINGS = ['id', 'lng', 'lat', 'time', 'tripId']
COLUMN_NAMES_STOPS = ['id', 'heading', 'lng', 'lat']
