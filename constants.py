API_KEY = 'AIzaSyCxvr7c1N-0YuzYkEl9k9Z3n9RCA9tEL4c'
DATE_FORMAT = '%Y-%m-%d'
TIME_FORMAT = '%H:%M:%S'
DATETIME_FORMAT = ' '.join([DATE_FORMAT, TIME_FORMAT])
API_RESULT_FILE = 'results/api-result.pickle'
MAX_TIME = 900 # Not predicting anything over 10 mins (we declare as >10 mins)