import time
from constants import API_RESULT_FILE
from save_and_load_variables import read_from_pickle
from datetime import datetime, timedelta
from run import run

# Execute the function 'run' every 60 seconds.
# If 'run' takes more than 60 seconds to finish,
# it will wait until the next available start of cycle to execute 'run' again.
def run_forever(seconds=60):
    start_time = time.time()
    while True:
        date_time = datetime.fromtimestamp(time.time())
        run(date_time)
        time.sleep(seconds- ((time.time() - start_time) % seconds))

if __name__ == '__main__':
	run_forever()