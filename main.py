import time
from datetime import datetime, timedelta
from run import run

# Execute the function 'run' every 60 seconds.
# If 'run' takes more than 60 seconds to finish,
# it will wait until the next available start of cycle to execute 'run' again.
def run_forever(seconds=60):
    while True:
        time.sleep(seconds - (time.time() % seconds))
        date_time = datetime.fromtimestamp(time.time())
        run(date_time)

if __name__ == '__main__':
	run_forever()