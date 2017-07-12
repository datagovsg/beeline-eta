import global_data
import time
from constants import DATETIME_FORMAT
from datetime import datetime, timedelta
from db_logic import destroy_and_recreate, setup_data, update_pings
from run import run

# Execute the function 'run' every 60 seconds.
# If 'run' takes more than 60 seconds to finish,
# it will wait until the next available start of cycle to execute 'run' again.
def run_forever(seconds=60, offset=timedelta()):
    while True:
        time.sleep(seconds - (time.time() % seconds))
        date_time = datetime.fromtimestamp(time.time())
        date_time = date_time - offset
        print(date_time)

        # Update routes, tripstops, trips every midnight
        if date_time.hour == 0 and date_time.minute <= 2:
            destroy_and_recreate()

        # Update pings every minute
        update_pings(date_time)

        run(date_time)

def replay_date():
    latest_known_date = datetime.strptime('2017-07-06 09:38:43+00', DATETIME_FORMAT + '+00')
    time_diff = datetime.now() - latest_known_date
    time_diff = time_diff + timedelta(minutes=20)
    run_forever(offset = time_diff)

if __name__ == '__main__':
    setup_data()
    #run_forever()
    replay_date()