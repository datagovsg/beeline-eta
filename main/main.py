import os
import time
from constants import DATETIME_FORMAT
from datetime import datetime, timedelta
from db_logic import get_offset
from file_system import destroy_predictions
from run import run

# Execute the function 'run' every 60 seconds.
# If 'run' takes more than 60 seconds to finish,
# it will wait until the next available start of cycle to execute 'run' again.
def run_forever(seconds=60, offset=timedelta()):
    while True:
        time.sleep(seconds - (time.time() % seconds))
        date_time = datetime.now() - offset
        print(date_time)

        # Update routes, tripstops, trips every midnight
        if date_time.hour == 0 and date_time.minute <= 2:
            destroy_predictions()

        run(date_time)

def replay_date(minutes=10):
    offset = get_offset(minutes=minutes)
    run_forever(offset=offset)

if __name__ == '__main__':
    print('Welcome to the trip prediction background task')

    offset = os.environ.get('PLAYBACK_OFFSET', '')
    if offset == '':
        run_forever()
    else:
        replay_date(int(offset))
