import math
from datetime import datetime, timedelta

# Transpose a 2D-list
transpose = lambda l: list(zip(*l))

# Flatten a 2D-list to 1D-list
flatten = lambda l: [item for sublist in l for item in sublist]

# Check if a variable is NaN
is_nan = lambda val: val != val

# Check if a list is sorted
is_sorted = lambda lst: all(lst[i] <= lst[i+1] for i in range(len(lst)-1))

# Takes a list and an index; Splits the list into two parts at that index.
split_list = lambda list_index: [list_index[0][:list_index[1]], list_index[0][list_index[1]:]]

# Takes a list and a value; Returns the first index of the list that is greater than the value. 
find_first_index_greater_than = lambda list_value: next((index for index, value in enumerate(list_value[0]) if value > list_value[1]), None)

# Compute distance given two coordinates in (x, y) form
def xy_distance(xy1, xy2):
    diff_x = xy1[0] - xy2[0]
    diff_y = xy1[1] - xy2[1]
    return math.sqrt(diff_x * diff_x + diff_y * diff_y)

# Compute bearing from first point to second point in (x, y) form
def xy_bearing(xy1, xy2):
    theta = math.atan2(xy2[0] - xy1[0], xy2[1] - xy1[1])
    if theta < 0.0:
        theta += 2 * math.pi
    return theta * 180 / math.pi

# Compute distance given two lat lngs
def latlng_distance(ll1, ll2):
    rr_1 = [ll1[0] / 180 * math.pi, ll1[1] / 180 * math.pi]
    rr_2 = [ll2[0] / 180 * math.pi, ll2[1] / 180 * math.pi]
    dx = (rr_1[1] - rr_2[1]) * math.cos(0.5 * (rr_1[0] + rr_2[0]))
    dy = rr_1[0] - rr_2[0]
    dist = math.sqrt(dx * dx + dy * dy) * 6371000
    return dist

# Compute bearing between two lat lngs (in degrees from 0 to 359)
# Code taken from https://gist.github.com/jeromer/2005586
def latlng_bearing(pointA, pointB):
    """
    Calculates the bearing between two points.
    The formulae used is the following:
        θ = atan2(sin(Δlong).cos(lat2),
                  cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
    :Parameters:
      - `pointA: The tuple representing the latitude/longitude for the
        first point. Latitude and longitude must be in decimal degrees
      - `pointB: The tuple representing the latitude/longitude for the
        second point. Latitude and longitude must be in decimal degrees
    :Returns:
      The bearing in degrees
    :Returns Type:
      float
    """
    lat1 = math.radians(pointA[0])
    lat2 = math.radians(pointB[0])

    diffLong = math.radians(pointB[1] - pointA[1])

    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1)
            * math.cos(lat2) * math.cos(diffLong))

    initial_bearing = math.atan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing

# To handle Google Maps API in development mode
def get_future_datetime(old_datetime, is_production=False):
    if is_production:
        return datetime.now() + timedelta(seconds=5) # Predict from 5 seconds later just in case API throws error
    else:
        temp_datetime = old_datetime
        now = datetime.now()
        while (temp_datetime <= now): # While it is still in the past, +1 week
            temp_datetime += timedelta(weeks=1)
        return temp_datetime

###
# Test split_list and find_first_index_greater_than

# a = [[0, 1, 2, 3, 4, 5, 6, 7]]
# a = a[:-1] + split_list((a[-1], 2)) # [[0, 1], [2, 3, 4, 5, 6, 7]]
# a = a[:-1] + split_list((a[-1], 0)) # [[0, 1], [], [2, 3, 4, 5, 6, 7]]
# a = a[:-1] + split_list((a[-1], 3)) # [[0, 1], [], [2, 3, 4], [5, 6, 7]]
# a = a[:-1] + split_list((a[-1], 1)) # [[0, 1], [], [2, 3, 4], [5], [6, 7]]
# a = a[:-1] + split_list((a[-1], 4)) # [[0, 1], [], [2, 3, 4], [5], [6, 7], []]
# print(a)

# b = [2, 4, 5, 3, 9]
# print(find_first_index_greater_than((b, 4))) # 2
# print(find_first_index_greater_than((b, 9))) # None (default value)
