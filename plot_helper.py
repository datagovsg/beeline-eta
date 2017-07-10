import matplotlib.pyplot as plt
from helper.constants import *
from helper.utility import percentile

def draw_confidence_interval(test_vs_predicted, color, label, low_high_percentile=[0.05, 0.95], alpha=0.3):
    test_vs_predicted.sort()
    baskets = []
    for i in range(MAX_TIME // 20 + 1):
        baskets.append([])
    for pair in test_vs_predicted:
        if pair[0] <= MAX_TIME:
            baskets[math.ceil(pair[0] / 20)].append(pair[1] - pair[0])
    low_percentiles = []
    high_percentiles = []
    for row in baskets:
        if len(row) == 0:
            low_percentiles.append(0)
            high_percentiles.append(0)
        else:
            low_percentiles.append(percentile(row, low_high_percentile[0]))
            high_percentiles.append(percentile(row, low_high_percentile[1]))
    timings = list(range(0, MAX_TIME + 1, 20))
    plt.plot(timings, low_percentiles, color, label=label)
    plt.plot(timings, high_percentiles, color)
    plt.fill_between(timings, low_percentiles, high_percentiles, color=color, alpha=alpha)