import matplotlib.pyplot as plt
import numpy as np
from constants import MAX_TIME
from math import ceil

def draw_confidence_interval(test_vs_predicted, color, label, low_high_percentile=[0.05, 0.95], alpha=0.3):
    test_vs_predicted.sort()
    baskets = []
    for i in range(MAX_TIME // 20 + 1):
        baskets.append([])
    for pair in test_vs_predicted:
        if pair[0] <= MAX_TIME:
            baskets[ceil(pair[0] / 20)].append(pair[1] - pair[0])
    low_percentiles = []
    high_percentiles = []
    for row in baskets:
        if len(row) == 0:
            low_percentiles.append(0)
            high_percentiles.append(0)
        else:
            row = np.array(row)
            low_percentiles.append(np.percentile(row, low_high_percentile[0] * 100))
            high_percentiles.append(np.percentile(row, low_high_percentile[1] * 100))
    timings = list(range(0, MAX_TIME + 1, 20))
    plt.plot(timings, low_percentiles, color, label=label)
    plt.plot(timings, high_percentiles, color)
    plt.fill_between(timings, low_percentiles, high_percentiles, color=color, alpha=alpha)

# Example on draw_confidence_interval
# plt.clf()
# test_vs_predicted = [(1, 2), (2, 3), (1, 5), (4, 2), (2, 4), (2, 2), (3, 5)]
# # Can stack a few confidence intervals here
# draw_confidence_interval(test_vs_predicted, 'orange', 'some method')
# plt.plot([0, MAX_TIME], [0, 0], c='black')
# plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
#            ncol=3, mode="expand", borderaxespad=0.)
# plt.savefig('compare.png')
# print('Saved as compare.png')