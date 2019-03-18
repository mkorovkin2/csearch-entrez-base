import matplotlib.pyplot as plt
import pandas as pd

def reverse_interpolate(series):
    q = list()
    for i in range(len(series)):
        if i % 5 == 0:
            q.append(series[i])
            print(series[i])
        else:
            x = series[i - (i % 5)]
            y = series[i + (5 - (i % 5))]
            diff = float(y - x) / 5.0
            q.append(x + diff * (i % 5))
    return q

array = [1, 2, 2, 2, 4, 5, 4, 3, 1, 4, 7, 3, 5, 10, 7, 5, 3, 5, 7, 9, 10]
print(reverse_interpolate(array))

plt.scatter(range(len(array)), array)
plt.show()

plt.scatter(range(len(array)), reverse_interpolate(array))
plt.show()