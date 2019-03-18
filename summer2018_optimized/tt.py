import matplotlib.pyplot as plt
from scipy.stats import gamma
import numpy as np

#import unidecode

#print(unidecode.unidecode("türkmen"))

n, bins, patches = plt.hist(t_array, bins=20)
plt.show()
fig, ax = plt.subplots(1, 1)
a = 1.99
mean, var, skew, kurt = gamma.stats(a, moments='mvsk')
x = np.linspace(gamma.ppf(0.01, a), gamma.ppf(0.99, a), 100)
ax.plot(x, gamma.pdf(x, a), 'r-', lw=5, alpha=0.6, label='gamma pdf')
rv = gamma(a)
ax.plot(x, rv.pdf(x), 'k-', lw=2, label='frozen pdf')
r = gamma.rvs(a, size=1000)
ax.hist(r, density=True, histtype='stepfilled', alpha=0.2)
plt.show()