

import numpy as np
import math
import matplotlib.pyplot as plt

_, axis = plt.subplots()

t = np.arange(0., 10000.)

x = np.cos(t) + 2*math.sqrt(2.0)*np.cos(t/2.0)
y = np.sin(t)

plt.plot(x, y, 'b+:')
plt.show()

