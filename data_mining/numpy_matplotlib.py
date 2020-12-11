import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt

x=np.arange(-1,1,0.02)
y=np.arcsin(x)

plt.title("Matplotlib Demo")
plt.xlabel("x axis caption")
plt.ylabel("y axis caption")
plt.plot(x,y)
plt.show()


