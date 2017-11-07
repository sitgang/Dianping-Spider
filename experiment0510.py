# -*- coding: utf-8 -*-
import pickle,datetime
with open('/Users/xuegeng/Spider_Workspace/crawler/shop_datas.pkl','r') as f:
    df = pickle.load(f)#得到数据df

import numpy as np
import pandas as pd
import seaborn as sns
from scipy import stats
from scipy.stats import kendalltau
import matplotlib as mpl
import matplotlib.pyplot as plt

#sns.set(style="ticks")
#sns.jointplot(df.taste, df.total_rank, kind="hex", stat_func=kendalltau, color="#4CB391")
#sns.jointplot(df.serv, df.total_rank, kind="hex", stat_func=kendalltau, color="blue")
#sns.jointplot(df.env, df.total_rank, kind="hex", stat_func=kendalltau, color="red")
#sns.jointplot(df.taste, df.total_rank, kind="reg", stat_func=kendalltau, color="#4CB391")
#sns.jointplot(df.serv, df.total_rank, kind="reg", stat_func=kendalltau, color="blue")
#sns.jointplot(df.env, df.total_rank, kind="reg", stat_func=kendalltau, color="red")

import seaborn as sns
sns.set()

# Load the example flights dataset and conver to long-form
flights_long = sns.load_dataset("flights")
flights = flights_long.pivot("month", "year", "passengers")
#
## Draw a heatmap with the numeric values in each cell
sns.heatmap(flights, annot=True, fmt="d", linewidths=.5)

sr = df.monthly_comm_dense
dl = []
for k,v in sr.iteritems():
    d = dict(v)
    dl.append(d)
dd = pd.DataFrame(dl)
avg = dd.mean()

avg.index = pd.DatetimeIndex(avg.index)
avgdf = pd.DataFrame([list(avg.index),list(avg.values)]).T
avgdf.columns = ['date','mcd']
avgdf['year'] = map(lambda x:x.year,avgdf.date)
avgdf['month'] = map(lambda x:x.month,avgdf.date)
#avgdf['month'] = map(lambda x:datetime.datetime.strftime(x,'%B'),avgdf.date)
avgm = avgdf.pivot("month", "year", "mcd")
avgm = avgm.replace(np.nan,0)

sns.heatmap(avgm, annot=True, linewidths=.5)
plt.show()

df['comm_dense_before'] = df.comm_dense_before_after.apply(lambda x:x[0])
df['comm_dense_after'] = df.comm_dense_before_after.apply(lambda x:x[1])
sns.jointplot(df['comm_dense_before'], df['comm_dense_after'], kind="reg", stat_func=kendalltau, color="#4CB391")



import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="darkgrid", palette="Set2")

# Create a noisy periodic dataset
sines = []
rs = np.random.RandomState(8)
for _ in range(15):
    x = np.linspace(0, 30 / 2, 30)
    y = np.sin(x) + rs.normal(0, 1.5) + rs.normal(0, .3, 30)
    sines.append(y)

# Plot the average over replicates with bootstrap resamples
sns.tsplot(sines, err_style="boot_traces", n_boot=500)
plt.show()