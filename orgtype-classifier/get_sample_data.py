import pandas as pd
from collections import Counter

df = pd.read_csv('./combined_training/combinedtrain.csv')
df = df.sample(frac=1)
dfset = df['cls'].unique()

dfc = pd.DataFrame()

for cls in dfset:
    dfa = df[df['cls'] == cls]
    for i in range(500):

