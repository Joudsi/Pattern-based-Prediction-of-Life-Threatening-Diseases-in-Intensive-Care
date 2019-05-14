import numpy as np
import pandas as pd

df = pd.read_csv("/Users/joudsi/Desktop/output.csv")

is_AKI = df[df['is_AKI'] == 1]

is_bb = df[df['is_Betablocker'] == 1]

is_AKI_bb = df[ (df['is_AKI'] ==1) & (df['is_Betablocker'] == 1) ]

is_AKI_Not_bb = df[ (df['is_AKI'] ==1) & (df['is_Betablocker'] == 0) ]

# print(is_AKI.index)
# print(is_bb.index)
# print(is_AKI_bb.index)
# print(is_AKI_Not_bb.index)

print('Number of observations for patients with sepsis: ' + str(len(df)))
print('########################################################################')
print( 'Number of sepsis patients with AKI: ' + str(len(is_AKI)))
print( 'Number of sepsis patients Who took bb treatment: ' + str(len(is_bb)))
print( 'Number of sepsis patients with AKI and took bb: ' + str(len(is_AKI_bb)))
print( 'Number of sepsis patients with AKI and didn\'t took bb: ' + str(len(is_AKI_Not_bb)))
print('########################################################################')
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
print(df.describe())