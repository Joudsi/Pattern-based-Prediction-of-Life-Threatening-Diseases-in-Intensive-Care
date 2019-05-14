import pandas as pd
from matplotlib import pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import statsmodels.api as sm
from scipy import stats
import numpy as np


df = pd.read_csv("/Users/joudsi/Desktop/output.csv")
print(df.head())

plt.scatter(df.is_Betablocker, df.is_AKI, marker = '+', color = 'red')
plt.show()
print(df.shape)

x_train, x_test, y_train, y_test = train_test_split(df[['is_Betablocker']], df.is_AKI, test_size = 0.1)

model = LogisticRegression()

model.fit(x_train, y_train)   #this is doing the training for the model

print(model.predict(x_test))

print(model.score(x_train, y_train))
print(model.score(x_test, y_test))

# print(model.get_params())

# print(model.predict_proba(x_test))

X2 = sm.add_constant(x_train)
est = sm.OLS(y_train, X2)
print('p-value is:')
print(est.fit().f_pvalue)