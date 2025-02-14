import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy as sp

df=pd.read_csv(r"C:\Users\Post Lab\Desktop\New folder\Pakistan Largest Ecommerce Dataset.csv")

print(df.head())
print(df.tail(6))
print(df.info())

# summary of dataset
print(df.describe())

#to display the shape of the dataset interms of row and column
print(f"In this dataset numbers of rows {df.shape[0]} and numbers of columns {df.shape[1]}")

print(df.isnull().sum())

missing_percentage=df.isnull().sum()/len(df)*100
missing_percentage.plot(kind='bar')
# add labels
plt.xlabel('columns')
plt.ylabel('percentage')
plt.title("percentage of missing values in each column")
plt.show()


#Define columns to drop
columns_to_drop = ["Unnamed: 21" , "Unnamed: 22" ,"Unnamed: 23" , "Unnamed: 24" ,"Unnamed: 25", "sales_commission_code"]
# Drop columns
df.drop(columns=columns_to_drop, inplace=True) 

#shappe of dataset after columns are dropede 
print(f"After remove columns , In this dataset numbers of rows {df.shape[0]} and numbers of columns {df.shape[1]}")


df.isnull().sum().sort_values(ascending=True)


drop_Nan_rows = df.dropna()

print(drop_Nan_rows.shape)

print(drop_Nan_rows.duplicated().sum())

print(drop_Nan_rows.info())


#now the dataset doesn't have any missing values, i droped rows with missing value
#The right way of cleaning the data is to drop the row containing missing datas but for education pupose and we are asked minimum of 1M rows i will fill NaN values with upper rows value


df.fillna(method = 'ffill', inplace=True)

print(df.shape)

print(df.isnull().sum())


#now the dataset is cleaned, it has no missing value and it contains 1M rows

#to conver those date related datas to datetime datatype
df['Working Date'] = pd.to_datetime(df['Working Date'])
df['Customer Since'] = pd.to_datetime(df['Customer Since'])
df['created_at'] = pd.to_datetime(df['created_at'])
df.describe(include=["object"])

#cleaned_df =df.to_csv(r"C:\Users\Post Lab\Desktop\New folder\E-commerce dataset1.csv")