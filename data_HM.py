import pandas as pd

df = pd.read_csv(r'/Users/student/Dropbox/PhD/2019 Fall/Hydrologic Modeling/Onion_Creek_Full_Time.csv', index_col=0)
df.index = pd.to_datetime(df.index)

monthly_df = df.groupby(df.index.strftime("%Y/%m")).sum()
monthly_df.index = pd.to_datetime(monthly_df.index)
print(monthly_df)

monthly_df.to_csv("/Users/student/Dropbox/PhD/2019 Fall/Hydrologic Modeling/Onion_Creek_Monthly.csv", index_label="Datetime")