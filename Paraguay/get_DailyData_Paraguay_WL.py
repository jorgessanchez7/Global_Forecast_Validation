import pandas as pd

hourly_df = pd.read_csv("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Paraguay/row_data/3862_WL.csv", index_col=0)
hourly_df.index = pd.to_datetime(hourly_df.index)

print(hourly_df)

daily_df = hourly_df.groupby(hourly_df.index.strftime("%Y-%m-%d")).mean()
daily_df.index = pd.to_datetime(daily_df.index)
print(daily_df)

daily_df.to_csv("/Volumes/GoogleDrive/My Drive/PhD/2022_Winter/Dissertation_v13/South_America/Paraguay/data/historical/Observed_Data_WL/3862.csv", index_label="Datetime")
