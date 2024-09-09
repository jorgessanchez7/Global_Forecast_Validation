import hydrostats
import pandas as pd
from sklearn.metrics import accuracy_score  # Use appropriate metric for your task, e.g., mean_squared_error for regression
from sklearn.ensemble import RandomForestRegressor  # Use RandomForestRegressor for regression tasks
from sklearn.ensemble import RandomForestClassifier  # Use RandomForestRegressor for regression tasks
from sklearn.model_selection import train_test_split

# Load the dataset
file_path = '/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/World_Total_Correlations_Model2_v1.csv'  # Update this to your dataset's actual path
data = pd.read_csv(file_path)

# Assuming the last column is the target variable and all others are predictors
X = data.copy()
X.set_index('id', inplace=True)
# Selecting all columns except the last one as features
X = pd.DataFrame(X.drop(columns=['KGE_diff']))

y2 = data['KGE_diff']  # Selecting the other variable to be predicted

# Splitting the dataset into training and test sets
X_train, X_test, y2_train, y2_test = train_test_split(X, y2, test_size=0.2, random_state=42)

# Initialize and train the Random Forest model
model2 = RandomForestRegressor(random_state=42)  # Use RandomForestRegressor for a regression task
model2.fit(X_train, y2_train)

# After fitting the model
feature_importances_model2 = model2.feature_importances_

# Get feature names
feature_names = X.columns

'''
print("\nFeature importances for Model 2 (KGE_diff):")
for name, importance in zip(feature_names, feature_importances_model2):
    print(f"{name}: {importance}")
'''

# If you want to sort the features by importance
sorted_indices_model2 = feature_importances_model2.argsort()[::-1]

print("\nSorted Feature importances for Model 2 (KGE_diff):")
for idx in sorted_indices_model2:
    print(f"{feature_names[idx]}: {round(feature_importances_model2[idx],10)}")

# Predicting the test set results
y2_pred = model2.predict(X_test)

# Evaluate the model
root_mean_squared_error_model2 = hydrostats.metrics.rmse(y2_test.to_list(), y2_pred.tolist())
print(f"RMSE Model 2: {root_mean_squared_error_model2}")

mean_squared_error_model2 = hydrostats.metrics.mse(y2_test.to_list(), y2_pred.tolist())
print(f"MSE Model 2: {mean_squared_error_model2}")

normalized_root_mean_squared_error_model2 = hydrostats.metrics.nrmse_mean(y2_test.to_list(), y2_pred.tolist())
print(f"Normalized RMSE (Mean) Model 2: {normalized_root_mean_squared_error_model2}")

nse_model2 = hydrostats.metrics.nse(y2_test.to_list(), y2_pred.tolist())
print(f"NSE Model 2: {nse_model2}")

pearson_correlation_model2 = hydrostats.metrics.pearson_r(y2_test.to_list(), y2_pred.tolist())
print(f"Pearson Correlation Model 2: {pearson_correlation_model2}")

kge_model2 = hydrostats.metrics.kge_2012(y2_test.to_list(), y2_pred.tolist())
print(f"KGE Model 2: {kge_model2}")

# Note: If you're working on a regression task, replace RandomForestClassifier with RandomForestRegressor
# and accuracy_score with an appropriate regression metric, like mean_squared_error or r2_score.