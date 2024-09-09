import hydrostats
import pandas as pd
from sklearn.metrics import accuracy_score  # Use appropriate metric for your task, e.g., mean_squared_error for regression
from sklearn.ensemble import RandomForestRegressor  # Use RandomForestRegressor for regression tasks
from sklearn.ensemble import RandomForestClassifier  # Use RandomForestRegressor for regression tasks
from sklearn.model_selection import train_test_split

# Load the dataset
file_path = '/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/World_Total_Correlations_Model1_v3.csv'  # Update this to your dataset's actual path
data = pd.read_csv(file_path)

# Assuming the last column is the target variable and all others are predictors
X = data.copy()
X.set_index('id', inplace=True)
# Selecting all columns except the last one as features
X = pd.DataFrame(X.drop(columns=['KGE_Corrected']))

y1 = data['KGE_Corrected']  # Selecting one of the variables to be predicted

# Splitting the dataset into training and test sets
X_train, X_test, y1_train, y1_test = train_test_split(X, y1, test_size=0.2, random_state=42)

# Initialize and train the Random Forest model
model1 = RandomForestRegressor(random_state=42)  # Use RandomForestRegressor for a regression task
model1.fit(X_train, y1_train)

# After fitting the model
feature_importances_model1 = model1.feature_importances_

# Get feature names
feature_names = X.columns

'''
# Print the feature importances
print("Feature importances for Model 1 (KGE_Corrected):")
for name, importance in zip(feature_names, feature_importances_model1):
    print(f"{name}: {importance}")
'''

# If you want to sort the features by importance
sorted_indices_model1 = feature_importances_model1.argsort()[::-1]

print("\nSorted Feature importances for Model 1 (KGE_Corrected):")
for idx in sorted_indices_model1:
    print(f"{feature_names[idx]}: {round(feature_importances_model1[idx],10)}")

# Predicting the test set results
y1_pred = model1.predict(X_test)

# Evaluate the model
root_mean_squared_error_model1 = hydrostats.metrics.rmse(y1_test.to_list(), y1_pred.tolist())
print(f"RMSE Model 1: {root_mean_squared_error_model1}")

mean_squared_error_model1 = hydrostats.metrics.mse(y1_test.to_list(), y1_pred.tolist())
print(f"MSE Model 1: {mean_squared_error_model1}")

normalized_root_mean_squared_error_model1 = hydrostats.metrics.nrmse_mean(y1_test.to_list(), y1_pred.tolist())
print(f"Normalized RMSE (Mean) Model 1: {normalized_root_mean_squared_error_model1}")

nse_model1 = hydrostats.metrics.nse(y1_test.to_list(), y1_pred.tolist())
print(f"NSE Model 1: {nse_model1}")

pearson_correlation_model1 = hydrostats.metrics.pearson_r(y1_test.to_list(), y1_pred.tolist())
print(f"Pearson Correlation Model 1: {pearson_correlation_model1}")

kge_model1 = hydrostats.metrics.kge_2012(y1_test.to_list(), y1_pred.tolist())
print(f"KGE Model 1: {kge_model1}")

# Note: If you're working on a regression task, replace RandomForestClassifier with RandomForestRegressor
# and accuracy_score with an appropriate regression metric, like mean_squared_error or r2_score.