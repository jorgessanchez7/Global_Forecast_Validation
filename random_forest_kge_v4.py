import shap
import hydrostats
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score  # Use appropriate metric for your task, e.g., mean_squared_error for regression
from sklearn.ensemble import RandomForestRegressor  # Use RandomForestRegressor for regression tasks
from sklearn.ensemble import RandomForestClassifier  # Use RandomForestRegressor for regression tasks
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor, plot_tree

# Load the dataset
file_path = '/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/World_Total_Correlations_Model1_v4.csv'  # Update this to your dataset's actual path
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

tree_model = DecisionTreeRegressor(random_state=42)
tree_model.fit(X_train, y1_train)

# Plot the tree
#plt.figure(figsize=(20,10))
#plot_tree(tree_model, filled=True, feature_names=[X.columns[0], X.columns[1], X.columns[2], X.columns[3]])
#plt.show()

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
y1_train_pred = model1.predict(X_train)

# Evaluate the model
root_mean_squared_error_model1_cal = hydrostats.metrics.rmse(y1_train_pred.tolist(), y1_train.to_list())
print(f"RMSE Model 1 (Calibration): {root_mean_squared_error_model1_cal}")
root_mean_squared_error_model1_val = hydrostats.metrics.rmse(y1_pred.tolist(), y1_test.to_list())
print(f"RMSE Model 1 (Validation): {root_mean_squared_error_model1_val}")

mean_squared_error_model1_cal = hydrostats.metrics.mse(y1_train_pred.tolist(), y1_train.to_list())
print(f"MSE Model 1 (Calibration): {mean_squared_error_model1_cal}")
mean_squared_error_model1_val = hydrostats.metrics.mse(y1_pred.tolist(), y1_test.to_list())
print(f"MSE Model 1 (Validation): {mean_squared_error_model1_val}")

normalized_root_mean_squared_error_model1_cal = hydrostats.metrics.nrmse_mean(y1_train_pred.tolist(), y1_train.to_list())
print(f"Normalized RMSE (Mean) Model 1 (Calibration): {normalized_root_mean_squared_error_model1_cal}")
normalized_root_mean_squared_error_model1_val = hydrostats.metrics.nrmse_mean(y1_pred.tolist(), y1_test.to_list())
print(f"Normalized RMSE (Mean) Model 1 (Validation): {normalized_root_mean_squared_error_model1_val}")

nse_model1_cal = hydrostats.metrics.nse(y1_train_pred.tolist(), y1_train.to_list())
print(f"NSE Model 1 (Calibration): {nse_model1_cal}")
nse_model1_val = hydrostats.metrics.nse(y1_pred.tolist(), y1_test.to_list())
print(f"NSE Model 1 (Validation): {nse_model1_val}")

pearson_correlation_model1_cal = hydrostats.metrics.pearson_r(y1_train_pred.tolist(), y1_train.to_list())
print(f"Pearson Correlation Model 1 (Calibration): {pearson_correlation_model1_cal}")
pearson_correlation_model1_val = hydrostats.metrics.pearson_r(y1_pred.tolist(), y1_test.to_list())
print(f"Pearson Correlation Model 1 (Validation): {pearson_correlation_model1_val}")

kge_model1_cal = hydrostats.metrics.kge_2012(y1_train_pred.tolist(), y1_train.to_list())
print(f"KGE Model 1 (Calibration): {kge_model1_cal}")
kge_model1_val = hydrostats.metrics.kge_2012(y1_pred.tolist(), y1_test.to_list())
print(f"KGE Model 1 (Validation): {kge_model1_val}")

# Note: If you're working on a regression task, replace RandomForestClassifier with RandomForestRegressor
# and accuracy_score with an appropriate regression metric, like mean_squared_error or r2_score.

# Initialize the Kernel SHAP explainer
# Using a background dataset to integrate over
explainer1 = shap.KernelExplainer(model1.predict, shap.sample(X_train, 100))

# Calculate SHAP values for the test set
shap_values1 = explainer1.shap_values(X_test, nsamples=100)  # nsamples specifies the number of times to re-evaluate the model when explaining each prediction

# Summarize the effects of all the features
shap.summary_plot(shap_values1, X_test)

# For a single prediction explanation
# idx = 0  # Index of the observation in the test set
# shap.force_plot(explainer.expected_value, shap_values[idx], X_test.iloc[idx])

# Using a background dataset to integrate over
explainer2 = shap.KernelExplainer(model1.predict, shap.sample(X_train, 100))
shap_values2 = explainer2.shap_values(X_test, nsamples=100)  # nsamples specifies the number of times to re-evaluate the model when explaining each prediction
shap.summary_plot(shap_values2, X_test)