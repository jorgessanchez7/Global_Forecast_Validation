import shap
import hydrostats
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score  # Use appropriate metric for your task, e.g., mean_squared_error for regression
from sklearn.ensemble import RandomForestRegressor  # Use RandomForestRegressor for regression tasks
from sklearn.ensemble import RandomForestClassifier  # Use RandomForestRegressor for regression tasks
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor, plot_tree

# Load the dataset
file_path = '/Users/grad/Library/CloudStorage/Box-Box/PhD/2022_Winter/Dissertation_v13/World_Total_Correlations.csv'  # Update this to your dataset's actual path
data = pd.read_csv(file_path)

# Assuming the last column is the target variable and all others are predictors
X = data.copy()
X.set_index('id', inplace=True)
# Selecting all columns except the last one as features
X = pd.DataFrame(X.drop(columns=['Data Folder', 'Data Source', 'code', 'Name', 'COMID', 'r_day_bc', 'r_month_bc',
                                 'Bias_Corrected', 'Variability_Corrected', 'Correlation_Corrected', 'KGE_Corrected',
                                 'KGE_Corrected_Categories', 'Bias Corrected (%)', 'Variability Corrected (%)',
                                 'KGE_Original_Categories', 'KGE_diff']))

y1 = data['KGE_Corrected_Categories']  # Selecting one of the variables to be predicted

# Splitting the dataset into training and test sets
X_train, X_test, y1_train, y1_test = train_test_split(X, y1, test_size=0.2, random_state=42)

# Initialize and train the Random Forest model
model1 = RandomForestClassifier(random_state=42)
model1.fit(X_train, y1_train)

# Select one of the trees from the random forest model
# Here, we choose the first tree as an example
estimator = model1.estimators_[0]

# Visualize the selected tree
plt.figure(figsize=(20,10))
plot_tree(estimator,
          feature_names=X.columns,
          class_names=['unacceptable', 'very poor', 'poor', 'intermediate', 'good'],  # Update class names as appropriate
          filled=True, impurity=True, rounded=True)
plt.show()

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
accuracy_cal = accuracy_score(y1_train_pred.tolist(), y1_train.to_list())
print(f"Accuracy Model 1 (Calibration): {accuracy_cal}")
accuracy_val = accuracy_score(y1_pred.tolist(), y1_test.to_list())
print(f"Accuracy Model 1 (Validation): {accuracy_val}")

# Note: If you're working on a regression task, replace RandomForestClassifier with RandomForestRegressor
# and accuracy_score with an appropriate regression metric, like mean_squared_error or r2_score.

# Initialize the Kernel SHAP explainer
# Assuming X_train is your training data

# SHAP Kernel Explainer initialization corrected
# Note: Using a subset of X_train as the background distribution
background = shap.sample(X_train, 100)  # Adjust the sample size as needed
explainer1 = shap.KernelExplainer(model=model1.predict_proba, data=background)

# Compute SHAP values for a subset of your test data
X_test_sample = shap.sample(X_test, 50)  # Adjust as needed
shap_values1 = explainer1.shap_values(X_test_sample)

# Summarize the effects of all the features
shap.summary_plot(shap_values1, X_test_sample)


#TreeExplainer
explainer2 = shap.TreeExplainer(model=model1.predict_proba, data=background)
shap_values2 = explainer2.shap_values(X_test_sample)
shap.summary_plot(shap_values2, X_test_sample)