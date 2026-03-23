#%% import files and reading in data

import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
import numpy as np
import os
from sklearn.preprocessing import OrdinalEncoder, OneHotEncoder, StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import KFold, cross_val_score, StratifiedKFold


os.chdir('/Users/jmthomas565/Desktop/Education/Applied Stats MSc/Data Science/Project/Final Project Files') 


bank_data = pd.read_csv("Banking Dataset - Marketing Targets.csv")

print (bank_data)


#%% train-test split

#Train-test split should happen before we explore the data. Note that we need to split the data into the label (y) and the features to do this. 
#But they are then concatenated back together for data exploration purposes.

y = bank_data["y"]
x = bank_data[bank_data.columns.drop('y')]

#Setting up train-test split.

x_train, x_test, y_train, y_test = train_test_split(
    x, y, test_size=0.30, random_state=42, stratify=y, shuffle=True
    )




# %% Feature Processing


CUSTOM_ORDINAL_CATEGORIES = [
    ['unknown', 'primary', 'secondary', 'tertiary'], # education order
    ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'] # month order
]
    

class FeatureProcessor(BaseEstimator, TransformerMixin):

    def __init__(self, num_cols, cat_cols, ord_cols, custom_ord_cats):
        self.num_cols = num_cols
        self.cat_cols = cat_cols
        self.ord_cols = ord_cols

        self.scalar_ = StandardScaler()
        self.encoder_ = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        self.ordinal_encoder_ = OrdinalEncoder(categories=custom_ord_cats, handle_unknown="use_encoded_value", unknown_value= -1)


    def fit(self, X, y=None):
        self.scalar_.fit(X[self.num_cols])
        self.encoder_.fit(X[self.cat_cols])
        self.ordinal_encoder_.fit(X[self.ord_cols])


        return self 
        
    def transform(self, X):
        X_num_scaled = self.scalar_.transform(X[self.num_cols])
        X_cat_encoded = self.encoder_.transform(X[self.cat_cols])
        X_ord_encoded = self.ordinal_encoder_.transform(X[self.ord_cols])



        X_processed = np.hstack((X_num_scaled, X_cat_encoded, X_ord_encoded))

        return X_processed



#Need to add ordinal encoding to the above methods!




# %% Checking Class and Method Creation




# 1. Define your specific column lists (assuming your data has these)
NUMERIC_FEATURES = ['age', 'balance', 'duration']
CATEGORICAL_FEATURES = ["job", "marital", "default"]
ORDINAL_FEATURES = ["education", "month"]


# 2. Instantiate the FeatureProcessor class
# This calls the __init__ method
processor = FeatureProcessor(
    num_cols=NUMERIC_FEATURES, 
    cat_cols=CATEGORICAL_FEATURES,
    ord_cols=ORDINAL_FEATURES,
    custom_ord_cats=CUSTOM_ORDINAL_CATEGORIES

)


x_train_processed = processor.fit_transform(x_train)



print("Output Type:", type(x_train_processed))
print("Output Shape:", x_train_processed.shape)


# %% Testing Class


num_col_out = NUMERIC_FEATURES
ord_col_out = ORDINAL_FEATURES


ohe_cols_out = list(processor.encoder_.get_feature_names_out(CATEGORICAL_FEATURES))


ALL_FEATURE_NAMES = num_col_out + ord_col_out + ohe_cols_out


x_train_processed_df = pd.DataFrame(
    x_train_processed,
    columns = ALL_FEATURE_NAMES
)


with pd.option_context('display.float_format', '{:.3f}'.format):
    print(x_train_processed_df.head(5))


# %% Printing new testing df


ordinal_encoder = processor.ordinal_encoder_

# Print the categories the encoder learned
print("Ordinal Encoding Order (Learned Categories):")
for i, category_list in enumerate(ordinal_encoder.categories_):
    # Print the name of the column being displayed
    col_name = ORDINAL_FEATURES[i] 
    
    # Print the index (the integer value) and the corresponding category string
    print(f"\n--- {col_name.upper()} ---")
    
    # The categories are stored in the order they will be assigned 0, 1, 2, ...
    for j, category in enumerate(category_list):
        print(f"Value {j}: {category}")


# %% Setting up K-fold 

KFS = StratifiedKFold(n_splits = 5, shuffle = True, random_state = 42)




# %% Storing in MLflow

# ... (Lines 1-90 of your training/processing code are here) ...

# %% Storing in MLflow

import mlflow
import os
import mlflow.sklearn

# This is the "Magic Line" that forces MLflow to save HERE
PROJECT_ROOT = "/Users/jmthomas565/Desktop/Education/Machine Learning Practise/Customer Propensity Model"
mlflow.set_tracking_uri(f"file://{PROJECT_ROOT}/mlruns")

mlflow.set_experiment("Customer_Propensity_Project")

# --- 1. Start the MLflow Run ---
# Replace "Tuned_L1_L2_LogReg" with a descriptive run name
with mlflow.start_run(run_name="Logistic_Regression_Tuned") as run:

    log_reg = LogisticRegression(solver='saga', class_weight='balanced', max_iter=2000)

    param_grid = {
    'penalty': ['l1', 'l2'],
    'C': [0.01, 0.1, 1, 10, 100]
    }


    grid_search_log_reg = GridSearchCV(
        estimator=log_reg,
        param_grid=param_grid,
        cv=KFS,
        scoring="roc_auc",
        verbose=1,
        n_jobs=1
    )

    grid_search_log_reg.fit(x_train_processed, y_train)


# Logging
    mlflow.log_params(grid_search_log_reg.best_params_)
    mlflow.log_metric("roc_auc_cv_score", grid_search_log_reg.best_score_)
    
    # Log the best estimator found by the search. 
    # This is the production-ready model trained on the full dataset.
    mlflow.sklearn.log_model(
        sk_model=grid_search_log_reg.best_estimator_, 
        artifact_path="model", # The folder path in MLflow UI
    )
    
    # Optional: Log the entire GridSearchCV object for full debug info
    # mlflow.sklearn.log_model(grid_search_log_reg, "full_search_object") 

    print(f"MLflow Run completed. Run ID: {run.info.run_id}")
    print(f"Best ROC-AUC Score Logged: {grid_search_log_reg.best_score_:.4f}")




# %% XG Boost Model

    
from xgboost import XGBClassifier
import mlflow.xgboost


y_train = y_train.astype(str).str.strip().str.lower().map({'no': 0, 'yes': 1})
y_test = y_test.astype(str).str.strip().str.lower().map({'no': 0, 'yes': 1})




with mlflow.start_run(run_name="XGBoost_Tuned") as run:

    model = XGBClassifier(
    objective = 'binary:logistic',
    random_state = 42,
    use_label_encoder=False,
    eval_metric='logloss'
    )


    xgb_param_grid = {
    'n_estimators': [100, 200],
    'learning_rate': [0.01, 0.1, 0.2],
    'max_depth': [3, 5, 7],
    'subsample': [0.8, 1.0]
    }


    grid_search_xgb = GridSearchCV(
    estimator=model,
    param_grid=xgb_param_grid,
    cv=KFS,
    scoring="roc_auc",
    verbose=1,
    n_jobs=-1
    )

    grid_search_xgb.fit(x_train_processed, y_train)



    # Logging
    mlflow.log_params(grid_search_xgb.best_params_)
    mlflow.log_metric("roc_auc_cv_score", grid_search_xgb.best_score_)


    # Log the best estimator found by the search. 
    # This is the production-ready model trained on the full dataset.
    mlflow.sklearn.log_model(
    sk_model=grid_search_xgb.best_estimator_, 
    artifact_path="model", # The folder path in MLflow UI
    registered_model_name="Customer_Propensity_Project" # Name for the Model Registry
    )


    mlflow.xgboost.log_model(
    xgb_model=grid_search_xgb.best_estimator_, 
    artifact_path="model" # <--- This MUST be "model" to match your Gatekeeper
    )


    print(f"MLflow Run completed. Run ID: {run.info.run_id}")
    print(f"Best ROC-AUC Score Logged: {grid_search_xgb.best_score_:.4f}")


# %% Checking MLFlow

#Running this in the terminal 

#mlflow ui --backend-store-uri "file:///Users/jmthomas565/Desktop/Education/Machine Learning Practise/Customer Propensity Model/mlruns"



print("Unique values in y_train:", y_train.unique())



# %% Selecting the winning model 

import mlflow
from mlflow.tracking import MlflowClient

# 1. Initialize the client
client = MlflowClient()

# 2. Get the experiment ID (using the name you set earlier)
experiment = client.get_experiment_by_name("Customer_Propensity_Project")
experiment_id = experiment.experiment_id

# 3. Search for all runs in this experiment, sorted by your metric
runs = client.search_runs(
    experiment_ids=[experiment_id],
    filter_string="",
    run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY,
    max_results=1,
    order_by=["metrics.roc_auc_cv_score DESC"] # DESC means highest score first
)


# 4. Extract the winner
if runs:
    
    best_run = runs[0]
    best_score = best_run.data.metrics["roc_auc_cv_score"]
    best_run_name = best_run.data.tags.get("mlflow.runName", "Unnamed Run")
    
    print("-" * 30)
    print(f"🏆 WINNER FOUND IN MLFLOW!")
    print(f"Model: {best_run_name}")
    print(f"Best Score: {best_score:.4f}")
    print(f"Run ID: {best_run.info.run_id}")
    print("-" * 30)
else:
    print("No runs found in MLflow.")

# %% 🏆 AUTOMATED WINNER SELECTION & PROMOTION

from mlflow.tracking import MlflowClient
import mlflow

client = MlflowClient()
model_name = "Customer_Propensity_Project"
metric_name = "roc_auc_cv_score"

# 1. Get the current Production model's performance
production_score = 0.0
try:
    winner = client.search_runs(...)[0]

    prod_run = client.get_run(latest_prod.run_id)
    production_score = prod_run.data.metrics.get(metric_name, 0.0)
    print(f"Current Production Score: {production_score:.4f}")
except Exception:
    print("No existing Production model found. Proceeding with first deployment.")

# 2. Find the best candidate from your recent experiment
experiment = client.get_experiment_by_name("Customer_Propensity_Project")
best_run = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    max_results=1,
    order_by=[f"metrics.{metric_name} DESC"]
)[0]

candidate_score = best_run.data.metrics[metric_name]
candidate_uri = f"runs:/{best_run.info.run_id}/model"

# 3. The "Gatekeeper" Logic
if candidate_score > production_score:
    print(f"🏆 Challenger ({candidate_score:.4f}) beats Champion ({production_score:.4f}). Promoting...")
    new_version = mlflow.register_model(model_uri=candidate_uri, name=model_name)
    
    client.transition_model_version_stage(
        name=model_name,
        version=new_version.version,
        stage="Production",
        archive_existing_versions=True
    )
else:
    print(f"✋ Challenger ({candidate_score:.4f}) did not beat Champion. No promotion.")
# %% Test Cell
    
# 1. Check if the model folder actually exists in your run
artifacts = client.list_artifacts(best_run.info.run_id)
print("Files in this run:")
for a in artifacts:
    print(f" - {a.path}")

# 2. Verify the URI you are sending to the registry
print(f"\nAttempting to register from: runs:/{best_run.info.run_id}/model")


# %% Predicting new customers


import mlflow
import pandas as pd

# 1. Connect to your MLflow server
mlflow.set_tracking_uri("http://localhost:5000")

# 2. Define the "Address" of your Production model
# We use the 'Production' stage to ensure we always get the winner
model_name = "Customer_Propensity_Project"
model_uri = f"models:/{model_name}/Production"

# 3. Load the model as a 'PyFunc' (Python Function)
# This is the "Senior" way because it works regardless of whether the 
# model was XGBoost or Logistic Regression!
model = mlflow.pyfunc.load_model(model_uri)

# 4. Prepare your new data (Simulating 2 new customers)
# IMPORTANT: These columns must match your training data exactly
new_customers = pd.DataFrame({
    'age': [34, 58],
    'job': ["unemployed", "housemaid"],
    'marital': ["single", "single"],
    'education': ["secondary", "unknown"],
    'default': ["no", "no"],
    'balance': [12, 360],
    'housing': ["yes", "yes"],
    'loan': ["yes", "no"],
    'contact': ["unknown", "telephone"],
    'day': [12, 2],
    'month': ["aug", "feb"],
    'duration': [104, 264],
    'campaign': [3, 2],
    'pdays': [-1, 4],
    'previous': [1, 3],
    'poutcome': ["unknown", "failure"]

    # ... add all other columns your model expects ...
})


# %% Transforming Prediction Data

import mlflow.xgboost

# 1. Load the model SPECIFICALLY as an XGBoost object
# This ensures it has the .predict_proba() method natively
model_uri = f"models:/Customer_Propensity_Project/Production"
xgb_model = mlflow.xgboost.load_model(model_uri)

# 2. Generate the numbers using your processor
numeric_matrix = processor.transform(new_customers)

# 3. Get the Probabilities (Propensity Scores)
# Now this will work PERFECTLY because xgb_model is a true XGBoost object
probs = xgb_model.predict_proba(numeric_matrix)[:, 1]

# 4. Get the hard Y/N Predictions
preds = xgb_model.predict(numeric_matrix)

# 5. Assign to your table
new_customers['propensity_score'] = probs
new_customers['final_prediction'] = preds

# 6. Show the results
print(new_customers[['age', 'propensity_score', 'final_prediction']])
