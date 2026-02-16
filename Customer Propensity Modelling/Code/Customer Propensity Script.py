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


# %% Training the Logistic Regression
        

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




# %% Storing in MLflow

# ... (Lines 1-90 of your training/processing code are here) ...

# %% Storing in MLflow

import mlflow
import os

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
        artifact_path="log_reg_model", # The folder path in MLflow UI
        registered_model_name="Customer_Propensity_Project" # Name for the Model Registry
    )
    
    # Optional: Log the entire GridSearchCV object for full debug info
    # mlflow.sklearn.log_model(grid_search_log_reg, "full_search_object") 

    print(f"MLflow Run completed. Run ID: {run.info.run_id}")
    print(f"Best ROC-AUC Score Logged: {grid_search_log_reg.best_score_:.4f}")




# %% XG Boost Model

    
from xgboost import XGBClassifier


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
        artifact_path="xgb_model", # The folder path in MLflow UI
        registered_model_name="Customer_Propensity_Project" # Name for the Model Registry
    )
    

    print(f"MLflow Run completed. Run ID: {run.info.run_id}")
    print(f"Best ROC-AUC Score Logged: {grid_search_xgb.best_score_:.4f}")


# %% Checking MLFlow

#Running this in the terminal 

#mlflow ui --backend-store-uri "file:///Users/jmthomas565/Desktop/Education/Machine Learning Practise/Customer Propensity Model/mlruns"



print("Unique values in y_train:", y_train.unique())

# %%
