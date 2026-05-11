# %% importing packages

from sklearn.model_selection import StratifiedKFold
from mlflow.tracking import MlflowClient
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
import joblib


# %% Setting up K-fold 

KFS = StratifiedKFold(n_splits = 5, shuffle = True, random_state = 42)



# %% Storing in MLflow

# ... (Lines 1-90 of your training/processing code are here) ...

# %% Storing in MLflow


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

# Use the native XGBoost logger, but keep your registration name here!
    mlflow.xgboost.log_model(
        xgb_model=grid_search_xgb.best_estimator_, 
        artifact_path="model", 
        registered_model_name="Customer_Propensity_Project" # Moved this here!
    )


    print(f"MLflow Run completed. Run ID: {run.info.run_id}")
    print(f"Best ROC-AUC Score Logged: {grid_search_xgb.best_score_:.4f}")


# %% Checking MLFlow

#Running this in the terminal 

#mlflow ui --backend-store-uri "file:///Users/jmthomas565/Desktop/Education/Machine Learning Practise/Customer Propensity Model/mlruns"



print("Unique values in y_train:", y_train.unique())



# %% Selecting the winning model 


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


# Save the processor so the prediction script can use it
joblib.dump(processor, 'models/feature_processor.joblib')
print("Processor saved to models/feature_processor.joblib")

