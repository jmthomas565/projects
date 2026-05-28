# %% importing packages
from sklearn.model_selection import StratifiedKFold
from mlflow.tracking import MlflowClient
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier

# Global definition so you don't repeat yourself across functions
PROJECT_ROOT = "/Users/jmthomas565/Desktop/Education/Machine Learning Practise/Customer Propensity Model"


# %% Logistic Regression Model
def train_logistic_reg(x_train_processed, y_train):
    KFS = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    # Set up local backend tracking engine paths
    mlflow.set_tracking_uri(f"file://{PROJECT_ROOT}/mlruns")
    mlflow.set_experiment("Customer_Propensity_Project")

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

        # All tracking commands stay encapsulated inside the open context block
        mlflow.log_params(grid_search_log_reg.best_params_)
        mlflow.log_metric("roc_auc_cv_score", grid_search_log_reg.best_score_)
        
        mlflow.sklearn.log_model(
            sk_model=grid_search_log_reg.best_estimator_, 
            artifact_path="model",
        )
        
        print(f"MLflow Run completed. Run ID: {run.info.run_id}")
        print(f"Best Logistic Regression ROC-AUC Score: {grid_search_log_reg.best_score_:.4f}")


# %% Gradient Boosting Model
def train_gradient_boost(x_train_processed, y_train):
    KFS = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    # Explicitly set tracking paths for this decoupled room
    mlflow.set_tracking_uri(f"file://{PROJECT_ROOT}/mlruns")
    mlflow.set_experiment("Customer_Propensity_Project")

    with mlflow.start_run(run_name="Hist_Gradient_Boost_Tuned") as run:
        model = HistGradientBoostingClassifier(random_state=42)

        xgb_param_grid = {
            'max_iter': [100, 200],
            'learning_rate': [0.01, 0.1, 0.2],
            'max_depth': [3, 5, 7]
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

        # Kept safely inside the 'with' context layout
        mlflow.log_params(grid_search_xgb.best_params_)
        mlflow.log_metric("roc_auc_cv_score", grid_search_xgb.best_score_)

        mlflow.sklearn.log_model(
            sk_model=grid_search_xgb.best_estimator_, 
            artifact_path="model"
        )
        
        # Bypassing the file system metadata error by manually handling creation
        client = MlflowClient()
        model_name = "Customer_Propensity_Project"
        model_uri = f"runs:/{run.info.run_id}/model"
        
        try:
            client.create_registered_model(model_name)
        except Exception:
            pass  # Structural block exists, skipping initialization
            
        client.create_model_version(
            name=model_name,
            source=model_uri,
            run_id=run.info.run_id
        )

        print(f"MLflow Run completed. Run ID: {run.info.run_id}")
        print(f"Best Gradient Boost ROC-AUC Score: {grid_search_xgb.best_score_:.4f}")


# %% Selecting the winning model & Model Registry Gatekeeper
def model_selection(processor):
    client = MlflowClient()
    mlflow.set_tracking_uri(f"file://{PROJECT_ROOT}/mlruns")
    
    model_name = "Customer_Propensity_Project"
    metric_name = "roc_auc_cv_score"

    experiment = client.get_experiment_by_name(model_name)
    if not experiment:
        print("No experiments found to select from.")
        return
        
    experiment_id = experiment.experiment_id

    # 1. Get the current Production model's performance metrics
    production_score = 0.0
    try:
        latest_versions = client.get_latest_versions(name=model_name, stages=["Production"])
        if latest_versions:
            latest_prod = latest_versions[0]
            prod_run = client.get_run(latest_prod.run_id)
            production_score = prod_run.data.metrics.get(metric_name, 0.0)
            print(f"Current Production Champion Score: {production_score:.4f}")
        else:
            print("No baseline Production version tagged. Treating champion score as 0.0.")
    except Exception as e:
        print(f"Proceeding with baseline deployment due to: {e}")

    # 2. Find the absolute best candidate out of all experiment simulations
    all_runs = client.search_runs(
        experiment_ids=[experiment_id],
        max_results=1,
        order_by=[f"metrics.{metric_name} DESC"]
    )

    if not all_runs:
        print("No simulation data found inside MLflow database.")
        return

    best_run = all_runs[0]
    candidate_score = best_run.data.metrics[metric_name]
    candidate_uri = f"runs:/{best_run.info.run_id}/model"

    print("-" * 30)
    print(f"🏆 HIGHEST CANDIDATE RUN FOUND")
    print(f"Run ID: {best_run.info.run_id}")
    print(f"Score: {candidate_score:.4f}")
    print("-" * 30)

    # 3. Automated Deployment Promotion Logic
    if candidate_score > production_score:
        print(f"📈 Challenger ({candidate_score:.4f}) beats Production Champion ({production_score:.4f}). Promoting...")
        
        try:
            client.create_registered_model(model_name)
        except Exception:
            pass
            
        new_version = client.create_model_version(
            name=model_name,
            source=candidate_uri,
            run_id=best_run.info.run_id
        )
        
        # Explicit promotion transition call via low level tracking client
        client.transition_model_version_stage(
            name=model_name,
            version=new_version.version,
            stage="Production",
            archive_existing_versions=True
        )
        print(f"✅ Model Version {new_version.version} successfully locked into Production.")
    else:
        print(f"✋ Challenger ({candidate_score:.4f}) did not beat Champion. Registry promotion denied.")

    # Save the feature processing state for prediction calls
    try:
        joblib.dump(processor, 'models/feature_processor.joblib')
        print("Processor pipeline state saved successfully to models/feature_processor.joblib")
    except FileNotFoundError:
        import os
        os.makedirs('models', exist_ok=True)
        joblib.dump(processor, 'models/feature_processor.joblib')
        print("Created missing directory and saved processor pipeline state successfully.")