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


processor

# 5. Get the Propensity Scores!



predictions = model.predict(new_customers)
probabilities = model.predict_proba(new_customers)[:, 1] if hasattr(model, 'predict_proba') else "N/A"

print("--- Propensity Results ---")
for i, pred in enumerate(predictions):
    result = "WILL BUY" if pred == 1 else "UNLIKELY TO BUY"
    print(f"Customer {i+1}: {result} (Probability: {probabilities[i]:.2f})")
# %%
