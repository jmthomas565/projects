import mlflow
import pandas as pd
import joblib # Needed to load the processor

def make_predictions():
    # 1. Connect to your LOCAL MLflow store (Match your train script path!)
    PROJECT_ROOT = "/Users/jmthomas565/Desktop/Education/Machine Learning Practise/Customer Propensity Model"
    mlflow.set_tracking_uri(f"file://{PROJECT_ROOT}/mlruns")

    # 2. Load the model from the Registry
    model_name = "Customer_Propensity_Project"
    model_uri = f"models:/{model_name}/latest" # Use 'latest' or 'Production'
    model = mlflow.pyfunc.load_model(model_uri)

    # 3. Load the Processor (The instruction manual we saved)
    # This must be the same path you used in joblib.dump()
    processor = joblib.load('models/feature_processor.joblib')

    # 4. New Data
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
    })

    # 5. TRANSFORM the data before predicting
    # This is the "Magic Step" where the processor turns words into numbers
    new_customers_processed = processor.transform(new_customers)

    # 6. Get Predictions
    predictions = model.predict(new_customers_processed)
    
    # Note: pyfunc models often return probabilities directly in predict() 
    # depending on the flavor, but let's keep it simple for MVP:
    print("\n--- Propensity Results ---")
    for i, pred in enumerate(predictions):
        result = "WILL BUY" if pred == 1 else "UNLIKELY TO BUY"
        print(f"Customer {i+1}: {result}")

if __name__ == "__main__":
    make_predictions()