import joblib
from src.data.make_dataset import prepare_data
from src.features.build_features import run_feature_engineering
from src.models.train_model import run_training_pipeline # Import your trainer!

def main():
    # 1. Load Data
    print("--- Loading Data ---")
    x_train, x_test, y_train, y_test = prepare_data("data/raw/Banking Dataset - Marketing Targets.csv")

    # 2. Build Features
    print("--- Engineering Features ---")
    x_train_final, x_test_final, processor = run_feature_engineering(x_train, x_test)

    # 3. Clean Targets (The 'y' mapping bit)
    # We do this here so the training script receives pure numbers
    y_train = y_train.astype(str).str.strip().str.lower().map({'no': 0, 'yes': 1})
    y_test = y_test.astype(str).str.strip().str.lower().map({'no': 0, 'yes': 1})

    # 4. Save the Processor (The "Translator")
    # This is vital for the Prediction script to work later!
    joblib.dump(processor, 'models/feature_processor.joblib')
    print("Processor saved to models/feature_processor.joblib")

    # 5. Run Training
    # This triggers your MLflow logging and model registration
    print("--- Starting Training Pipeline ---")
    run_training_pipeline(x_train_final, x_test_final, y_train, y_test)
    
    print("Pipeline Complete! Run 'mlflow ui' to see your results.")

if __name__ == "__main__":
    main()

