import joblib
from src.data.make_dataset import prepare_data
from src.features.build_features import run_feature_engineering
from src.models.train_model import train_logistic_reg
from src.models.train_model import train_gradient_boost
from src.models.train_model import model_selection



def main():
    # 1. Load Data
    print("--- Loading Data ---")
    x_train, x_test, y_train, y_test = prepare_data("data/Bank Marketing - UC Irvine ML Repo.csv")

    # 2. Build Features
    print("--- Engineering Features ---")
    x_train_final, x_test_final, processor = run_feature_engineering(x_train, x_test)

    # 4. Save the Processor (The "Translator")
    # This is vital for the Prediction script to work later!
    print("Processor saved to models/feature_processor.joblib")
    joblib.dump(processor, 'models/feature_processor.joblib')


    train_logistic_reg(x_train_final, y_train)
    train_gradient_boost(x_train_final, y_train)

    model_selection(processor)
    
    print("Pipeline Complete! Run 'mlflow ui' to see your results.")

if __name__ == "__main__":
    main()

