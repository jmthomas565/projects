import joblib
from src.data.make_dataset import prepare_data
from src.features.build_features import run_feature_engineering
from src.models.train_model import train_logistic_reg
from src.models.train_model import train_gradient_boost
from src.models.train_model import model_selection



def main():
    # loads the data
    print("--- Loading Data ---")
    x_train, x_test, y_train, y_test = prepare_data("data/Bank Marketing - UC Irvine ML Repo.csv")

    # builds features
    print("--- Engineering Features ---")
    x_train_final, x_test_final, processor = run_feature_engineering(x_train, x_test)

    # saving the processor
    print("Processor saved to models/feature_processor.joblib")

    # training the models
    train_logistic_reg(x_train_final, y_train)
    train_gradient_boost(x_train_final, y_train)

    # run automated Champion-Challenger gatekeeper for production deployment
    model_selection(processor)
    
    print("Pipeline Complete! Run 'mlflow ui' to see your results.")

if __name__ == "__main__":
    main()

