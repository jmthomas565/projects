from src.data.make_dataset import prepare_data
from src.features.build_features import run_feature_engineering

def main()
    
    x_train, x_test, y_train, y_test = prepare_data("data/raw/Banking Dataset - Marketing Targets.csv")

    x_train_final, x_test_final, processor = run_feature_engineering(x_train, x_test)

    return x_train_final, x_test_final


x_train_final.head()

x_train_final.shape


x_test_final.head()

x_test_final.shape


if __name__ == "__main__"
