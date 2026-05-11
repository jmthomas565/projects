import pandas as pd
import numpy as np
from sklearn.preprocessing import OrdinalEncoder, OneHotEncoder, StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from data.make_dataset import prepare_data # Keeping this here is fine for MVP

class FeatureProcessor(BaseEstimator, TransformerMixin):
    def __init__(self, num_cols, cat_cols, ord_cols, custom_ord_cats):
        self.num_cols = num_cols
        self.cat_cols = cat_cols
        self.ord_cols = ord_cols
        self.scalar_ = StandardScaler()
        self.encoder_ = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        self.ordinal_encoder_ = OrdinalEncoder(categories=custom_ord_cats, handle_unknown="use_encoded_value", unknown_value=-1)

    def fit(self, X, y=None):
        self.scalar_.fit(X[self.num_cols])
        self.encoder_.fit(X[self.cat_cols])
        self.ordinal_encoder_.fit(X[self.ord_cols])
        return self 
        
    def transform(self, X):
        X_num_scaled = self.scalar_.transform(X[self.num_cols])
        X_cat_encoded = self.encoder_.transform(X[self.cat_cols])
        X_ord_encoded = self.ordinal_encoder_.transform(X[self.ord_cols]) # Essential fix

        # This joins them into one block for the model
        return np.hstack((X_num_scaled, X_cat_encoded, X_ord_encoded))

def run_feature_engineering(x_train, x_test):
    NUMERIC_FEATURES = ['age', 'balance', 'duration']
    CATEGORICAL_FEATURES = ["job", "marital", "default"]
    ORDINAL_FEATURES = ["education", "month"]
    CUSTOM_ORDINAL_CATEGORIES = [
        ['unknown', 'primary', 'secondary', 'tertiary'],
        ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    ]
    
    processor = FeatureProcessor(NUMERIC_FEATURES, CATEGORICAL_FEATURES, ORDINAL_FEATURES, CUSTOM_ORDINAL_CATEGORIES)

    # 1. Fit and transform
    x_train_raw = processor.fit_transform(x_train)
    x_test_raw = processor.transform(x_test)

    # 2. Create the column list (So your DataFrame isn't just numbers)
    ohe_cols = list(processor.encoder_.get_feature_names_out(CATEGORICAL_FEATURES))
    all_feature_names = NUMERIC_FEATURES + ohe_cols + ORDINAL_FEATURES

    # 3. Create DataFrames
    x_train_df = pd.DataFrame(x_train_raw, columns=all_feature_names, index=x_train.index)
    x_test_df = pd.DataFrame(x_test_raw, columns=all_feature_names, index=x_test.index)

    return x_train_df, x_test_df, processor

if __name__ == "__main__":
    # Just verify this path exists on your machine!
    path = "/Users/jmthomas565/Desktop/Coding/Coding Practise - 2024/Git Repo (Projects)/Customer Propensity Modelling/Data/Bank Marketing - UC Irvine ML Repo.csv"
    
    x_train, x_test, y_train, y_test = prepare_data(path)
    x_train_df, x_test_df, processor = run_feature_engineering(x_train, x_test)

    print(x_train_df.head())
    print(f"Shape: {x_train_df.shape}")