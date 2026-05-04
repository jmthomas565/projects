#%% import files and reading in data

import pandas as pd
import numpy as np
from sklearn.preprocessing import OrdinalEncoder, OneHotEncoder, StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from make_dataset import prepare_data




# %% Feature Processing    

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



CUSTOM_ORDINAL_CATEGORIES = [
    ['unknown', 'primary', 'secondary', 'tertiary'], # education order
    ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'] # month order
]
def run_feature_engineering(x_train, x_test):
    # 1. Configuration (The Master Lists)
    NUMERIC_FEATURES = ['age', 'balance', 'duration']
    CATEGORICAL_FEATURES = ["job", "marital", "default"]
    ORDINAL_FEATURES = ["education", "month"]
    CUSTOM_ORDINAL_CATEGORIES = [
        ['unknown', 'primary', 'secondary', 'tertiary'],
        ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    ]
    
    # 2. Instantiate the "Machine"
    processor = FeatureProcessor(
        num_cols=NUMERIC_FEATURES, 
        cat_cols=CATEGORICAL_FEATURES,
        ord_cols=ORDINAL_FEATURES,
        custom_ord_cats=CUSTOM_ORDINAL_CATEGORIES
    )

    # 3. Transform the data (fit_transform on train, transform on test)
    # Using .index ensures we don't lose our row IDs
    x_train_raw = processor.fit_transform(x_train)
    x_test_raw = processor.transform(x_test)

    # 4. Reconstruct Column Names
    ohe_cols = list(processor.encoder_.get_feature_names_out(CATEGORICAL_FEATURES))
    all_feature_names = NUMERIC_FEATURES + ohe_cols + ORDINAL_FEATURES

    # 5. Convert back to beautiful DataFrames
    x_train_df = pd.DataFrame(x_train_raw, columns=all_feature_names, index=x_train.index)
    x_test_df = pd.DataFrame(x_test_raw, columns=all_feature_names, index=x_test.index)

    return x_train_df, x_test_df, processor



# %% testing output of script


if __name__ == "__main__":


    x_train, x_test, y_train, y_test = prepare_data("/Users/jmthomas565/Desktop/Coding/Coding Practise - 2024/Git Repo (Projects)/Customer Propensity Modelling/Data/Bank Marketing - UC Irvine ML Repo.csv")




    x_train_df, x_test_df, processor  = run_feature_engineering (x_train, x_test)

    print (x_test_df, x_test_df)


    print (f"Shape of x_train_df:{x_train_df.shape}. Shape of x_test_df:{x_test_df.shape}")
    # %%
