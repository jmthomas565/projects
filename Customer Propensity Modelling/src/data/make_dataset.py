#%% import files and reading in data

import pandas as pd
from sklearn.model_selection import train_test_split
import numpy as np



def prepare_data (file_path):

    bank_data = pd.read_csv(file_path)

    #file_path = "Banking Dataset - Marketing Targets.csv"

    #Train-test split should happen before we explore the data. Note that we need to split the data into the label (y) and the features to do this. 
    #But they are then concatenated back together for data exploration purposes.

    y = bank_data["y"]
    x = bank_data[bank_data.columns.drop('y')]

    #Setting up train-test split.

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.30, random_state=42, stratify=y, shuffle=True
        )
    
    y_train = y_train.astype(str).str.strip().str.lower().map({'no': 0, 'yes': 1})
    y_test = y_test.astype(str).str.strip().str.lower().map({'no': 0, 'yes': 1})
    
    return x_train, x_test, y_train, y_test 


if __name__ == "__main__":

    data_path = "/Users/jmthomas565/Desktop/Coding/Coding Practise - 2024/Git Repo (Projects)/Customer Propensity Modelling/Data/Bank Marketing - UC Irvine ML Repo.csv"

    x_train, x_test, y_train, y_test = prepare_data(data_path)

    print (f"x_train shape: {x_train.shape}, x_test shape: {x_test.shape}") 
           
    print (x_train.head())

    print (y_train.value_counts())

    print (y_test.value_counts())

    