# msc-project-source-code-files-24-25-jmthomas565
msc-project-source-code-files-24-25-jmthomas565 created by GitHub Classroom

All the required files are kept in the Code folder:

https://github.com/Birkbeck/msc-project-source-code-files-24-25-jmthomas565/tree/main/Code

In the Code folder are these folders:

### Project Code Pipeline
This is the main folder of the project. It contains the whole pipeline for the project. The pipeline has been split into separate notebooks, for easier unit testing, and functionality. These individual notebooks are described in more depth below. 

### Data Exploration
Contains a file with data exploration, simple plots and descriptive stats

### Figure Creation
Contains a file used to create many of the figures

### Outtakes 
Contains outtake and early project Python package testing files 





## More information on Project Code Pipeline notebooks

Below is a summary of the contents of the Project Code Pipeline folder.

### 00_inititial_transformations.ipynb
Loads in the CSV Kaggle and macroeconomic variable data. 
Also performs preliminary data cleaning techniques, and fixes dates to all unify. This is run in every other notebook in the file. This technique avoids duplicating code. 

### 1. Baseline regression.ipynb
Performs a baseline regression, using marketing channels as the regressors, and sales as the response. 

### 2. Testing for Stationarity Dependent Variable.ipynb
Tests for stationary of the dependent variable, using plots and ADF test.

### 3. Testing for Stationarity independent Variable.ipynb
Tests for stationary of the independent variable, using plots and ADF test.

### 4. Stationary Variable Regression.ipynb
Converts the Kaggle variables to all be stationary, and performs a regression on these. 

### 5. Adstock Transformation.ipynb
Performs a regression, using cross-validation, whilst optimising the decay rate of adstock. 

### 6. Saturation Transformation.ipynb
Performs a regression, using cross-validation, whilst optimising the s and K hyperparameters of Saturation. 

### 7. Macroeconomic Variable Regression.ipynb
Performs a regression with the Macroeconomic variables and their lags. 

### 8. Optimising Ridge (alpha).ipynb
Performs a regression whilst optimising Ridge (alpha).

### 9. PACF and AIC.ipynb
Uses PACF plot and AIC to identify the optimal lags for macroeconomic variables and the response.

### 10. Final Regression with AdStock and Lags.ipynb
Performs the final cross-validation regression, with all the predetermined hyperparameter settings and lags in-place. 

### 11. Final Model Evaluation.ipynb
Tests the final best-performing model in Evaluation. 

### 12. Plotting Regression Residuals.ipynb
Plots the regression residuals.




