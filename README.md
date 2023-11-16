# sparkify-churn-prediction

### Relevant links
- <a href="https://medium.com/@jonathan.mi.collier/churn-prediction-in-music-streaming-c1d41e47ccbb">related blog post</a>
- sparkify dataset: `s3n://udacity-dsnd/sparkify/sparkify_event_data.json`

### Libraries used in the project
- <a href="https://spark.apache.org/docs/latest/api/python/index.html">pyspark</a>
- <a href="https://matplotlib.org">matplotlib</a>
- <a href="https://seaborn.pydata.org">seaborn</a>
- <a href="https://pandas.pydata.org">pandas</a>
- <a href="https://docs.python.org/3/library/unittest.html">unittest</a>
- <a href="https://docs.python.org/3/library/dataclasses.html">dataclasses</a>
- <a href="https://docs.python.org/3/library/os.html">os</a>
- <a href="https://docs.python.org/3/library/sys.html">sys</a>

### Computing Environment and Setup
For my computing environment, I configured an EMR cluster with emr-6.13.0 release and the following applications installed:
  - Ganglia 3.7.2
  - Hadoop 3.3.3,
  - JupyterEnterpriseGateway 2.6.0
  - JupyterHub 1.5.0,
  - Spark 3.4.1
  - Zeppelin 0.10.1.

To install the python packages for my analysis, I performed the following (the code for these steps exist in `SparkifyChurnPrediction.ipynb`):
1. I installed packages `matplotlib`, `pandas` and `seaborn` from Jupyter notebook in my EMR Workspace, using the pyspark API `install_pypi_package`. 
2. I added the `sparkify.zip` file in a folder on an S3 bucket accessible to my cluster. 
3. I set the `S3_BUCKET` and `FOLDER` variables in `SparkifyChurnPrediction.ipynb`.


### Motivation for the project
I was motivated to work on this project based on my interest in Big Data and distributed frameworks. I also find the complexity of the problem interesting in the context of streaming platforms, 
given that streaming platforms have many different types of customers (e.g. free-tier, paid-tier, etc.) who are generating many different data attributes, and who are upgrading, downgrading, and/or churning frequently.

### Files in the repository 
- SparkifyChurnPrediction.ipynb : contains full analysis and churn prediction of the Sparkify dataset.
- src/
  - sparkify.zip : a zip file of the `sparkify` package for importing into the `SparkifyChurnPrediction` Jupyter notebook.
  - sparkify/
    - __init__.py
    - categorical_transformer.py : contains a class for applying string indexing and one-hot encoding to categorical features.
    - numeric_transformer.py : contains a class for assembling numeric features into a vector and applying a scaler.
    - data_cleaning.py : contains methods for cleaning the `Sparkify` event log data.
    - data_cleaning_query.py : contain queries for cleaning the `Sparkify` event log data. Imported into `data_cleaning.py`.
    - data_pipeline.py : contains methods for transforming the `Sparkify` event log data into user features.
    - data_pipeline_query.py : contains queries for transforming the `Sparkify` event log data into user features. Imported into `data_pipeline.py`.
    - eda.py : contains functions for generating plots and tables for performing Exploratory Data Analysis.
- tests/
  - __init__.py
  - context.py :
  - utils.py : contain utility classes for testing.
  - test_data_cleaning.py : a class for testing the underlying transformations in `data_cleaning.py`.
  - test_data_pipeline.py : a class for testing the underlying transformations in `data_pipeline.py`.


### Summary of the analysis results 
The goal of this project was to predict churn in a fictional music streaming service called Sparkify. The dataset consists of user events over a period of time (the observation period) with users who either joined the service prior to or during the observation period. I chose to define churn as when a customer cancels service (i.e., when a customer has a `Cancellation Confirmation` event) and aimed to predict whether the next event would be a churn event.

After transforming the sparkify dataset into user features, I tested several different models and hyperparameters using Cross Validation. Since the data I was working with had class imbalance (only 22% of users churned, whereas 78% of users remained active) I used area under the Precision-Recall curve to evaluate the Cross Validation steps due to it's performance on imbalanced data. 
I tested four different models: Gradient Boosting Tree Classifier, Decision Tree Classifier, Random Forest, and Logistic Regression - Gradient Boosting Tree Classifier performed the best at optimizing the area under the precision recall curve. 
The Gradient Boosting Tree resulted in an F1 Score of .94, a precision of .98, a recall value of .91, and accuracy of .97.

