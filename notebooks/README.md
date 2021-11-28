# NYSE Data Pipeline

##### Author: Dung Ngo

## Data Engineering Capstone Project

#### Project Summary

This project is a part of my project to build an interactive dashboard to track real-time popular NYSE stock data. This project will build an ETL pipeline in Airflow to load data from API to S3 and finally to Redshift.

### General Steps

The project follows the follow steps:

- [Step 1: Scope the Project and Gather Data](#step-1)
- [Step 2: Explore and Assess the Data](#step-2)
- [Step 3: Define the Data Model](#step-3)
- [Step 4: Run ETL to Model the Data](#step-4)
- [Step 5: Complete Project Write Up](#step-5)
- [License](#license)

### File structure

### Step 1: Scope the Project and Gather Data

In this project, I will build an ETL pipeline that can fetch data from external API, called Yahoo Finance API, then load to S3 and finally to Redshift for any future analysis. Data (in json) is fetched at endpoint `/getchart` and then is parsed into 2 tables, including indicators and metadata. `indicators` table will contain timeseries data of opening, high, low, and closing (OHLC) price, while `metadata` table will demonstrate a specific company profile (e.g. Amazon or Apple).<br><br>

By looking at `notebooks/Staging (API & S3).ipynb`, the top entries of two tables are showed below.
<img src="../images/indicators.png"></img>
Figure 1: indicators table
<br><br>
<img src="../images/metadata.png"></img>
Figure 2: metadata table

### Step 2: Explore and Assess the Data

Data is cleaned and there is no need to process any further so I jump into data exploration. This analysis uses Kaggle dataset, called DJIA, which includes 30 popular historical timeseries stock data, such as AAPL, AMZN, GOGL, JPM.
<br><br>
By looking at `notebooks/Exploration (Pandas & MLP).ipynb`, we can see I compute 5 statistics (max, min, avg, median, IQR) for OHLC columns through boxplot. Furthermore, I graphed an OHLC chart to see how price changes in a specific timeframe
<img src="../images/opening_ts.png"></img>
Figure 3: Opening price in Jan 2017 (1-hour interval)
<br><br>
<img src="../images/apple_ohlc.png"></img>
Figure 4: AAPL's OHLC chart

### Step 3: Define the data model

The data model follows STAR schema, where indicators table is a fact table while metadata is a dimensional table. As I move along exploring new API endpoint, more data will be added and be normalized accordingly. By looking at file `Data warehouse (S3 & Redshift).ipynb`, we can see that processed data stored in S3 are copied to Redshift. Below are schemas for two tables.
<img src="../images/schemas.png"></img>
Figure 5: Table schemas

### Step 4: Run pipelines to model the data

The data pipeline can be seen in file dag.py. The general pipeline can be seen as below.
<img src="../images/graph_view_dag.png"></img>
Figure 6: Graph view of ETL pipeline
