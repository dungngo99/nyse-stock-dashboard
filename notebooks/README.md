# NYSE Data Pipeline

##### Author: Dung Ngo

## Data Engineering Capstone Project

#### Project Summary

This project will build an ETL pipeline in Airflow to load data from API to S3 and finally to Redshift. It is a part of my project to build an interactive dashboard to track real-time popular NYSE stock data.

### General Steps

The project follows the follow steps:

- [Step 1: Scope the Project and Gather Data](#step-1)
- [Step 2: Explore and Assess the Data](#step-2)
- [Step 3: Define the Data Model](#step-3)
- [Step 4: Run ETL to Model the Data](#step-4)
- [Step 5: Complete Project Write Up](#step-5)
- [License](#license)

### File structure

This directory's file structure is showed as following<br>

1. Exploration (Pandas & MLP).ipynb: This notebook shows data exploration and preliminary analysis for stock data
2. Staging (API & S3).ipynb: This notebook shows how data is moved from API fetching to S3
3. Data warehouse (S3 & Redshift).ipynb: This notebook shows how data is migrated from S3 to Redshift
4. ETL Pipeline (Airflow).ipynb: This notebook shows how everything is put together in an ETL pipeline
   <br><br>

### Step 1: Scope the Project and Gather Data

#### Scope

In this project, I build an ETL pipeline that can fetch data from external API, called Yahoo Finance API then load to S3 and finally migrate to Redshift. The end goal is that the interactive stock dashboard can easily fetch internal data from Redshift without worrying about limited API calls and data cleaning like transforming raw JSON to tabular format. <br><br>
After data is fetched in JSON format, I use S3 as a staging area to store data in csv. Then, Redshift is used as a data warehouse to store tabular tables. Finally, I use Airflow to put all steps in a pipeline to streamline the data flow and be able to schedule hourly run for this pipeline. <br><br>

#### Describe and gather data

Data (in json) is fetched at endpoint `/getchart` and then is parsed into 2 tabular tables, including indicators and metadata. `indicators` table will contain timeseries data of opening, high, low, and closing (OHLC) price for a particular stock, while `metadata` table will demonstrate a specific company profile (e.g. Amazon or Apple).<br><br>

In `notebooks/Staging (API & S3).ipynb`, a few top entries of two tables are showed in these figures. Other details like data types and number of entries in each table are also showed in the notebook.<br><br>
<img src="../images/indicators.png"></img>
Figure 1: indicators table
<br><br>
<img src="../images/metadata.png"></img>
Figure 2: metadata table
<br><br>

### Step 2: Explore and Assess the Data

#### Clcean steps

Data is fetched in JSON format so I need to extract only necessary information and put it to tabular format. The processing steps can be seen in `Staging (API & S3).ipynb`.

#### Explore the data

Then, I jump into analysis part. The analysis uses Kaggle dataset instead, called DJIA, which includes 30 popular historical timeseries stock data, such as AAPL, AMZN, GOGL, JPM.<br><br>
In `notebooks/Exploration (Pandas & MLP).ipynb`, I compute 5 statistics (max, min, avg, median, IQR) for OHLC columns and display them in boxplots. Furthermore, I graphed an OHLC chart to see how price changes in a specific timeframe.<br><br>
<img src="../images/opening_ts.png"></img>
Figure 3: Opening price in Jan 2017 (1-hour interval)
<br><br>
<img src="../images/apple_ohlc.png"></img>
Figure 4: AAPL's OHLC chart
<br><br>

### Step 3: Define the data model

#### Conceptual data model

The data model follows STAR schema, where indicators table is a fact table while metadata is a dimensional table. As I move along exploring new API endpoint, more data will be added and be normalized accordingly. The star schema can allow fast queries without being afraid of deeply nested JOIN commands. Also, star schema is much easier to comprehend at high level when presenting to clients and executive members<br><br>

In `Data warehouse (S3 & Redshift).ipynb`, it shows the primary-foreign-key relationship between metadata and indicators tables via a column called symbol. For example, a symbol can be AAPL or AMZN. The following figures show the table name, all columns, and their constraints for two tables using the `CREATE TABLE`command.<br><br>
<img src="../images/schemas.png"></img><br>
Figure 5: schamas for two tabl indicators and metadata

#### Mapping out data pipelines

Once I put everything in a data pipeline in Airflow, I can see how data is flowed via a graph view of `nyse-stock` DAG.<br><br>
<img src="../images/graph_view_dag.png"></img>
Figure 6: Graph view of ETL pipeline
<br><br>

### Step 4: Run pipelines to model the data

#### Create the data model

The details about the pipeline can be seen in `etl` folder which includes `dags` and `plugins` folder.

#### Data quality checks

In `check_quality_operator.py` under folder `etl/plugins/operators`, there are two non-empty-table quality checks whether two tables I migrated to Redshift contain any values. There are also two non-duplicate-primary-key checks whether two tables have any duplicate keys. If any of them has, I have to redesign the primary key for that table. Having data quality checks will ensure that the entire pipeline run correctly by looking at the final output. 

#### Data dictionary

The following two tables show the data dictionaries for table `indicators` and table `metadata`
Table | Description | Owner | Last accessed | Last updated | Number of columns | Primary key
--- | --- | --- | --- | --- | --- | --- |
indicators | Timeseries OHLC stock data | Redshift - dngo | Nov 28th, 2021 | Nov 28th, 2021 | 8 | (timestamps, symbol)
metadata | Company profiles | Redshift - dngo | Nov 28th, 2021 | Nov 28th, 2021 | 10 | symbol

Column | Description | Data type | Constraint | Example |
--- | --- | --- | --- | --- |
currency | currency unit | text | not null | USD |
symbol | company abbr. | text | primary key | AAPL (Apple Inc.) |
instrumentType | type of stock | text | not null | equity |
firstTradeDate | initial public offering (IPO) | text | not null | 1980-12-12 8:30:00 | 
exchangeTimezoneName | timezone name | text | not null | America/New York |
timezone | timezone | text | not null | EST |
trade_period | trade peiod | text | not null | 8:30 - 12:00 |
range | query range | text | not null | 1mo (1 month) |
interval | query interval | text | not null | 1h (1 hour) | 
start_date | the minimum trade datetime in this query | text | not null | 1635341400 |

Figure 6: `metadata`'s schema <br><br>

Column | Description | Data type | Constraint |
--- | --- | --- | --- |
timestamps | timestamps | big int | not null |
volume | total trading volume | decimal | not null |
low | low price at current timeframe | decimal | not null |
open_ | opening price at current timeframe | decimal | not null |
high | high price at current timeframe | decimal | not null |
close | closing price at current timeframe | decimal | not null |
datetime | current datetime | timestamp | not null
symbol | company abbr. | text | not null foreign key

Figure 7: `indicators`'s schema <br><br>

### Step 5: Complete project write up
For this project, I use S3 due to its flexible data storage of any format. I used Redshift due to its ability to scale, high and fast performance for OLAP queries, and no-single-point-failure characteristic. Above all, these three tools are developing open-source projects with active communities that can answer my questions and help me learn more about their implementation in-depth.
<br><br>
Data will be updated every hour as the pipeline (aka my DAG) will run hourly. When it runs, new data will be generated and appended to Redshift as a final destination. Since popular stocks such as AAPL, AMZN, or BAC are highly volatile, the data needs to be frequently updated.
<br><br>
If data was increased by 100x, it is better to increase the number of nodes or node type in Redshift to process data faster (both DDL and DML queries). In Redshift dashboard, it is adviceable to check number of queries and data volume for latency and bottleneck issues. Finally, we should deploy our pipeline to bigger EC2 instances (larger RAM and disk) to run faster.
<br><br>
Since my pipeline will run every hour, it assures that data populating the dashboard can be updated by 7 am every day.
<br><br>
Since Redshift can easily resolve an issue of many connections at once, we don't have to worry about if database is accessed by 100+ people. However, we should assign IAM roles wisely to each person to limit their ability on Redshift. 

