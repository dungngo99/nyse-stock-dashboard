# [NYSE Stock Dashboard](https://github.com/dungngo99/nyse-stock-dashboard) &middot; [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/dungngo99/nyse-stock-dashboard/blob/main/LICENSE)

A final capstone project for my Udacity's Data Engineering Nanodegree where I track real-time performance of popular stock listed on NYSE by building an interactive dashboard to provide the users with up-to-date market data.

## Link to live production:

## 📚 Table of contents

- [Technical stack](#technical-stack)
- [Introduction](#introduction)
- [Project Plan](#projectplan)
- [Conclusion](#conclusion)
- [Note](#note)
- [License](#license)

## 🛠 Technical stack

- Python libraries:
  1. Data Science libraries: numpy, pandas, matplotlib, and plotly
  2. Data storage: S3 and Redshift
  3. Task scheduling and ETL pipeline: Airflow
  4. Dashboard: Dash and Flask
  5. Deployment: Docker and EC2

## 🎯 Introduction
In this project, I plan to build an ETL pipeline that runs and is scheduled in Airflow to fetch timeseries stock data from Yahoo Finance API, then load to S3 as staging, and to Redshift for future analysis. Once data are loaded, the dashboard, built in Dash and Flask framework, can query from Redshift using GraphQL and display the charts.

## ⬇ Project Plan
The project will follow these main milestones:
1. Scope the project and gather data (from Yahoo Finance API)
2. Design the architecture to store the data (in S3 and Redshift)
3. Build ETL pipelines to load data from API to S3 and Redshift
4. Design API endpoints in Flask that can talk to Redshift to retrieve data
5. Build the dashboard in Dash

## 🤩 Project Structure
1. data: a folder to host both experimental data use for data exploration and analysis
2. notebooks: a folder as a work space to plan, design, and test codes
3. images: a folder to store all screenshots of my result
4. etl: a folder for ETL pipeline
5. flask: a folder for flask server that host the dashboard

## 😀 Conclusion
Throughout the course and this capstone project, I have a chance to work with different technologies widely used for a Data Engineering role. I learned
  1. How to efficiently model data through star schema by writing SQL queries in PostgreSQL
  2. How to move data across different cloud platforms (S3, Redshift, EMR)
  3. How to write Spark application to analyze big data with the benefit of parallel computing
  4. How to build data pipelines and schedule tasks with Airflow.
  5. How to use Github to control version while working with Jupyter Notebook.
  
## 🚀 Future improvement
...TBD..

## 📄 License

NYSE Stock Dashboard is [MIT licensed](./LICENSE).