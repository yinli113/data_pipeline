
# Sparkify Airflow Data Pipeline

## Introduction
Sparkify, a music streaming company, has decided to automate and monitor their data warehouse ETL pipelines using Apache Airflow. The goal is to create dynamic, high-grade data pipelines that are built from reusable tasks, can be monitored, and allow easy backfills. Moreover, data quality checks are integral to ensure the integrity of data within the data warehouse.

## Project Overview
This project involves the creation of custom operators in Airflow to stage the data, fill the data warehouse, and run checks on the data quality. The source data is in JSON format and resides in S3, which needs to be processed in Sparkify's data warehouse in Amazon Redshift.

## Prerequisites
- Create an IAM User in AWS.
- Create a Redshift cluster in the `us-west-2` region.
- Set up connections between Airflow and AWS.
- Set up a connection between Airflow and the AWS Redshift Cluster.

## Datasets
The datasets are provided in two S3 buckets:
- Log data: `s3://udacity-dend/log_data`
- Song data: `s3://udacity-dend/song_data`

## Project Template
The project template includes:
- A DAG template with all imports and task templates.
- Operator templates within the `operators` folder.
- A helper class for SQL transformations.

## Configuring the DAG
The DAG is configured with the following parameters:
- No dependencies on past runs.
- On failure, tasks are retried 3 times.
- Retries happen every 5 minutes.
- Catchup is turned off.
- No email notifications on retry.

Task dependencies are set to follow the logical flow of the data pipeline.

## Building the Operators
### Stage Operator
Loads JSON formatted files from S3 to Redshift. It uses a templated field to load timestamped files from S3.

### Fact and Dimension Operators
Utilize the provided SQL helper class to run data transformations. Supports both the truncate-insert pattern for dimension tables and append functionality for fact tables.

### Data Quality Operator
1. Runs checks on the data, such as verifying if certain columns contain NULL values. Raises an exception if the checks fail.
2. As there are still some NULL values in tables, running the code below in redshift cluster with the query editor untill there is no NULL values and data quality check passed.
-  ``` DELETE FROM public."time" WHERE start_time IS NULL;```
-  ``` DELETE FROM public.artists WHERE name IS NULL;```
-  ``` DELETE FROM public.songs WHERE title IS NULL;```
-  ``` DELETE FROM public.users WHERE first_name IS NULL;```
-  ``` DELETE FROM public.songplays WHERE start_time IS NULL;```


## Authors
- [Yin Li]

## Acknowledgements
- Udacity for providing the project template and guidelines.
- The Sparkify team for their support and collaboration.


```python

```
