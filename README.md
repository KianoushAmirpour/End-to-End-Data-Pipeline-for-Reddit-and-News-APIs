## Reddit-News-Data-Pipeline

## why i didnt use the xcom push????

## CI pipeline and airflow tests
## how to setup

## Pipeline
- Pipeline
 
![pipeline](https://github.com/KianoushAmirpour/Reddit-News-Data-Pipeline/assets/112323618/15822741-d3b6-4950-ae73-8d82d01661e7)

- Airflow dag
  
![dags](https://github.com/KianoushAmirpour/Reddit-News-Data-Pipeline/assets/112323618/eb634b1e-2145-4fb8-8d41-6353ef5dac9c)

## Workflow

- Data is collected from two APIs (reddit API and News API) for a specified subject.
  -  Top, hot, and new posts are fetched for the specified subject.
  -  NewsAPI will return the top headlines for the specified category.
  
- Collected Data from APIs is stored in Landing zone bucket (S3 Compatible Object Storage).
  - for managing the cloud storage, AWS SDK for Python (Boto3) is used.
    
- Data is read from landing zone bucket and data cleaning process will be performed on them using pandas library.
  - Special characters will be removed.
  - Unnecessary new lines will be removed.
  - Quotes will be standardized.
  - Useful keys in data will be extracted.
    
- Transformed and cleaned data is sent to processed zone bucket.
  
- Staging tables are created and the transformed data will be copied into them. these tasks are done by Postgresoperator of airflow.
  
- Data quality checks are executed to make sure that end user will get the accurate data.
  - With the help of SQLColumnCheckOperator from Airflow, criteria for null values, minimum and maximum values, and distinct values will be checked.
  - With the help of SQLTableCheckOperator from Airflow, criteria for the number of rows in the table and the date range of a table will be checked.
 
- if the data qulity checks pass, data will be stored in data warehouse tables.

## Tools:
- Cloud : [Chabokan](https://chabokan.net/)
- Containerization : Docker, Docker Compose
- Orchestration : Apache Airflow
- Transformation : Pandas
- Cloud Storage Buckets : Minio
- Data Warehouse: PostgreSQL

