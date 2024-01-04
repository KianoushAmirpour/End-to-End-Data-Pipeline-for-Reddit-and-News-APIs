## Reddit-News-Data-Pipeline
### test and ci pirpline

### Pipeline
- Pipeline
 
![pipeline](https://github.com/KianoushAmirpour/Reddit-News-Data-Pipeline/assets/112323618/15822741-d3b6-4950-ae73-8d82d01661e7)

- Airflow dag
  
![dags](https://github.com/KianoushAmirpour/Reddit-News-Data-Pipeline/assets/112323618/eb634b1e-2145-4fb8-8d41-6353ef5dac9c)

### Workflow

- Data is collected from two APIs (reddit API and News API) for a specified subject.
  -  Top, hot, and new posts are fetched for the specified subject.
  -  NewsAPI will return the top headlines for the specified category.
  
- Collected Data from APIs is stored in Landing zone bucket (S3 Compatible Object Storage).
  - for managing the cloud storage, AWS SDK for Python `(Boto3)` is used.
    
- Data is read from landing zone bucket and data cleaning process will be performed on them using pandas library.
  - Special characters will be removed.
  - Unnecessary new lines will be removed.
  - Quotes will be standardized.
  - Useful keys in data will be extracted.
    
- Transformed and cleaned data is sent to processed zone bucket.
  
- Staging tables are created and the transformed data will be copied into them. these tasks are done by `Postgresoperator` of airflow.
  
- Data quality checks are executed to make sure that end user will get the accurate data.
  - With the help of `SQLColumnCheckOperator` from Airflow, criteria for null values, minimum and maximum values, and distinct values will be checked.
  - With the help of `SQLTableCheckOperator` from Airflow, criteria for the number of rows in the table and the date range of a table will be checked.
 
- if the data qulity checks pass, data will be stored in data warehouse tables.

### Tools:
- Cloud : [Chabokan](https://chabokan.net/)
- Containerization : Docker, Docker Compose
- Orchestration : Apache Airflow
- Transformation : Pandas
- Cloud Storage Buckets : Minio
- Data Warehouse: PostgreSQL

### How to setup
1. Create an account on reddit and get the credentials from [here](https://www.reddit.com/prefs/apps).
2. Get the API key from [NewsAPI](https://newsapi.org/).
3. Install [docker](https://docs.docker.com/engine/install/).
4. Based on `template.env` file, set up your cloud infrastructure.
    We used a `FERNET_KEY` for airflow configuration. You can use the below code:
   
   ```
   from cryptography.fernet import Fernet
   fernet_key = Fernet.generate_key()
   print(fernet_key.decode())  # your fernet_key, keep it in secured place!
   ```
  
6. Run `docker compose build`. (you can also follow the instructions from [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) to run airflow with docker compose.)
7. Run `docker compose up airflow-init`
8. Run `docker compose up`
9. Schedule your airflow dag or you can trigger it manually.
10. Run `docker compose down`.

### Chat with database
Utilizing Langchain and Hugging Face, we leveraged the Language Models' capability to interact with the database.
