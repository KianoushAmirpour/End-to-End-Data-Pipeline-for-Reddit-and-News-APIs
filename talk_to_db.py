import os
from dotenv import dotenv_values
from langchain import HuggingFaceHub
from langchain.sql_database import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain

config = dotenv_values('.env')
HUGGINGFACEHUB_API_TOKEN = config['HUGGINGFACE_API']
os.environ["HUGGINGFACEHUB_API_TOKEN"] = HUGGINGFACEHUB_API_TOKEN
db_uri = f"postgresql+psycopg2://{config['DB_USER']}:{config['DB_PASSWORD']}@{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_NAME']}?options=-csearch_path%3Ddbo,{config['WAREHOUSE_SCHEMA']}"

db = SQLDatabase.from_uri(db_uri)

# model_id = 'meta-llama/Llama-2-7b-chat-hf'
# repo_id  = "Salesforce/xgen-7b-8k-base"
# repo_id = "databricks/dolly-v2-3b"
repo_id = "google/flan-t5-xxl"

llm = HuggingFaceHub(
    repo_id=repo_id, model_kwargs={"temperature": 0.01, "max_length": 64}
)

db_chain = SQLDatabaseChain(llm=llm, database=db, verbose=True)

print(db_chain.run("how many tables are in this schema?"))
