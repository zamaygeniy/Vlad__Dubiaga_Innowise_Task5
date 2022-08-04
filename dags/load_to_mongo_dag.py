from airflow.models import Variable
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.utils.dates import days_ago
from airflow import DAG
import re
from airflow.providers.mongo.hooks.mongo import MongoHook

EMOJI_PATTERN = re.compile(r'[^\!\"\#\$\%\&\'\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^\`\{\|\}\~\w \n]+')
MONGO_CONNECTION_STRING = 'airflow_mongodb'
FILE_PATH = Variable.get('file_path')
MONGO_COLLECTION = 'reviews'
MONGO_DB = 'tiktok_reviews'

mongo_dag = DAG(
    "mongo_dag",
    start_date=days_ago(0, 0, 0, 0, 0)
)


def read_clean_and_load(ti):
    raw_df = pd.read_csv(FILE_PATH)
    clean_df = raw_df.dropna(how='all').fillna('-')
    clean_df[['content']] = clean_df[['content']].replace(EMOJI_PATTERN, '')

    sorted_df = clean_df.sort_values(by=['at'])

    hook = MongoHook(MONGO_CONNECTION_STRING)
    hook.insert_many(mongo_collection=MONGO_COLLECTION, docs=sorted_df.to_dict('records'), mongo_db=MONGO_DB)


read_clean_and_load_to_mongodb = PythonOperator(
    task_id='read_clean_and_load_to_mongodb',
    python_callable=read_clean_and_load,
    dag=mongo_dag
)

read_clean_and_load_to_mongodb
