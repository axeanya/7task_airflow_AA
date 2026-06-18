from airflow.decorators import dag, task
from pendulum import datetime
from airflow.sdk.definitions.asset import Asset
from airflow.providers.mongo.hooks.mongo import MongoHook
import pandas as pd
from shared_assets import aa_asset


# 2nd DAG for loading the proccessed file into Mongo
@dag(
    dag_id="dag2_mongo_load",
    schedule=[aa_asset],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["Traineeship"],
    description="DAG for loading file to MongoDB",
)
def dag2_mongo_load():

    @task
    def load_2mongo_task():
        # Define path of the file
        file_path = aa_asset.uri.replace("file:///localhost", "")

        # transform file to format acceptable by Mongo
        df = pd.read_csv(file_path, keep_default_na=False)
        records = df.to_dict(orient="records")

        # create connection to MongoDB
        mongo_hook = MongoHook(mongo_conn_id="mongo_AA")
        # connect (create if not exists) to database airflow_practice
        db = mongo_hook.get_conn()["airflow_practice"]
        # connect (create if not exists) to collection(table) tiktok_reviews
        collection = db["tiktok_reviews"]

        # Delete previous data from the collection
        collection.delete_many({})

        # Load data to Mongo
        collection.insert_many(records)
        print("AA Loading Successfull")

    load_2mongo_task()


dag2_mongo_load()
