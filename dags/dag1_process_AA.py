import os
from airflow.decorators import dag, task, task_group
from pendulum import datetime
import pandas as pd
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.sdk.definitions.asset import Asset
from airflow.utils.edgemodifier import Label
from shared_assets import aa_asset

FILE_PATH = "/opt/airflow/share/tiktok_google_play_reviews.csv"
FILE_PATH_UPD = "/opt/airflow/share/tiktok_google_play_reviews_upd.csv"


# 1st DAG for proccessing file
@dag(
    dag_id="dag1_process",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["Traineeship"],
    description="DAG for proccessing a file",
)
def dag1_process():

    # 1. create SENSOR to check existance of a file. The sensor is waiting for the file.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="tiktok_google_play_reviews.csv",
        fs_conn_id="file_AA",
        mode="poke",
        poke_interval=10,
        timeout=60 * 30,
        soft_fail=False,
    )

    # 2. TASK.BRANCH to check whether the file is empty
    @task.branch(task_id="empty_file_check")
    def empty_file_check():
        if not os.path.exists(FILE_PATH) or os.path.getsize(FILE_PATH) == 0:
            return "log_empty_file"

        else:
            return "data_processing_group"

    # later, we'll use check_file for dependencies
    check_file = empty_file_check()

    # 3.1. If the file is empty, BASH SCRIPT that logs the fact that the file is empty
    log_empty = BashOperator(
        task_id="log_empty_file",
        bash_command=(
            "mkdir -p /opt/airflow/logs/my_logs && "
            "echo '[{{ ts }}] [{{ dag.dag_id }}] [{{ run_id }}] Error: File at "
            + FILE_PATH
            + " is empty!' "
            ">> /opt/airflow/logs/my_logs/dag1_process_AA.txt"
        ),
    )

    # 3.2 If the file contains data, run 3 tasks in GROUP
    @task_group(group_id="data_processing_group")
    def data_processing_group():
        @task(
            task_id="replace_nulls",
            trigger_rule="all_success",
        )
        def replace_nulls():
            df = pd.read_csv(FILE_PATH)
            df = df.fillna("-")
            df.to_csv(FILE_PATH_UPD, index=False)
            return FILE_PATH_UPD

        @task(
            task_id="sort_data",
            trigger_rule="all_success",
        )
        def sort_data(file_path: str):
            df2 = pd.read_csv(file_path)
            df2 = df2.sort_values(by="reviewCreatedVersion", ascending=True)
            df2.to_csv(file_path, index=False)
            return file_path

        @task(
            task_id="clean_content",
            outlets=[aa_asset],
            trigger_rule="all_success",
        )
        def clean_content(file_path: str):
            df3 = pd.read_csv(file_path)
            df3["content"] = (
                df3["content"].astype(str).str.replace(r"[\n\r\t]+", " ", regex=True)
            )

            clean_pattern = r"[^ \w.,!?;:\"'()\-_\/]"
            df3["content"] = (
                df3["content"].astype(str).str.replace(clean_pattern, "", regex=True)
            )
            df3.to_csv(file_path, index=False)

        # add dependencies
        replace_null = replace_nulls()
        clean_content(sort_data(replace_null))

    processing_group_instance = data_processing_group()

    wait_for_file >> check_file
    check_file >> Label("file is empty") >> log_empty
    check_file >> Label("file is not empty") >> processing_group_instance


dag1_process()
