from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
import subprocess
from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timezone

import os
import logging
from dotenv import load_dotenv

load_dotenv()

DBT_VENV_BIN = os.getenv("DBT_VENV_BIN", "/home/dottier/venvs/dbt-venv/bin/dbt")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/home/dottier/flights_etl/dbt_flights")


def log_subprocess_result(result):
    logging.info("===== STDOUT =====")
    for line in result.stdout.splitlines():
        logging.info(f"[OUT] {line}")

    logging.info("===== STDERR =====")
    for line in result.stderr.splitlines():
        logging.info(f"[ERR] {line}")


def run_dbt_model(dbt_model: str = "model", first_run_identifier: str = None):
    logging.info(f"Running DBT model: {dbt_model}")

    excluded_tags = []

    def is_first_run(identifier):
        # So the latter check always fail, prolly could do better but whatever
        if not identifier:
            return True
        
        first_run = Variable.get(identifier, default="True") == "True"
        if first_run:
            Variable.set(identifier, "False")
            return True
        return False
    
    if not is_first_run(first_run_identifier):
        excluded_tags.append("run_once")
        logging.info("Excluding 'run_once' models for subsequent runs.")

    dbt_cmd = f"{DBT_VENV_BIN} run --select {dbt_model}"
    if excluded_tags:
        exclude_str = " ".join([f"tag:{t}" for t in excluded_tags])
        dbt_cmd += f" --exclude {exclude_str}"

    logging.info(f"Running DBT command: {dbt_cmd}")
    result = subprocess.run(
        dbt_cmd,
        shell=True,
        cwd=DBT_PROJECT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    logging.info(f"DBT command {dbt_model} completed successfully")

    log_subprocess_result(result)

    if result.returncode != 0:
        raise Exception(f"DBT command {dbt_model} failed with return code {result.returncode}")

@dag(
    schedule=None,
    start_date=datetime(2025, 11, 14, tzinfo=timezone.utc),
    catchup=False,
    tags=["dbt"],
)
def dbt_transform():
    # wait_pipeline = ExternalTaskSensor(
    #     task_id="wait_pipeline",
    #     external_dag_id="pipeline_run",
    #     external_task_id=None,
    #     mode="poke",
    #     timeout=600,
    #     poke_interval=30,
    #     failed_states=["failed"],
    #     allowed_states=["success"],
    # )

    @task()
    def run(**context):
        logging.info("Dbt task started")

        conf = context["dag_run"].conf or {}
        run_staging = conf.get("run_staging", False)

        if run_staging:
            run_dbt_model("staging")
        
        run_dbt_model("marts", "first_run_marts")

        logging.info("Dbt task finished")
    run()

dbt_transform()