#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_extractor.py                                                                    #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, February 14th 2022, 12:59:55 pm                                               #
# Modified : Monday, February 14th 2022, 7:45:37 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import pytest
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.operators.python import PythonOperator
from deepcvr.airflow.operators.extractor import ExtractOperator
from deepcvr.utils.config import Taobao
import pytz

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

config = Taobao()
SOURCE = config.get_train_filepath()
DESTINATION = config.destination
TRAIN = os.path.join(DESTINATION, "train.csv")
TEST = os.path.join(DESTINATION, "test.csv")

DATA_INTERVAL_START = pytz.utc.localize(datetime(2022, 2, 14))
DATA_INTERVAL_END = DATA_INTERVAL_START + timedelta(days=1)
TEST_DAG_ID = "etl_dag"
TEST_TASK_ID = "extractor_task"
PRINT_TASK_ID = "print_xcom"
# ------------------------------------------------------------------------------------------------ #


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule_interval="@daily",
        default_args={"start_date": DATA_INTERVAL_START},
    ) as dag:
        t1 = ExtractOperator(source=SOURCE, destination=DESTINATION, task_id=TEST_TASK_ID)

        def print_xcom(**kwargs):
            logger.debug("\nPrinting Filepaths from Extract Step:\n")
            ti = kwargs["ti"]
            filepaths = ti.xcom_pull(task_ids=TEST_TASK_ID, key="filepaths")
            print(filepaths)

        # Prints the xCom from the extraction step
        t2 = PythonOperator(task_id=PRINT_TASK_ID, python_callable=print_xcom)

        t1 >> t2

    return dag


def test_extractor(dag, caplog, reset_db):

    reset_db
    caplog.set_level(logging.DEBUG)

    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID)
    ti.task = dag.get_task(task_id=TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    # Get second task
    ti = dagrun.get_task_instance(task_id=PRINT_TASK_ID)
    ti.task = dag.get_task(task_id=PRINT_TASK_ID)
    ti.run(ignore_ti_state=True)


if __name__ == "__main__":
    logger.info(" Started ETL Pipeline Tests ")
    test_extractor()
    logger.info(" Completed ETL Pipeline Tests ")

#%%
