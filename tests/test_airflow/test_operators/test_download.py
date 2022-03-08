#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_download.py                                                                     #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 5th 2022, 1:54:21 am                                                  #
# Modified : Saturday, March 5th 2022, 6:19:28 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import os
import datetime
import pendulum

import pytest
import logging

from airflow import DAG
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.state import State

from deepcvr.flow.tasks.download import S3DownloadOperator
from deepcvr.utils.config import S3Config

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
DATA_INTERVAL_START = pendulum.datetime(2021, 3, 5, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "etl_dag"
TEST_TASK_ID = "downloader_task"

config = S3Config()
BUCKET = "deepcvr-data"
KEY = config.key
PASSWORD = config.password
FOLDER = "development/"
DESTINATION = "tests/data/development/external/"
TARGET_1 = "tests/data/development/external/taobao_train.tar.gz"
TARGET_2 = "tests/data/development/external/taobao_test.tar.gz"
FORCE = False


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule_interval="@daily",
        start_date=DATA_INTERVAL_START,
    ) as dag:
        S3DownloadOperator(
            task_id=TEST_TASK_ID,
            bucket=BUCKET,
            key=KEY,
            password=PASSWORD,
            folder=FOLDER,
            destination=DESTINATION,
            force=FORCE,
        )
    return dag


@pytest.mark.download_operator
def test_download(dag, reset_db):
    reset_db
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
    assert ti.state == State.SUCCESS, logger.error("Task did not succeed!")
    assert os.path.exists(TARGET_1), logger.error("{} does not exist".format(TARGET_1))
    assert os.path.exists(TARGET_2), logger.error("{} does not exist".format(TARGET_2))
