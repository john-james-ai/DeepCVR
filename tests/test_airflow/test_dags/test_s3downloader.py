#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_s3downloader.py                                                                 #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, February 27th 2022, 8:54:52 am                                                #
# Modified : Wednesday, March 2nd 2022, 11:16:55 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
import datetime
import pendulum

import pytest

from airflow import DAG
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.state import State

from deepcvr.airflow.operators.download import S3DownloadOperator
from deepcvr.utils.config import S3Config

# ------------------------------------------------------------------------------------------------ #
DATA_INTERVAL_START = pendulum.datetime(2021, 9, 13, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "etl_dag"
TEST_TASK_ID = "s3_download_task"
CREDENTIALS = S3Config()
BUCKET = "deepcvr-data"
DESTINATION = "data/external"
FORCE = False
FILEPATH = "data/external/taobao_test.tar.gz"


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule_interval="@once",
        start_date=DATA_INTERVAL_START,
    ) as dag:
        S3DownloadOperator(
            task_id=TEST_TASK_ID,
            credentials=CREDENTIALS,
            bucket=BUCKET,
            destination=DESTINATION,
            force=FORCE,
        )
    return dag


@pytest.mark.s3
def test_s3_download_operator_execute_no_trigger(dag, reset_db):
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
    assert ti.state == State.SUCCESS
