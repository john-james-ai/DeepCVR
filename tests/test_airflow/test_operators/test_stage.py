#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_stage.py                                                                        #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 5th 2022, 8:10:28 am                                                  #
# Modified : Saturday, March 5th 2022, 6:19:26 pm                                                  #
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

from deepcvr.flow.tasks.stage import StageOperator

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
DATA_INTERVAL_START = pendulum.datetime(2021, 3, 5, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "etl_dag"
TEST_TASK_ID = "stage_task"

SOURCE = "tests/data/development/raw/"
DESTINATION = "tests/data/development/staged/"
DESTINATION_1 = os.path.join(DESTINATION, "sample_skeleton_test.csv")
N_PARTITIONS = 20

FORCE = True


@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule_interval="@once",
        start_date=DATA_INTERVAL_START,
    ) as dag:
        StageOperator(
            task_id=TEST_TASK_ID,
            source=SOURCE,
            destination=DESTINATION,
            n_partitions=N_PARTITIONS,
            force=FORCE,
        )
    return dag


@pytest.mark.stage_operator
def test_stage(dag, reset_db):
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
    assert os.path.exists(DESTINATION_1), logger.error("{} does not exist".format(DESTINATION_1))
