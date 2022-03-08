#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_extract.py                                                                      #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 5th 2022, 8:09:23 am                                                  #
# Modified : Saturday, March 5th 2022, 6:19:29 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Tests the extract phase of the ETL"""
import os
import datetime
import pendulum

import pytest
import logging

from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.state import State

from deepcvr.flow.dags.extract_dag import ExtractDAGBuilder
from deepcvr.flow.tasks.download import DownloadConfig
from deepcvr.flow.tasks.extract import ExtractConfig
from deepcvr.flow.tasks.stage import StageConfig

from deepcvr.utils.config import S3Config

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #
DATA_INTERVAL_START = pendulum.datetime(2021, 3, 5, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

# ------------------------------------------------------------------------------------------------ #
# Download config
TASK_ID_1 = "download"
BUCKET = "deepcvr-data"
CONFIG = S3Config()
FOLDER = "development"
DESTINATION_1 = "data/development/external"
FORCE = True
download_config = DownloadConfig(
    task_id=TASK_ID_1,
    bucket=CONFIG.bucket,
    key=CONFIG.key,
    password=CONFIG.password,
    folder=FOLDER,
    destination=DESTINATION_1,
    force=FORCE,
)
# ------------------------------------------------------------------------------------------------ #
# Extract config
TASK_ID_2 = "extract"
SOURCE = "data/development/external"
DESTINATION_2 = "data/development/raw"
extract_config = ExtractConfig(
    task_id=TASK_ID_2, source=SOURCE, destination=DESTINATION_2, force=FORCE
)
# ------------------------------------------------------------------------------------------------ #
# Stage config
TASK_ID_3 = "stage"
SOURCE = "data/development/external"
DESTINATION_3 = "data/development/staged"
N_PARTITIONS = 20
stage_config = StageConfig(
    task_id=TASK_ID_3,
    source=SOURCE,
    destination=DESTINATION_3,
    n_partitions=N_PARTITIONS,
    force=FORCE,
)
# ------------------------------------------------------------------------------------------------ #
dag = ExtractDAGBuilder(
    download_config=download_config, extract_config=extract_config, stage_config=stage_config
)


@pytest.mark.extract_dag
def test_extract_dag(reset_db):
    reset_db
    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )
    # Task 1: Download
    ti = dagrun.get_task_instance(task_id=TASK_ID_1)
    ti.task = dag.get_task(task_id=TASK_ID_1)
    ti.run(ignore_ti_state=True)
    assert ti.state == State.SUCCESS, logger.error("Task did not succeed!")
    assert len(os.listdir(DESTINATION_1)) == 2, logger.error(
        "Download Task Error: Invalid number of files in the external directory"
    )

    # Task 2: Extract
    ti = dagrun.get_task_instance(task_id=TASK_ID_2)
    ti.task = dag.get_task(task_id=TASK_ID_2)
    ti.run(ignore_ti_state=True)
    assert ti.state == State.SUCCESS, logger.error("Task did not succeed!")
    assert len(os.listdir(DESTINATION_2)) == 4, logger.error(
        "Extract Task Error: Invalid number of files in the raw directory"
    )

    # Task 3: Stage
    ti = dagrun.get_task_instance(task_id=TASK_ID_3)
    ti.task = dag.get_task(task_id=TASK_ID_3)
    ti.run(ignore_ti_state=True)
    assert ti.state == State.SUCCESS, logger.error("Task did not succeed!")
    assert len(os.listdir(DESTINATION_3)) == 4, logger.error(
        "Stage Task Error: Invalid number of files in the staged directory"
    )
