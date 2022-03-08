#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /extract.py                                                                           #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Wednesday, March 2nd 2022, 8:48:43 pm                                                 #
# Modified : Tuesday, March 8th 2022, 7:05:05 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Extract DAG Factor"""
import os
import logging
import pendulum
from datetime import datetime
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from airflow.utils.state import State

from deepcvr.utils.config import config_dag
from plugins.operators.download import S3DownloadOperator
from plugins.operators.extract import ExtractOperator
from plugins.operators.stage import StageOperator

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
config_filepath = os.path.join("config", os.path.splitext(os.path.basename(__file__))[0] + ".yaml")
config = config_dag(config_filepath)
# ------------------------------------------------------------------------------------------------ #
DATA_INTERVAL_START = pendulum.datetime(2021, 3, 5, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)


class ExtractDAG:
    """Produces a the DAG for the extract phase of the ETL

    Args:
        config_filepath (str): filepath containing the yaml configuration. Defaults to
        a yaml file with the same basename as the current file.
        development (bool): Specifies whether to use the development configuration.
        If False, the production configuration is used. Defaults to True.

    """

    __config_filepath = os.path.join(
        "config", os.path.splitext(os.path.basename(__file__))[0] + ".yaml"
    )

    def __init__(self, config_filepath: str = None, development: bool = True) -> None:
        self._config_filepath = config_filepath or ExtractDAG.__config_filepath
        self._development = development
        self._interval_start = None
        self._interval_end = None

    def create_dag(self) -> DAG:

        mode = "development" if self._development else "production"

        config = self._config_dag(self._config_filepath)[mode]

        with DAG(
            dag_id=config["dag_id"],
            description=config["dag_description"],
            default_args={
                "depends_on_past": False,
                "retries": 1,
                "relay_delay": timedelta(minutes=5),
                "owner": "john",
            },
            schedule_interval="@once",
            start_date=datetime(2022, 3, 5),
            catchup=False,
            tags=["extract"],
        ) as dag:

            t1 = S3DownloadOperator(
                task_id=config["download"]["task_id"],
                bucket=config["download"]["bucket"],
                key=config["download"]["key"],
                password=config["download"]["password"],
                folder=config["download"]["folder"],
                destination=config["download"]["destination"],
                objects=config["download"]["objects"],
                force=config["download"]["force"],
                owner="john",
            )

            t2 = ExtractOperator(
                task_id=config["extract"]["task_id"],
                source=config["extract"]["source"],
                destination=config["extract"]["destination"],
                force=config["extract"]["force"],
                owner="john",
            )

            t3 = StageOperator(
                task_id=config["stage"]["task_id"],
                source=config["stage"]["source"],
                destination=config["stage"]["destination"],
                n_partitions=config["stage"]["n_partitions"],
                force=config["stage"]["force"],
                owner="john",
            )

            t1 >> t2 >> t3
        return dag

globals()[]