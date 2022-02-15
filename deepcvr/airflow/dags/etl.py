#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /etl.py                                                                               #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, February 14th 2022, 5:58:20 am                                                #
# Modified : Monday, February 14th 2022, 12:43:06 pm                                               #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow import DAG
import pandas as pd

from deepcvr.airflow import EMAIL
from deepcvr.airflow.operators.extractor import ExtractOperator

# ------------------------------------------------------------------------------------------------ #


class ETL:
    """Extracts, Transforms and Loads a downsample of the Taobao dataset."""

    def __init__(self, source: str, destination: str) -> None:
        self._source = source
        self._destination = destination
        self._data = None
        self._dag = None

        self._default_args = {
            "owner": "deepcvr",
            "depends_on_past": False,
            "email": EMAIL,
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=30),
        }

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    def execute(self) -> None:
        """Orchestrates the construction and execution of an Airflow DAG"""

        with DAG(
            "ETL",
            default_args=self._default_args,
            description="Extract Transform Load Taobao Data",
            start_date=datetime(2022, 2, 15),
            catchup=False,
            tags=["etl"],
        ) as dag:

            dag.doc_md = dedent(
                """\
            # Extracts data from source, stores in destination directory and
            pushes the filepaths to xCom.
                    """
            )

            t1 = ExtractOperator(
                task_id="extract",
                source=self._source,
                destination=self._destination,
                depends_on_past=False,
                do_xcom_push=True,
            )
            t1.doc_md = dedent(
                """\
            # Extracts data from source, stores in destination directory and
            pushes the filepaths to xCom.
                    """
            )

            t2 = BashOperator(
                task_id="bash_pull",
                bash_command='echo "bash pull demo" && '
                f'echo "The xcom pushed manually is {t1.output["train"]}" && '
                f'echo "The returned_value xcom is {t1.output["test"]}" && '
                'echo "finished"',
                do_xcom_push=False,
            )

        t1 >> t2
