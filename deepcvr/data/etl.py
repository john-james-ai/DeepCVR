#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /etl.py                                                                               #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Thursday, March 17th 2022, 2:35:47 am                                                 #
# Modified : Thursday, March 17th 2022, 4:52:02 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Extract, Transform Load of Source Data into Database"""
import os
from datetime import datetime
from collections import OrderedDict
from deepcvr.data.core import DagBuilder
from deepcvr.data.core import Task
from deepcvr.utils.config import config_dag
from deepcvr.utils.printing import Printer

# ------------------------------------------------------------------------------------------------ #


class ETL(Task):
    """Extract Transform Load

    Args:
        task_id (int): Sequence number for task
        task_name (str): Name for task
        params (dict): Dictionary containing parameters for the ETL process, such as:
            dataset (str): train and test
            mode (str): development or production

    """

    __config_filepath = {"development": "tests/test_config", "production": "config"}

    def __init__(self, task_id: int, task_name: str, params: list) -> None:
        super(ETL, self).__init__(task_id=task_id, task_name=task_name, params=params)
        self._mode = params["mode"]
        self._dataset = params["dataset"]
        self._config_filepath = (
            ETL.__config_filepath["development"]
            if "dev" in self._mode.lower()
            else ETL.__config_filepath["production"]
        )
        self._start = None
        self._printer = Printer()

    def execute(self) -> None:
        self._hello()
        self._extract()
        self._transform()
        self._load()
        self._goodbye()

    def _extract(self) -> None:
        config_filepath = os.path.join(self._config_filepath, "extract.yaml")
        config = config_dag(config_filepath)

        dag = DagBuilder(config=config).build()
        dag.run()

    def _transform(self) -> None:
        config_filepath = os.path.join(self._config_filepath, "transform.yaml")
        config = config_dag(config_filepath)[self._dataset]

        dag = DagBuilder(config=config).build()
        dag.run()

    def _load(self) -> None:
        config_filepath = os.path.join(self._config_filepath, "load.yaml")
        config = config_dag(config_filepath)[self._dataset]

        credentials_filepath = os.path.join(self._config_filepath, "credentials.yaml")
        credentials = config_dag(credentials_filepath)

        dag = DagBuilder(config=config, context=credentials).build()
        dag.run()

    def _hello(self) -> None:
        self._start = datetime.now()
        self._printer.print_header(header=self._task_name)

    def _goodbye(self) -> None:
        self._end = datetime.now()
        duration = self._end - self._start
        d = OrderedDict()
        d["Task Id"] = self._task_id
        d["Task Name"] = self._task_name
        d["Start"] = self._start
        d["End"] = self._end
        d["Duration"] = duration
        d["Status"] = "ok"
        self._printer.print_dictionary(d)
        self._printer.print_footer()

