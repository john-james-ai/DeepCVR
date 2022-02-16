#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /extractor.py                                                                         #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, February 14th 2022, 12:32:13 pm                                               #
# Modified : Tuesday, February 15th 2022, 9:40:57 am                                               #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import logging
from airflow.models.baseoperator import BaseOperator

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #


class ExtractOperator(BaseOperator):
    """Decompresses a gzip archive, stores the raw data and pushes the filepaths to xCom

    Args:
        source (str): The source filepath
        destination (str): The destination directory
        kwargs (dict): Default parameters
    """

    def __init__(self, source: str, destination: str, **kwargs) -> None:
        super(ExtractOperator, self).__init__(**kwargs)
        self._source = source
        self._destination = destination

    def execute(self, context) -> None:
        """Extracts and stores the data, then pushes filepaths to xCom. """
        from deepcvr.data.extract import Extractor

        extractor = Extractor(source=self._source, destination=self._destination)
        filepaths = extractor.execute()

        # Push the extracted filepaths to xCom
        ti = context["ti"]
        ti.xcom_push("filepaths", filepaths)
