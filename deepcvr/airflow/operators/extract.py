#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /download.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, February 27th 2022, 8:31:48 am                                                #
# Modified : Sunday, February 27th 2022, 9:51:17 am                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Airflow operator responsible for downloading source data from an Amazon S3 resource. """
from airflow.models.baseoperator import BaseOperator
from deepcvr.data.extract import Extractor

# ------------------------------------------------------------------------------------------------ #


class GZIPExtractOperator(BaseOperator):
    """GZIP Extract Operator

    Args:
        source (str): The filepath to the source file to be decompressed
        destination (str): The destination directory into which data shall be stored.
        force (bool): Forces extraction even when files already exist.
    """

    def __init__(self, source: str, destination: str, force: bool = False, **kwargs) -> None:
        super(GZIPExtractOperator, self).__init__(**kwargs)
        self._extractor = Extractor(source=source, destination=destination, force=force)

    def execute(self, context) -> None:
        self._extractor.execute()
