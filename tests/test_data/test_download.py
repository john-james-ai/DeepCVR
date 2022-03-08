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
# Created  : Friday, February 25th 2022, 4:08:17 pm                                                #
# Modified : Saturday, March 5th 2022, 2:52:28 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #

#%%
import os
import pytest
import logging
import inspect

from deepcvr.data.download import S3Downloader
from deepcvr.utils.config import S3Config

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.download
class TestS3Download:
    def test_download(self) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        config = S3Config()
        destination = "tests/data/external"
        folder = "development/"

        downloader = S3Downloader(
            bucket=config.bucket,
            key=config.key,
            password=config.password,
            folder=folder,
            destination=destination,
            objects=None,
            force=False,
        )
        downloader.execute()
        # Downloads all objects in folder if objects is None
        objects = ["taobao_train.tar.gz", "taobao_test.tar.gz"]
        for object in objects:
            filepath = os.path.join(destination, object)
            assert os.path.exists(filepath), logger.error("\tDestination file does not exist")
            os.remove(filepath)

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )
