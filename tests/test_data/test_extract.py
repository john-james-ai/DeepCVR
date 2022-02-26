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
# Modified : Friday, February 25th 2022, 9:58:11 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #

#%%
import pytest
import logging
import inspect

from deepcvr.data.extract import Extractor

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.extract
class TestExtract:
    def test_extract(self) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        source = "data/external/taobao_test.tar.gz"
        destination = "data/raw"

        extractor = Extractor(source=source, destination=destination)
        extractor.execute()

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )


if __name__ == "__main__":

    t = TestExtract()
    t.test_extract()
#%%
