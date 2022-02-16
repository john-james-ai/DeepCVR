#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_sampling.py                                                                     #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, February 15th 2022, 9:44:17 am                                               #
# Modified : Tuesday, February 15th 2022, 10:32:22 am                                              #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import pytest
import logging
import inspect
import pandas as pd
import numpy as np

from deepcvr.data.sampling import MultilabelStratifiedRandomSampling

# ---------------------------------------------------------------------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------- #


@pytest.mark.sampling
class MultilabelStratifiedRandomSamplingTests:
    def __init__(self):
        self.population = 1000000
        y = np.random.choice(a=[0, 1], size=self.population)
        z = np.random.choice(a=[0, 1], size=self.population)
        self.df = pd.DataFrame(data={"y": y, "z": z})

    def test_sampling(self) -> None:
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        sampler = MultilabelStratifiedRandomSampling(labels=['y','z'],size=0.1)
        df = sampler.sample()
        assert len(df) <=

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack[0][3])
        )

    def test_teardown(self):
        logger.info("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        logger.info(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack[0][3])
        )


if __name__ == "__main__":

    t = SomeTests()
    t.test_setup()
    t.test_something()
    t.test_teardown()
