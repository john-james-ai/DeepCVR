#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /decorators.py                                                                        #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, March 14th 2022, 7:53:27 pm                                                   #
# Modified : Thursday, March 17th 2022, 7:27:03 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import functools
import pprint
from datetime import datetime
import pandas as pd
from deepcvr.utils.logger import LoggerBuilder

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.colheader_justify", "center")
pd.set_option("display.precision", 2)


# ------------------------------------------------------------------------------------------------ #
logger_builder = LoggerBuilder()
logger = (
    logger_builder.reset()
    .build(name="task_event")
    .set_file_handler()
    .set_level(level="info")
    .logger
)


def event(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        args_fmt = pprint.pformat([arg for arg in args])
        kwargs_fmt = pprint.pformat(kwargs)
        signature = args_fmt + ", " + kwargs_fmt

        logger.info("{} called with {}".format(func.__qualname__, pprint.pformat(signature)))

        try:
            result = func(self, *args, **kwargs)
            return result

        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


def task_event(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        signature = self.__dict__.values()

        logger.info("{} called with {}".format(func.__qualname__, signature))

        try:
            start = datetime.now()
            result = func(self, *args, **kwargs)
            end = datetime.now()
            print_result(self, start, end)
            return result

        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


def print_result(self, start, end):
    task_id = self.__dict__["_task_id"]
    task_name = self.__dict__["_task_name"].replace("_", " ")
    duration = end - start
    duration = duration.total_seconds()
    msg = "\tTask {}:\t{} complete.\tDuration: {} seconds.".format(
        str(task_id), task_name, str(round(duration, 2))
    )
    print(msg)
