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
# Modified : Thursday, March 31st 2022, 5:59:54 pm                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import functools
from datetime import datetime
import pandas as pd
from deepcvr.utils.logger import LoggerBuilder

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.colheader_justify", "center")
pd.set_option("display.precision", 2)


# ------------------------------------------------------------------------------------------------ #


def event(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):

        logger_builder = LoggerBuilder()

        logger = (
            logger_builder.reset()
            .set_events_logfile()
            .set_operations_logfile()
            .set_level(level="info")
            .build(name=func.__module__)
            .logger
        )

        signature = self.__dict__.values()

        logger.info("{} called with {}".format(func.__qualname__, signature))

        try:
            result = func(self, *args, **kwargs)
            return result

        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


def operator(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):

        logger_builder = LoggerBuilder()

        logger = (
            logger_builder.reset()
            .set_events_logfile()
            .set_operations_logfile()
            .set_level(level="info")
            .build(name=func.__module__)
            .logger
        )

        try:
            module = func.__module__
            classname = func.__qualname__
            start = datetime.now()
            result = func(self, *args, **kwargs)
            end = datetime.now()
            print_result(module, classname, self, start, end)
            return result

        except Exception as e:
            logger.exception(f"Exception raised in {func.__name__}. exception: {str(e)}")
            raise e

    return wrapper


def print_result(module: str, classname: str, self: str, start: datetime, end: datetime):
    task_id = self.__dict__["_task_id"]
    task_name = self.__dict__["_task_name"]
    duration = end - start
    duration = duration.total_seconds()
    module = module.split(".")[2]
    msg = "Module: {}\t\tTask {}:\t{}\tComplete.\tDuration:{} seconds.".format(
        str(module), str(task_id), task_name, str(round(duration, 2))
    )
    print(msg)
