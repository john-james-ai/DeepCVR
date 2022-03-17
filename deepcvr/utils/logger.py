#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /logger.py                                                                            #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, March 13th 2022, 1:41:40 pm                                                   #
# Modified : Monday, March 14th 2022, 11:56:07 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import logging
from logging.handlers import TimedRotatingFileHandler

# ------------------------------------------------------------------------------------------------ #


class LoggerBuilder:
    """Builds a logger to specification"""

    __level = {
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "warn": logging.WARN,
        "error": logging.ERROR,
    }

    def __init__(self) -> None:
        self._logger = None
        self.reset()

    def reset(self):
        self._logger = None
        return self

    @property
    def logger(self) -> logging.getLogger:
        return self._logger

    def set_console_handler(self):
        """Formats the handler and sets logger"""
        # Obtain handller
        handler = logging.StreamHandler()
        # Configure formatter
        format = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
        # Set format on handler
        handler.setFormatter(format)
        # Add handler to logger
        self._logger.addHandler(handler)
        return self

    def set_file_handler(self, filepath: str = "logs/deepcvr.log"):
        """Formats the handler and sets logger"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        # Time rotating file handler is preferred
        handler = TimedRotatingFileHandler(filename=filepath)
        # Configure formatter for files to include time
        format = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
        # Set format on handler
        handler.setFormatter(format)
        # Add handler to logger
        self._logger.addHandler(handler)
        return self

    def set_level(self, level: str = "info"):
        self._logger.setLevel(LoggerBuilder.__level.get(level, logging.INFO))
        return self

    def build(self, name: str):
        # Instantiate logger with 'info' level. User can override
        self._logger = logging.getLogger(name)
        self._logger.setLevel(LoggerBuilder.__level.get("info", logging.INFO))
        return self
