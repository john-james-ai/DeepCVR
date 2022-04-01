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
# Modified : Thursday, March 31st 2022, 5:14:01 pm                                                 #
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
        self._logger = None
        self._level = "info"
        self._operations_logfile = "logs/operations.log"
        self._events_logfile = "logs/events.log"
        self.reset()

    def reset(self):
        self._logger = None
        self._level = "info"
        self._operations_logfile = "logs/operations.log"
        self._events_logfile = "logs/events.log"
        return self

    @property
    def logger(self) -> logging.getLogger:
        logger = self._logger
        self.reset()
        return logger

    def set_level(self, level: str = "info") -> None:
        self._level = level
        return self

    def set_operations_logfile(self, logfile: str = "logs/operations.log") -> None:
        self._operations_logfile = logfile
        return self

    def set_events_logfile(self, logfile: str = "logs/events.log") -> None:
        self._events_logfile = logfile
        return self

    def build_console_handler(self):
        """Formats the handler and sets logger"""
        # Obtain handller
        handler = logging.StreamHandler()
        # Configure formatter
        format = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
        # Set format on handler
        handler.setFormatter(format)
        return handler

    def build_file_handler(self, logfile: str):
        """Formats the operations file handler and sets logger"""
        os.makedirs(os.path.dirname(logfile), exist_ok=True)
        # Time rotating file handler is preferred
        handler = TimedRotatingFileHandler(filename=self._operations_logfile)
        # Configure formatter for files to include time
        format = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
        # Set format on handler
        handler.setFormatter(format)
        return handler

    def build(self, name: str):
        # Instantiate logger with 'info' level. User can override
        self._logger = logging.getLogger(name)
        self._logger.setLevel(LoggerBuilder.__level.get(self._level, logging.INFO))
        self._logger.addHandler(self.build_console_handler())
        self._logger.addHandler(self.build_file_handler(self._operations_logfile))
        self._logger.addHandler(self.build_file_handler(self._events_logfile))
        return self
