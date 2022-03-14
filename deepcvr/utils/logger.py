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
# Modified : Sunday, March 13th 2022, 3:32:27 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import logging
from logging.handlers import TimedRotatingFileHandler

# ------------------------------------------------------------------------------------------------ #
# Define the formatters
format1 = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
format2 = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
# Create default configuration with file logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d-%Y %H:%M",
    filename="logs/metevent.log",
    filemode="w",
)
# set a format which is simpler for console use

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler()
console.setFormatter(format1)
console.setLevel(logging.INFO)
# Add the console and file handler to the module logger
filehandler = TimedRotatingFileHandler(filename="logs/deepcvr.log")
filehandler.setFormatter(format2)
filehandler.setLevel(logging.DEBUG)
# Add both handlers to module logger
logging.getLogger(__name__).addHandler(filehandler)
# Speak
logger = logging.getLogger(__name__)
logger.debug("I'm speaking")


def func():
    logger.info("Uh oh")


func()
