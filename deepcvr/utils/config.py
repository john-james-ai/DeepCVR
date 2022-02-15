#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /config.py                                                                            #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, February 14th 2022, 1:25:40 pm                                                #
# Modified : Monday, February 14th 2022, 5:52:27 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from abc import ABC
import os
import yaml


# ---------------------------------------------------------------------------- #


class Config(ABC):
    """Abstract base class for Config classes."""

    def load_config(self, filepath: str) -> dict:

        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                return yaml.full_load(f)
        else:
            return {}

    def save_config(self, config: dict, filepath: str) -> None:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as f:
            yaml.dump(config, f)


# ---------------------------------------------------------------------------- #


class Taobao(Config):
    """Encapsulates the Taobao Labs data source configuration.

    Args:
        config_filepath (str): A string containing the path
        to the YAML configuration file.

    """

    __filepath = "config/data.yaml"

    def __init__(self) -> None:
        super(Taobao, self).__init__()
        self._config = self.load_config(Taobao.__filepath)

    def get_train_filepath(self, stage: str = None) -> str:
        if stage == "raw":
            return self._config["raw_train"]
        else:
            return self._config["train"]

    def get_test_filepath(self, stage: str = None) -> str:
        if stage == "raw":
            return self._config["raw_test"]
        else:
            return self._config["test"]

    @property
    def destination(self) -> str:
        return self._config["destination"]
