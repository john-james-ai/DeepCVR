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
# Modified : Saturday, March 5th 2022, 2:58:58 pm                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from abc import ABC
import os
import yaml
import yamlordereddictloader


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
def config_dag(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return yaml.load(f, Loader=yamlordereddictloader.Loader)
    else:
        return {}


# ---------------------------------------------------------------------------- #


class S3Config(Config):
    """Encapsulates the Amazon S3 Credentials and Configuration"""

    __filepath = "config/credentials.yaml"

    def __init__(self) -> None:
        super(S3Config, self).__init__()
        self._config = self.load_config(S3Config.__filepath)["amazon"]

    @property
    def bucket(self) -> str:
        return self._config["bucket"]

    @property
    def key(self) -> str:
        return self._config["aws_access_key_id"]

    @property
    def password(self) -> str:
        return self._config["aws_secret_access_key"]


# ---------------------------------------------------------------------------- #
class MySQLConfig(Config):
    """Encapsulates credentials for a MySQL user"""

    __filepath = "config/credentials.yaml"

    def __init__(self, user: str = "mysql") -> None:
        super(MySQLConfig, self).__init__()
        self._config = self.load_config(MySQLConfig.__filepath)[user]

    @property
    def host(self) -> str:
        return self._config["host"]

    @property
    def user(self) -> str:
        return self._config["user"]

    @property
    def password(self) -> str:
        return self._config["password"]

    @property
    def dbname(self) -> str:
        return self._config["dbname"]


# ---------------------------------------------------------------------------- #
class AirflowBackendConfig(Config):
    """Encapsulates the connection parameters for the Airflow backend database"""

    __filepath = "config/credentials.yaml"

    def __init__(self) -> None:
        super(AirflowBackendConfig, self).__init__()
        self._config = self.load_config(AirflowBackendConfig.__filepath)["airflow"]

    @property
    def user(self) -> str:
        return self._config["user"]

    @property
    def password(self) -> str:
        return self._config["password"]

    @property
    def host(self) -> str:
        return self._config["host"]

    @property
    def port(self) -> str:
        return self._config["port"]

    @property
    def dbname(self) -> str:
        return self._config["dbname"]

    @property
    def string(self) -> str:
        return self._config["string"]
