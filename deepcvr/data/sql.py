#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /create_db.py                                                                         #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 5:50:10 am                                              #
# Modified : Saturday, February 26th 2022, 9:07:47 pm                                              #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC

# ------------------------------------------------------------------------------------------------ #


@dataclass
class Query(ABC):
    name: str = ""
    desc: str = ""
    sql: str = """;"""
    data: tuple = ()


@dataclass
class CreateDatabaseSQL(Query):
    name: str = "create_deepcvr_database"
    desc: str = "Create deepcvr database"
    sql: str = """CREATE DATABASE IF NOT EXISTS deepcvr;"""


@dataclass
class CreateImpressionsTableSQL(Query):
    name: str = "create_impressions_table"
    desc: str = "Create impressions table"
    sql: str = """
CREATE TABLE IF NOT EXISTS impressions (
  sample_id BIGINT PRIMARY KEY,
  click_label BIGINT,
  conversion_label BIGINT,
  num_core_features BIGINT,
  common_features_index CHAR(16),
  FOREIGN KEY (common_features_index)
    REFERENCES common_features(common_features_index)
) ENGINE=INNODB;
 """


@dataclass
class CreateCoreFeaturesTableSQL(Query):
    name: str = "create_core_features_table"
    desc: str = "Create core features table"
    sql: str = """
CREATE TABLE IF NOT EXISTS core_features (
  sample_id BIGINT PRIMARY KEY,
  feature_name VARCHAR(40),
  feature_id BIGINT,
  feature_value DOUBLE
  FOREIGN KEY (sample_id)
    REFERENCES impressions(sample_id)
    ON DELETE CASCADE
) ENGINE=INNODB;
 """


@dataclass
class CreateCommonFeaturesTableSQL(Query):
    name: str = "create_common_features_table"
    desc: str = "Create common features table"
    sql: str = """
CREATE TABLE IF NOT EXISTS common_features (
  common_features_index CHAR(16) PRIMARY KEY,
  feature_name VARCHAR(40),
  feature_id BIGINT,
  feature_value DOUBLE
  FOREIGN KEY (common_features_index)
    REFERENCES impressions(common_features_index)
    ON DELETE CASCADE
) ENGINE=INNODB;
 """


@dataclass
class DatabaseExistsSQL(Query):
    name: str = "deepcvr_database_exists"
    desc: str = "Deepcvr Database Exists"
    sql: str = (
        """SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = deepcvr";"""
    )
