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
# Modified : Saturday, February 26th 2022, 8:19:06 am                                              #
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
class CreateDatabase(Query):
    name: str = "create_deepcvr_database"
    desc: str = "Create deepcvr database"
    sql: str = """CREATE DATABASE IF NOT EXISTS deepcvr;"""


@dataclass
class CreateImpressionsTable(Query):
    name: str = "create_impressions_table"
    desc: str = "Create impressions table"
    sql: str = """
CREATE TABLE IF NOT EXISTS impressions (
  sample_id BIGINT PRIMARY KEY,
  click_label BIGINT,
  conversion_label BIGINT,
) ENGINE=INNODB;
 """


@dataclass
class CreateFeaturesTable(Query):
    name: str = "create_features_table"
    desc: str = "Create features table"
    sql: str = """
CREATE TABLE IF NOT EXISTS features (
  sample_id BIGINT PRIMARY KEY,
  feature_field_id VARCHAR(40),
  feature_id BIGINT,
  feature_value DOUBLE
  FOREIGN KEY (sample_id)
    REFERENCES impressions(sample_id)
    ON DELETE CASCADE
) ENGINE=INNODB;
 """
