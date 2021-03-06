#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /load.py                                                                              #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 12th 2022, 5:34:59 am                                                 #
# Modified : Thursday, March 31st 2022, 3:38:39 pm                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Tasks that complete the Load phase of the ETL DAG"""
import pandas as pd
import sqlalchemy
from pymysql import connect
from pymysql.cursors import DictCursor
from typing import Any

from deepcvr.base.operator import Operator
from deepcvr.utils.decorators import operator


# ================================================================================================ #
#                                DATABASE DEFINITION LANGUAGE                                      #
# ================================================================================================ #
"""Defines the DDL for the ETL process"""
DDL = {}
# ------------------------------------------------------------------------------------------------ #
#                             FOREIGN KEY CHECKS OFF                                               #
# ------------------------------------------------------------------------------------------------ #
DDL["foreign_key_checks_off"] = """SET FOREIGN_KEY_CHECKS = 0;"""
DDL["foreign_key_checks_on"] = """SET FOREIGN_KEY_CHECKS = 1;"""
# ------------------------------------------------------------------------------------------------ #
#                                    DROP TABLES                                                   #
# ------------------------------------------------------------------------------------------------ #
DDL["drop_tables"] = """DROP TABLE IF EXISTS common_features, features, cvr, cvr_schema;"""
# ------------------------------------------------------------------------------------------------ #
#                                  DROP DATABASES                                                  #
# ------------------------------------------------------------------------------------------------ #
DDL["drop_development_train_db"] = """DROP DATABASE IF EXISTS deepcvr_development_train;"""
DDL["drop_development_test_db"] = """DROP DATABASE IF EXISTS deepcvr_development_test;"""
DDL["drop_production_train_db"] = """DROP DATABASE IF EXISTS deepcvr_train;"""
DDL["drop_production_test_db"] = """DROP DATABASE IF EXISTS deepcvr_test;"""

# ------------------------------------------------------------------------------------------------ #
#                                 CREATE DATABASES                                                 #
# ------------------------------------------------------------------------------------------------ #
DDL["create_development_train_db"] = """CREATE DATABASE deepcvr_development_train;"""
DDL["create_development_test_db"] = """CREATE DATABASE deepcvr_development_test;"""
DDL["create_production_train_db"] = """CREATE DATABASE deepcvr_train;"""
DDL["create_production_test_db"] = """CREATE DATABASE deepcvr_test;"""

# ------------------------------------------------------------------------------------------------ #
#                                  CREATE TABLES                                                   #
# ------------------------------------------------------------------------------------------------ #
# Schema table
DDL[
    "create_schema_table"
] = """
CREATE TABLE cvr_schema (
    feature_name VARCHAR(32) NOT NULL PRIMARY KEY,
    feature_category VARCHAR(64) NOT NULL,
    feature_description VARCHAR(256) NOT NULL
) ENGINE=INNODB;
"""
# cvr table
DDL[
    "create_cvr_table"
] = """
CREATE TABLE cvr (
    sample_id BIGINT(20) NOT NULL PRIMARY KEY,
    label VARCHAR(12) NOT NULL,
    click_label BIGINT(8) NOT NULL,
    conversion_label BIGINT(8) NOT NULL,
    common_features_index VARCHAR(64) NOT NULL,
    num_features BIGINT(8) NOT NULL,

    INDEX cfi (common_features_index),
    INDEX label_ind (label)
) ENGINE=INNODB;
"""
# features table
DDL[
    "create_features_table"
] = """
CREATE TABLE features (
    id BIGINT(12) AUTO_INCREMENT PRIMARY KEY,
    sample_id BIGINT(20) NOT NULL,
    feature_name VARCHAR(32) NOT NULL,
    feature_id BIGINT(8) NOT NULL,
    feature_value DOUBLE(8,2) NOT NULL,

    INDEX (sample_id),
    INDEX (feature_name),

    FOREIGN KEY (sample_id)
        REFERENCES cvr(sample_id)
        ON UPDATE CASCADE ON DELETE CASCADE,

    FOREIGN KEY (feature_name)
        REFERENCES cvr_schema(feature_name)


) ENGINE=INNODB;
"""

# common_features table
DDL[
    "create_common_features_table"
] = """
CREATE TABLE common_features (
    id BIGINT(12) AUTO_INCREMENT PRIMARY KEY,
    common_features_index VARCHAR(64) NOT NULL,
    feature_name VARCHAR(32) NOT NULL,
    feature_id BIGINT(8) NOT NULL,
    feature_value DOUBLE(8,2) NOT NULL,

    INDEX (common_features_index),
    INDEX (feature_name),

    FOREIGN KEY (common_features_index)
        REFERENCES cvr(common_features_index)
        ON UPDATE CASCADE
        ON DELETE CASCADE,

    FOREIGN KEY (feature_name)
        REFERENCES cvr_schema (feature_name)

) ENGINE=INNODB;
"""


# ------------------------------------------------------------------------------------------------ #
#                                  DATABASE DEFINITION CLASS                                       #
# ------------------------------------------------------------------------------------------------ #


class DbDefine(Operator):
    """Used to create, drop, constrain, adn define databases and tables.

    Args:
        task_id: Sequence number for the task in its dag
        task_name: Human readable name
        params: The parameters the task requires

    """

    def __init__(self, task_id: int, task_name: str, params: dict) -> None:
        super(DbDefine, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> None:

        # Obtain credentials for mysql database from context
        credentials = context["john"]

        for database, statements in self._params.items():
            connection = connect(
                host=credentials["host"],
                user=credentials["user"],
                password=credentials["password"],
                database=database,
                charset="utf8",
                cursorclass=DictCursor,
            )

            with connection.cursor() as cursor:
                for statement in statements:
                    cursor.execute(DDL[statement])
                connection.commit()

            connection.close()


# ------------------------------------------------------------------------------------------------ #
#                                     DATABASE LOADER                                              #
# ------------------------------------------------------------------------------------------------ #


class DataLoader(Operator):
    """Loads data into tables."""

    def __init__(self, task_id: int, task_name: str, params: dict) -> None:
        super(DataLoader, self).__init__(task_id=task_id, task_name=task_name, params=params)

    @operator
    def execute(self, data: pd.DataFrame = None, context: Any = None) -> None:

        engine = sqlalchemy.create_engine(
            context["database_uri"][self._params["connection_string"]]
        )

        # Identify columns and convert strings to sqlalchemy datatypes
        columns = []
        dtypes = self._params["dtypes"]
        for column, dtype in self._params["dtypes"].items():
            dtypes[column] = eval(dtype)
            columns.append(column)

        # Extract columns of interest
        data = data[columns]

        data.to_sql(
            name=self._params["table"], con=engine, index=False, if_exists="append", dtype=dtypes,
        )
