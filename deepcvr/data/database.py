#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /databbase.py                                                                         #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 1:28:55 am                                              #
# Modified : Sunday, March 13th 2022, 3:54:28 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import logging
from sqlalchemy import create_engine
import pandas as pd
from pymysql import connect
from pymysql.cursors import DictCursor

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Database:
    """Provides database creation and management services

    Args:
        connection_string (str): Database URL string for the database.
    """

    def __init__(self, database: str, credentials: dict) -> None:
        self._connection = connect(
            host=credentials["host"],
            user=credentials["user"],
            password=credentials["password"],
            database=database,
            charset="utf8mb4",
            cursorclass=DictCursor,
        )

    def __enter__(self) -> None:
        return self

    def __exit__(self) -> None:
        self._connection.close()

    @property
    def connection(self) -> connect:
        return self._connection

    def commit(self) -> None:
        self._connection.commit()

    def close(self, commit: bool = True) -> None:
        if commit:
            self.commit()
        self._connection.close()

    def _execute(self, statement: str, params: tuple = (), commit: bool = True) -> connect().cursor:
        with self._connection as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, params)
                if commit:
                    connection.commit()
                return cursor

    def execute(self, statement: str, params: tuple = (), commit: bool = True) -> int:
        return self._execute(statement, params, commit)


class DAO:
    """Data access object."""

    def __init__(self, connection_string: str) -> None:
        self._engine = create_engine(connection_string)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._engine.dispose()
        return True

    def select(self, statement: str, params: tuple = ()) -> pd.DataFrame:
        with self._engine.connect() as connection:
            return pd.read_sql_query(sql=statement, con=connection, params=params)

    def selectall(self, table_name: str) -> pd.DataFrame:
        with self._engine.connect() as connection:
            return pd.read_sql_table(table_name=table_name, con=connection)

    def insert(self, table_name, data: pd.DataFrame, dtype: dict = {}) -> int:
        with self._engine.connect() as connection:
            return data.to_sql(name=table_name, con=connection, dtype=dtype)
