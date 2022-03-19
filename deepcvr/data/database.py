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
# Modified : Friday, March 18th 2022, 7:50:57 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
from sqlalchemy import create_engine
import pandas as pd

# ------------------------------------------------------------------------------------------------ #


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
