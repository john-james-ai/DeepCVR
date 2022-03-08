#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /load_database.py                                                                     #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 9:14:54 pm                                              #
# Modified : Sunday, February 27th 2022, 4:20:28 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from airflow.models.baseoperator import BaseOperator
from sqlalchemy import create_engine

from deepcvr.utils.config import MySQLConfig
from deepcvr.data import COLS_IMPRESSIONS_TBL
from deepcvr.data import COLS_CORE_DATASET
from deepcvr.utils.io import load_csv

# ------------------------------------------------------------------------------------------------ #


class LoadImpressionsOperator(BaseOperator):
    """Loads the impressions table"""

    def __init__(self, credentials: MySQLConfig, filepath: str, **kwargs) -> None:
        super(LoadImpressionsOperator, self).__init__(**kwargs)
        self._credentials = credentials
        self._filepath = filepath

    def execute(self, context=None) -> None:
        engine = create_engine(
            "mysql+pymysql://{user}:{pwd}@{host}/{db}".format(
                host=self._credentials.host,
                db=self._credentials.dbname,
                user=self._credentials.user,
                pwd=self._credentials.password,
            )
        )

        # Obtain core data
        df = load_csv(filepath=self._filepath, sep=",", header=None, index_col=False)
        df.columns = COLS_CORE_DATASET

        # Set a chunksize
        chunksize = int(df.shape[0] / 20)

        # Create impressions dataframe
        df_impressions = df[COLS_IMPRESSIONS_TBL]
        # Save DataFrame to table
        df_impressions.to_sql(
            name="impressions",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=chunksize,
            method=None,
        )
