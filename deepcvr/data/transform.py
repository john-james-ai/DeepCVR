#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /transform.py                                                                         #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Sunday, February 27th 2022, 10:11:02 am                                               #
# Modified : Sunday, February 27th 2022, 5:17:18 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Transforms ALI-CCP impression and feature data into 3rd Normal Form prior to loading."""
from abc import ABC, abstractmethod
import logging
import os
import re
import pandas as pd
from typing import Union
from deepcvr.utils.io import load_csv, save_csv
from deepcvr.data import (
    COLS_CORE_DATASET,
    COLS_IMPRESSIONS_TBL,
    # COLS_COMMON_FEATURES_TBL,
    # COLS_COMMON_FEATURES_DATASET,
)

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Transformer(ABC):
    """Abstract base class for Transformer classes

    Args:
        source (str): Filepath to input file or input data.
        destination (str): Filepath to output file
        force (bool): Overwrites prior transformations if True
    """

    def __init__(self, source: str, destination: str, force: bool = False) -> None:
        self._source = source
        self._destination = destination
        self._force = force

    @abstractmethod
    def execute(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class Worker(ABC):
    """Abstract base class for 'Worker objects.

    In contrast to Transformers, Workers perform no IO, instead they take data as input and
    and return data from the execute method. Output data are also exposed as properties.

    Args:
        data (pd.DataFrame): Input data to be processed
    """

    def __init__(self, data: pd.DataFrame) -> None:
        self._data = data
        self._output = None

    @abstractmethod
    def execute(self) -> None:
        pass

    @property
    def output(self) -> pd.DataFrame:
        return self._output


# ------------------------------------------------------------------------------------------------ #


class Reducer(ABC):
    """Abstract base class for Reducer objects.

    Reducer objects combine results from various tasks into a single output

    Args:
        source (str): Directory in which the input data are stored
        destination (str): Filepath to which the output is stored.
    """

    def __init__(self, source: str, destination: str) -> None:
        self._source = source
        self._destination = destination

    @abstractmethod
    def execute(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class TransformerImpressions(Transformer):
    """Creates a transformed impressions file

    Args:
        source (str): Filepath to input file or input data.
        destination (str): Filepath to output file
        force (bool): Overwrites prior transformations if True
    """

    def __init__(self, source: str, destination: str, force: bool = False) -> None:
        super(TransformerImpressions, self).__init__(
            source=source, destination=destination, force=force
        )

    def execute(self) -> None:

        # Skip if already executed unless force is True
        if not os.path.exists(self._destination) or self._force:

            df_in = load_csv(filepath=self._source, chunksize=100000, names=COLS_CORE_DATASET)

            logger.debug("Transforming impressions shape is {}".format(str(df_in.shape)))

            df_out = df_in[COLS_IMPRESSIONS_TBL]

            chunksize = int(df_out.shape[0] / 20)

            logger.debug("Saving impressions. Chunksize: {}".format(str(chunksize)))

            save_csv(
                data=df_out,
                filepath=self._destination,
                header=False,
                index=False,
                chunksize=chunksize,
            )


# ------------------------------------------------------------------------------------------------ #


class WorkerCoreFeatures(Worker):
    """Transforms a chunk of ALI-CCP Core data into a Core Features DataFrame

    Args:
        data (pd.DataFrame): Input data to be processed
    """

    def __init__(self, data: pd.DataFrame) -> None:
        super(WorkerCoreFeatures, self).__init__(data=data)

    def execute(self) -> pd.DataFrame:
        self._output = pd.DataFrame()

        for _, row in self._data.iterrows():
            sample_id = int(row[0].values[0])
            num_features = int(row[4].values[0])
            feature_string = row[5].values[0]

            df = parse_feature_string(
                id_name="sample_id",
                id_value=sample_id,
                num_features=num_features,
                feature_string=feature_string,
            )

            self._output = pd.concat([self._output, df], axis=0)
        return self._output


# ------------------------------------------------------------------------------------------------ #


class WorkerCommonFeatures(Worker):
    """Transforms a chunk of ALI-CCP commmon features data into a common features dataFrame

    Args:
        data (pd.DataFrame): Input data to be processed
    """

    def __init__(self, data: pd.DataFrame) -> None:
        super(WorkerCommonFeatures, self).__init__(data=data)

    def execute(self) -> pd.DataFrame:
        self._output = pd.DataFrame()

        for _, row in self._data.iterrows():
            common_features_index = row[0].values[0]
            num_features = int(row[1].values[0])
            feature_string = row[2].values[0]

            df = parse_feature_string(
                id_name="common_features_index",
                id_value=common_features_index,
                num_features=num_features,
                feature_string=feature_string,
            )

            self._output = pd.concat([self._output, df], axis=0)
        return self._output


# ------------------------------------------------------------------------------------------------ #
def parse_feature_string(
    id_name: str, id_value: Union[str, int], num_features: int, feature_string: str
) -> dict:
    """Parses a feature string from the ALI-CCP File into DataFrame of feature values.

    Feature strings contain one or more feature structures, each deliminated by ASCII character
    '0x01'. Each feature structure contains three components,
     - feature_name (string),
     - feature_id (int), and
     - feature_value (float)

    This function parses the feature string and its feature structures into three column DataFrame
    comprised of one row per feature structure.

    Args:
        id_name (str): The column name for the id
        id_value (str,int): The column value for the id
        num_features (int): The number of feature structures in the list
        feature_string (str): String containing feature structures

    """
    feature_names = []
    feature_ids = []
    feature_values = []

    # Expand into a list of feature structures
    feature_structures = re.split("\x01", feature_string)

    for structure in feature_structures:
        name, id, value = re.split("\x02|\x03", structure)
        feature_names.append(name)
        feature_ids.append(int(id))
        feature_values.append(float(value))

    d = {
        id_name: id_value,
        "feature_name": feature_names,
        "feature_id": feature_ids,
        "feature_value": feature_values,
    }
    df = pd.DataFrame(data=d)

    # Confirms number of rows equals expected number of features.
    assert df.shape[0] == num_features, logger.error(
        "Number of features mismatch index {}. Expected: {}. Actual: {}.".format(
            id_value, num_features, df.shape[0]
        )
    )
    return df
