#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /io.py                                                                                #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, February 26th 2022, 6:41:17 pm                                              #
# Modified : Wednesday, March 9th 2022, 12:44:02 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from abc import ABC, abstractmethod
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

# ------------------------------------------------------------------------------------------------ #


class IO(ABC):
    """Base class for IO classes"""

    @abstractmethod
    def load(self, filepath: str, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def save(self, data: pd.DataFrame, filepath: str, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
class CsvIO(IO):
    """Handles IO of pandas DataFrames to /from CSV Files """

    def load(
        self,
        filepath: str,
        sep: str = ",",
        header: list = "infer",
        names: list = None,
        usecols: list = None,
        index_col=False,
        n_chunks=20,
    ) -> pd.DataFrame:
        """Reads a CSV file into pandas DataFrame with progress monitor."""

        rows = sum(1 for _ in open(filepath, "r"))

        chunksize = int(rows / n_chunks)

        chunks = []

        with tqdm(total=rows, desc="Rows read: ") as bar:
            for chunk in pd.read_csv(
                filepath,
                sep=sep,
                header=header,
                names=names,
                usecols=usecols,
                index_col=index_col,
                low_memory=False,
                chunksize=chunksize,
            ):
                chunks.append(chunk)
                bar.update(len(chunk))

        df = pd.concat((f for f in chunks), axis=0)

        return df

    def save(
        self,
        data: pd.DataFrame,
        filepath: str,
        index_label: str = None,
        sep: str = ",",
        header: bool = True,
        index: bool = False,
        n_chunks=20,
    ) -> None:
        """Writes a large DataFrame to CSV file with progress monitor."""

        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        chunks = np.array_split(data.index, n_chunks)

        for chunk, subset in enumerate(tqdm(chunks)):
            if chunk == 0:  # Write in 'w' mode
                data.loc[subset].to_csv(
                    filepath,
                    sep=sep,
                    header=header,
                    index_label=index_label,
                    mode="w",
                    index=index,
                )
            else:
                data.loc[subset].to_csv(
                    filepath, sep=sep, index_label=index_label, header=None, mode="a", index=index,
                )
