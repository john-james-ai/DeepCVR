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
# Modified : Thursday, March 31st 2022, 6:17:40 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
from abc import ABC, abstractmethod
import os
from dask.diagnostics import ProgressBar
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
class ParquetIO(IO):
    """Processes IO to and from Parquet files"""

    def load(
        self,
        filepath: str,
        columns: list = None,
        engine: str = "auto",
        index: bool = False,
        partition_cols: list = None,
        **kwargs
    ) -> pd.DataFrame:

        with ProgressBar():
            df = pd.read_parquet(filepath, engine=engine, columns=columns)
        return df

    def save(
        self,
        data: pd.DataFrame,
        filepath: str,
        engine: str = "auto",
        index: bool = False,
        partition_cols: list = None,
        **kwargs
    ) -> None:

        with ProgressBar():
            data.to_parquet(filepath, engine=engine, partition_cols=partition_cols)


# ------------------------------------------------------------------------------------------------ #
class CsvIO(IO):
    """Handles IO of pandas DataFrames to /from CSV Files """

    def load(
        self,
        filepath: str,
        sep: str = ",",
        header: list = None,
        names: list = None,
        usecols: list = None,
        index_col: bool = False,
        dtype: dict = None,
        n_chunks: int = 20,
        progress_bar: bool = True,
    ) -> pd.DataFrame:
        """Reads a CSV file into pandas DataFrame with progress monitor."""

        if progress_bar:
            return self._load_progress_bar(
                filepath=filepath,
                sep=sep,
                header=header,
                names=names,
                usecols=usecols,
                index_col=index_col,
                dtype=dtype,
                n_chunks=n_chunks,
            )
        else:
            return self._load_no_progress_bar(
                filepath=filepath,
                sep=sep,
                header=header,
                names=names,
                usecols=usecols,
                index_col=index_col,
                dtype=dtype,
                n_chunks=n_chunks,
            )

    def _load_progress_bar(
        self,
        filepath: str,
        sep: str = ",",
        header: list = None,
        names: list = None,
        usecols: list = None,
        index_col: bool = False,
        dtype: dict = None,
        n_chunks: int = 20,
    ) -> pd.DataFrame:

        rows = sum(1 for _ in open(filepath, "r"))

        chunksize = int(rows / n_chunks)

        chunks = []

        with tqdm(total=rows, desc="\tRows read: ") as bar:
            for chunk in pd.read_csv(
                filepath,
                sep=sep,
                header=header,
                names=names,
                usecols=usecols,
                index_col=index_col,
                dtype=dtype,
                low_memory=False,
                chunksize=chunksize,
            ):
                chunks.append(chunk)
                bar.update(len(chunk))

        df = pd.concat((f for f in chunks), axis=0)

        return df

    def _load_no_progress_bar(
        self,
        filepath: str,
        sep: str = ",",
        header: list = None,
        names: list = None,
        usecols: list = None,
        index_col: bool = False,
        dtype: dict = None,
        n_chunks: int = 20,
    ) -> pd.DataFrame:

        rows = sum(1 for _ in open(filepath, "r"))

        chunksize = int(rows / n_chunks)

        chunks = []

        for chunk in pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            names=names,
            usecols=usecols,
            index_col=index_col,
            dtype=dtype,
            low_memory=False,
            chunksize=chunksize,
        ):
            chunks.append(chunk)

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
        n_chunks: int = 20,
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
