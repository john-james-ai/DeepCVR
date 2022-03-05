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
# Modified : Friday, March 4th 2022, 5:10:05 pm                                                    #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Reading and writing dataframes with progress bars"""
import os
import math
import pandas as pd
import numpy as np
from tqdm import tqdm

# ------------------------------------------------------------------------------------------------ #


def load_csv(
    filepath: str,
    sep: str = ",",
    header: list = None,
    names: list = None,
    usecols: list = None,
    index_col=False,
    chunksize=None,
) -> pd.DataFrame:
    """Reads a large CSV file into pandas DataFrame with progress monitor."""

    rows = sum(1 for _ in open(filepath, "r"))

    chunksize = chunksize if chunksize else int(rows / 20)

    chunks = []

    with tqdm(total=rows, desc="Rows read: ") as bar:
        for chunk in pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            names=names,
            usecols=usecols,
            index_col=index_col,
            chunksize=chunksize,
        ):
            chunks.append(chunk)
            bar.update(len(chunk))

    df = pd.concat((f for f in chunks), axis=0)

    return df


# ------------------------------------------------------------------------------------------------ #
def save_csv(
    data: pd.DataFrame,
    filepath: str,
    index_label: str = None,
    sep: str = ",",
    header: bool = True,
    index: bool = False,
    chunksize=50000,
) -> pd.DataFrame:
    """Writes a large DataFrame to CSV file with progress monitor."""

    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    n_chunks = math.ceil(data.memory_usage(deep=True).sum() / chunksize)
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
                filepath,
                sep=sep,
                index_label=index_label,
                header=None,
                mode="a",
                index=index,
            )
