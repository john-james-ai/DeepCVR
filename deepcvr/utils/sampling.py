#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /sampling.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, March 1st 2022, 11:35:15 am                                                  #
# Modified : Tuesday, March 1st 2022, 11:45:01 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Sampling utilities"""
#%%
import os
import pandas as pd

# ------------------------------------------------------------------------------------------------ #


def create_basic_sample(infile, outfile, n) -> None:
    """Selects first n rows from infile and stores thows rows in outfile"""
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    df = pd.read_csv(infile, nrows=n, header=None, index_col=None)
    df.to_csv(outfile, index=False, header=False)
    assert os.path.exists(outfile)


# ------------------------------------------------------------------------------------------------ #
if __name__ == "__main__":
    infile_core = "data/raw/sample_skeleton_test.csv"
    infile_common = "data/raw/common_features_test.csv"
    outfile_core = "data/development/sample_skeleton_test.csv"
    outfile_common = "data/development/common_features_test.csv"
    n = 100
    create_basic_sample(infile_core, outfile_core, n)
    create_basic_sample(infile_common, outfile_common, n)

#%%
