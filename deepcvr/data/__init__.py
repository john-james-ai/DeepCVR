#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /__init__.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, February 22nd 2022, 5:37:23 am                                               #
# Modified : Friday, March 18th 2022, 10:42:17 am                                                  #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Contants related to the data representation."""
import sqlalchemy

# ------------------------------------------------------------------------------------------------ #
# Database table columns
COLS_IMPRESSIONS_TBL = [
    "sample_id",
    "click_label",
    "conversion_label",
    "num_features",
    "common_features_index",
]
COLS_CORE_FEATURES_TBL = ["sample_id", "feature_name", "feature_id", "feature_value"]
COLS_COMMON_FEATURES_TBL = ["common_features_index", "feature_name", "feature_id", "feature_value"]

# Dataframe columns
COLS_CORE_DATASET = [
    "sample_id",
    "click_label",
    "conversion_label",
    "common_features_index",
    "num_features",
    "features_list",
]
COLS_COMMON_FEATURES_DATASET = [
    "common_features_index",
    "num_features",
    "features_list",
]
# ------------------------------------------------------------------------------------------------ #
#                                         DATA TYPES                                               #
# ------------------------------------------------------------------------------------------------ #
# Sqlalchemy Data Types
DTYPES_SA = {
    "impressions": {
        "sample_id": sqlalchemy.types.BigInteger(),
        "click_label": sqlalchemy.types.BigInteger(),
        "conversion_label": sqlalchemy.types.BigInteger(),
        "common_features_index": sqlalchemy.types.VARCHAR(32),
        "num_features": sqlalchemy.types.BigInteger(),
    },
    "features": {
        "id": sqlalchemy.types.BigInteger(),
        "sample_id": sqlalchemy.types.BigInteger(),
        "feature_name": sqlalchemy.types.VARCHAR(64),
        "feature_id": sqlalchemy.types.BigInteger(),
        "feature_value": sqlalchemy.types.Float(precision=2, asdecimal=True),
    },
    "common_feature_groups": {
        "common_features_index": sqlalchemy.types.VARCHAR(32),
        "num_features": sqlalchemy.types.BigInteger(),
    },
    "common_features": {
        "id": sqlalchemy.types.BigInteger(),
        "common_features_index": sqlalchemy.types.VARCHAR(32),
        "feature_name": sqlalchemy.types.VARCHAR(64),
        "feature_id": sqlalchemy.types.BigInteger(),
        "feature_value": sqlalchemy.types.Float(precision=2, asdecimal=True),
    },
}

# Pandas Data Types
DTYPES_PD = {
    "core": {
        "sample_id": "Int64",
        "click_label": "Int64",
        "conversion_label": "Int64",
        "common_features_index": "object",
        "num_features": "Int64",
        "features_list": "object",
    },
    "common_features": {
        "common_features_index": "object",
        "num_features": "Int64",
        "features_list": "object",
    },
}
