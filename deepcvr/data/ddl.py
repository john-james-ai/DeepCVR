#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /ddl.py                                                                               #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 12th 2022, 6:33:27 am                                                 #
# Modified : Sunday, March 13th 2022, 5:47:35 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
"""Defines the DDL for the ETL process"""
DDL = {}

DDL["foreign_key_checks_off"] = """SET FOREIGN_KEY_CHECKS = 0;"""
DDL["foreign_key_checks_on"] = """SET FOREIGN_KEY_CHECKS = 1;"""

# Drop Foreign Key Constraints
DDL[
    "drop_constraint_features"
] = """ALTER TABLE features DROP FOREIGN KEY FK_tbl_features_ibfk_1;"""
DDL[
    "drop_constraint_common_features"
] = """ALTER TABLE common_features DROP FOREIGN KEY FK_tbl_common_features_ibfk_1;"""

# Drop Databases
DDL["drop_development_train_db"] = """DROP DATABASE IF EXISTS development_train;"""
DDL["drop_development_test_db"] = """DROP DATABASE IF EXISTS development_test;"""
DDL["drop_production_train_db"] = """DROP DATABASE IF EXISTS production_train;"""
DDL["drop_production_test_db"] = """DROP DATABASE IF EXISTS production_test;"""

# Create Databases
DDL["create_development_train_db"] = """CREATE DATABASE IF NOT EXISTS development_train;"""
DDL["create_development_test_db"] = """CREATE DATABASE IF NOT EXISTS development_test;"""
DDL["create_production_train_db"] = """CREATE DATABASE IF NOT EXISTS production_train;"""
DDL["create_production_test_db"] = """CREATE DATABASE IF NOT EXISTS production_test;"""


# Create Tables
DDL[
    "create_impressions_table"
] = """
CREATE TABLE IF NOT EXISTS impressions (
    sample_id BIGINT NOT NULL UNIQUE,
    click_label BIGINT NOT NULL,
    conversion_label BIGINT NOT NULL,
    common_features_index VARCHAR(64) NOT NULL,
    num_features BIGINT NOT NULL,
    PRIMARY KEY (sample_id)
) ENGINE=INNODB;
"""

DDL[
    "create_features_table"
] = """
CREATE TABLE IF NOT EXISTS features (
    id BIGINT NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    sample_id BIGINT NOT NULL,
    feature_name VARCHAR(64) NOT NULL,
    feature_id BIGINT NOT NULL,
    feature_value DOUBLE NOT NULL,
    INDEX sid (sample_id)
) ENGINE=INNODB;
"""

DDL[
    "create_common_feature_groups_table"
] = """
CREATE TABLE IF NOT EXISTS common_feature_groups (
    common_features_index VARCHAR(64) UNIQUE NOT NULL,
    num_features BIGINT NOT NULL,
    PRIMARY KEY (common_features_index)
) ENGINE=INNODB;
"""

DDL[
    "create_common_features_table"
] = """
CREATE TABLE IF NOT EXISTS common_features (
    id BIGINT NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY,
    common_features_index VARCHAR(64) NOT NULL,
    feature_name VARCHAR(64) NOT NULL,
    feature_id BIGINT NOT NULL,
    feature_value DOUBLE NOT NULL,
    INDEX cfi (common_features_index)
) ENGINE=INNODB;
"""

DDL[
    "add_foreign_key_constraint_1"
] = """
ALTER TABLE features ADD CONSTRAINT fk_sample_id FOREIGN KEY(sample_id)
REFERENCES impressions(sample_id);
"""

DDL[
    "add_foreign_key_constraint_2"
] = """
ALTER TABLE common_features ADD CONSTRAINT fk_common_features_index
FOREIGN KEY(common_features_index)
REFERENCES common_feature_groups(common_features_index);
"""

"""Defines the METADATA_DDL for  Metadata"""

METABASE_DDL = {}

# Drop Databases
METABASE_DDL["drop_metabase_db"] = """DROP DATABASE IF EXISTS metabase;"""

# Create Databases
METABASE_DDL["create_metabase_db"] = """CREATE DATABASE IF NOT EXISTS metabase;"""

# Create Tables
METABASE_DDL[
    "create_asset_tbl"
] = """
CREATE TABLE IF NOT EXISTS asset (
    id INT NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(16) NOT NULL,
    desc VARCHAR(64),
    uri VARCHAR(128),
    created DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
) ENGINE=INNODB;
"""

METABASE_DDL[
    "create_event_table"
] = """
CREATE TABLE IF NOT EXISTS event (
    id INT NOT NULL UNIQUE AUTO_INCREMENT PRIMARY KEY,
    module: VARCHAR(64),
    classname: VARCHAR(64),
    method: VARCHAR(64),
    action: VARCHAR(64),
    param1: VARCHAR(64),
    param2: VARCHAR(64),
    param3: VARCHAR(64),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
) ENGINE=INNODB;
"""

