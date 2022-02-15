#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /extractor.py                                                                         #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, February 14th 2022, 12:32:13 pm                                               #
# Modified : Monday, February 14th 2022, 7:20:32 pm                                                #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
import tarfile
import tempfile
import logging
import inspect
import shutil
from airflow.models.baseoperator import BaseOperator

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ExtractOperator(BaseOperator):
    """Decompresses a gzip archive, stores the raw data and pushes the filepaths to xCom

    Args:
        source (str): The source filepath
        destination (str): The destination directory
        kwargs (dict): Default parameters
    """

    def __init__(self, source: str, destination: str, **kwargs) -> None:
        super(ExtractOperator, self).__init__(**kwargs)
        self._source = source
        self._destination = destination

    def execute(self, context) -> None:
        """Extracts and stores the data, then pushes filepaths to xCom. """
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))
        logger.debug("\tSource: {}\t\nDestination: {}".format(self._source, self._destination))

        if not self._exists():
            # Recursively extract data and store in destination directory
            self._extract(self._source)

        # Extract filepaths for all data downloaded and extracted
        filepaths = self._get_filepaths()

        # Push the extracted filepaths to xCom
        ti = context["ti"]
        ti.xcom_push("filepaths", filepaths)

        logger.debug(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )

    def _exists(self) -> bool:
        """Checks destination directory and returns True if not empty. False otherwise."""

        return len(os.listdir(self._destination)) > 0

    def _extract(self, filepath: str) -> None:
        """Extracts the data and returns the extracted filepaths"""

        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        if tarfile.is_tarfile(filepath):
            logger.debug("\tExtracting: {}".format(filepath))
            with tempfile.TemporaryDirectory() as tempdirname:
                data = tarfile.open(filepath)
                for member in data.getmembers():
                    # If the file already exists, skip this step
                    filepath = os.path.join(tempdirname, member.name)
                    data.extract(member, tempdirname)
                    return self._extract(filepath)
        else:
            self._savefile(filepath)

        logger.debug(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )

    def _savefile(self, filepath: str) -> None:
        """Saves file to destination and adds name and filepath to filepaths dictionary

        Args:
            filepath (str): the path to the extracted file in the temp directory
        """
        logger.debug("\tStarted {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

        # Create destination filepath and move the file
        destination = os.path.join(self._destination, os.path.basename(filepath))
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.move(filepath, destination)

        logger.debug("\tSaved Source: {}\n\tDestination: {}".format(filepath, destination))

        logger.debug(
            "\tSuccessfully completed {} {}".format(self.__class__.__name__, inspect.stack()[0][3])
        )

    def _get_filepaths(self) -> dict:
        """Creates a dictionary of destination file paths."""
        filepaths = {}

        filenames = os.listdir(self._destination)
        if len(filenames) > 0:
            for filename in filenames:
                filepath = os.path.join(self._destination, filename)
                name = os.path.splitext(filename)[0]
                filepaths[name] = filepath
        else:
            msg = "Destination directory is empty"
            raise FileNotFoundError(msg)

        return filepaths
