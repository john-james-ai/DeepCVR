#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /extract.py                                                                           #
# Language : Python 3.10.2                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Tuesday, February 15th 2022, 9:32:40 am                                               #
# Modified : Wednesday, February 16th 2022, 7:14:29 am                                             #
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
import shutil

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------ #


class Extractor:
    """Decompresses a gzip archive, stores the raw data and pushes the filepaths to xCom

    Args:
        source (str): The source filepath
        destination (str): The destination directory
    """

    def __init__(self) -> None:

        self._source = None
        self._destination = None

    def execute(self, source: str, destination: str) -> None:
        """Extracts and stores the data, then pushes filepaths to xCom. """

        self._source = source
        self._destination = destination

        if not self._exists():
            # Recursively extract data and store in destination directory
            self._extract(self._source)

        # Extract filepaths for all data downloaded and extracted
        filepaths = self._get_filepaths()

        return filepaths

    def _exists(self) -> bool:
        """Checks destination directory and returns True if not empty. False otherwise."""

        return len(os.listdir(self._destination)) > 0

    def _extract(self, filepath: str) -> None:
        """Extracts the data and returns the extracted filepaths"""

        if tarfile.is_tarfile(filepath):
            with tempfile.TemporaryDirectory() as tempdirname:
                data = tarfile.open(filepath)
                for member in data.getmembers():
                    # If the file already exists, skip this step
                    filepath = os.path.join(tempdirname, member.name)
                    data.extract(member, tempdirname)
                    return self._extract(filepath)
        else:
            self._savefile(filepath)

    def _savefile(self, filepath: str) -> None:
        """Saves file to destination and adds name and filepath to filepaths dictionary

        Args:
            filepath (str): the path to the extracted file in the temp directory
        """

        # Create destination filepath and move the file
        destination = os.path.join(self._destination, os.path.basename(filepath))
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        shutil.move(filepath, destination)

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
