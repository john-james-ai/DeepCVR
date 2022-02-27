#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /extract.py                                                                           #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, February 25th 2022, 6:03:54 pm                                                #
# Modified : Saturday, February 26th 2022, 2:43:20 pm                                              #
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

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Extractor:
    """Decompresses a gzip archive, stores the raw data

    Args:
        source (str): The filepath to the source file to be decompressed
        destination (str): The destination directory into which data shall be stored.
        filetype (str): The file extension for the uncompressed data
        force (bool): Forces extraction even when files already exist.
    """

    def __init__(self, source: str, destination: str, force: bool = False) -> None:

        self._source = source
        self._destination = destination
        self._force = force

    def execute(self) -> None:
        """Extracts and stores the data, then pushes filepaths to xCom."""
        logger.debug("\tSource: {}\tDestination: {}".format(self._source, self._destination))

        # If all 4 raw files exist, it is assumed that the data have been downloaded
        n_files = len(os.listdir(self._destination))
        if n_files < 4:

            with tempfile.TemporaryDirectory() as tempdir:
                # Recursively extract data and store in destination directory
                self._extract(source=self._source, destination=tempdir)

    def _extract(self, source: str, destination: str) -> None:
        """Extracts the data and returns the extracted filepaths"""

        logger.debug("\t\tOpening {}".format(source))
        data = tarfile.open(source)

        for member in data.getmembers():
            if self._is_csvfile(filename=member.name):
                if self._not_exists_or_force(member_name=member.name):
                    logger.debug("\t\tExtracting {} to {}".format(member.name, self._destination))
                    data.extract(member, self._destination)  # Extract to destination
                else:
                    pass  # Do nothing if the csv file already exists and Force is False

            else:
                logger.debug("\t\tExtracting {} to {}".format(member.name, destination))
                data.extract(member, destination)  # Extract to tempdirectory

    def _not_exists_or_force(self, member_name: str) -> bool:
        """Returns true if the file doesn't exist or force is True."""
        filepath = os.path.join(self._destination, member_name)
        return not os.path.exists(filepath) or self._force

    def _is_csvfile(self, filename: str) -> bool:
        """Returns True if filename is a csv file, returns False otherwise."""
        return ".csv" in filename
