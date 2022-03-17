#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /files.py                                                                             #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, March 14th 2022, 2:18:51 pm                                                   #
# Modified : Monday, March 14th 2022, 3:05:12 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import os
from datetime import datetime
import tarfile

# ------------------------------------------------------------------------------------------------ #


def gzip_dir(source: str, destination: str, name: str) -> str:
    """Compresses files in a directory using gzip compression

    Args:
        source: Directory containing files to compress archive
        destination: Directory into which the gzip file is to be saved
        name: short alphanumeric description of file.
            Should not have date, periods, or file extension.

    Returns (str): filepath of compressed archive

    """
    name = os.path.splitext(name)[0]  # Just in case.
    filename = name + "_" + datetime.now().strftime("%Y-%m-%d") + ".tar.gz"
    filepath = os.path.join(destination, filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with tarfile.open(filepath, "w:gz") as tar:
        for file in os.listdir(source):
            path = os.path.join(source, file)
            tar.add(path)

    return filepath


# ------------------------------------------------------------------------------------------------ #
if __name__ == "__main__":
    source = "data/production/s3"
    destination = "data/production/external"
    name = "taobao"
    gzip_dir(source=source, destination=destination, name=name)
