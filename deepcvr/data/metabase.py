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
# Modified : Sunday, March 13th 2022, 5:59:27 pm                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
from dataclasses import dataclass
import logging
import pandas as pd
from typing import Any

from deepcvr.utils.config import config_dag
from deepcvr.data.database import DAO

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d-%Y %H:%M",
    filename="logs/metevent.log",
    filemode="w",
)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@dataclass
class EventParams:
    module: str
    classname: str
    method: str
    action: str
    param1: Any = None
    param2: Any = None
    param3: Any = None


class Asset(DAO):
    """Metadata asset repository """

    __filepath = "config/credentials.yaml"

    def __init__(self) -> None:
        connection_string = config_dag(Asset.__filepath)
        super(Asset, self).__init__(connection_string=connection_string)

    def add(self, name: str, desc: str, uri: str) -> int:
        d = {"name": name, "desc": desc, "uri": uri}
        df = pd.DataFrame(data=d, index=[0])
        self.insert(table_name="asset", data=df)

    def get(self, name: str) -> pd.DataFrame:
        sql = """SELECT id, name, desc, uri FROM asset WHERE name=%s;"""
        params = [name]
        return self.select(statement=sql, params=params)

    def getall(self) -> pd.DataFrame:
        return self.selectall(table_name="asset")

    def update(self, name: str, desc: str = None, uri: str = None) -> None:
        d = {"name": name, "desc": desc, "uri": uri}
        df = pd.DataFrame(data=d, index=[0])
        self.insert(table_name="asset", data=df)


class Event(DAO):
    """Metadata asset repository """

    __filepath = "config/credentials.yaml"

    def __init__(self) -> None:
        connection_string = config_dag(Asset.__filepath)
        super(Asset, self).__init__(connection_string=connection_string)

    def add(self, event: EventParams) -> int:
        d = {
            "module": event.module,
            "classname": event.classname,
            "method": event.method,
            "action": event.action,
            "param1": event.param1,
            "param2": event.param2,
            "param3": event.param3,
        }
        df = pd.DataFrame(data=d, index=[0])
        self.insert(table_name="asset", data=df)

    def get(self, classname: str, method: str) -> pd.DataFrame:
        sql = """SELECT id, module, classname, method, action,
        param1, param2, param3 FROM event WHERE classname=%s, method=%s;"""
        params = [classname, method]
        return self.select(statement=sql, params=params)

    def getall(self) -> pd.DataFrame:
        return self.selectall(table_name="event")
