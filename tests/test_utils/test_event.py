#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /test_logger.py                                                                       #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Monday, March 14th 2022, 6:42:55 pm                                                   #
# Modified : Thursday, March 31st 2022, 3:38:39 pm                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#%%
import logging
import inspect
from deepcvr.utils.decorators import event, operator

# ------------------------------------------------------------------------------------------------ #
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class TestEvent1:
    @event
    def test_event_1(self, a, d: dict = {}):
        print(a)
        print(d)
        logger.debug("\tInside {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    @event
    def test_event_2(self, li=[1, 2, 3, 4], t=("t")):
        print(li)
        print(t)
        logger.debug("\tInside {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


class TestEvent2:
    @event
    def test_event_3(self, a, d: dict = {}):
        print(a, d)
        logger.debug("\tInside {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))

    @event
    def test_event_4(self, li=[1, 2, 3, 4], t=("t")):
        print(li, t)
        logger.debug("\tInside {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


class TestTaskEvent1:
    def __init__(self, a, k, li, t, d, nd):
        self.a = a
        self.k = k
        self.li = li
        self.t = t
        self.d = d
        self.nd = nd

    @operator
    def test_operator_1(self):
        logger.debug("\tInside {} {}".format(self.__class__.__name__, inspect.stack()[0][3]))


if __name__ == "__main__":
    # t = TestEvent1()
    # t.test_event_1(22, d={"ab": "cd"})
    # t.test_event_2(t=(2, 3))

    # t = TestEvent2()
    # t.test_event_3(22, d={"ab": "cd"})
    # t.test_event_4(12, t=("right", 3))

    t = TestTaskEvent1(
        "a",
        k="some_kwarg",
        li=[1, 2, 3],
        t=(1, "tuple"),
        d={"a": 3, "dict": 4},
        nd={"a": {"nested": {"dict": "4u"}}},
    )
    t.test_operator_1()

#%%
