#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /hello.py                                                                             #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Saturday, March 5th 2022, 10:50:46 pm                                                 #
# Modified :                                                                                       #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return "Hello world!"


dag = DAG(
    "hello_world",
    description="Simple tutorial DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2017, 3, 20),
    catchup=False,
)

dummy_operator = DummyOperator(task_id="dummy_task", retries=3, dag=dag)

hello_operator = PythonOperator(task_id="hello_task", python_callable=print_hello, dag=dag)

dummy_operator >> hello_operator
