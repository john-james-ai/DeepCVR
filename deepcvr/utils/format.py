#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /format.py                                                                            #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Thursday, March 17th 2022, 4:37:08 am                                                 #
# Modified : Thursday, March 17th 2022, 4:39:26 am                                                 #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ============================================================================ #
# Project  : Deep Learning for Conversion Rate Prediction (CVR)                #
# Version  : 0.1.0                                                             #
# File     : \format.py                                                        #
# Language : Python 3.7.12                                                     #
# ---------------------------------------------------------------------------- #
# Author   : John James                                                        #
# Email    : john.james.ai.studio@gmail.com                                    #
# URL      : https://github.com/john-james-ai/cvr                              #
# ---------------------------------------------------------------------------- #
# Created  : Wednesday, December 29th 2021, 11:58:51 pm                        #
# Modified : Thursday, February 10th 2022, 11:16:59 am                         #
# Modifier : John James (john.james.ai.studio@gmail.com)                       #
# ---------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2022 Bryant St. Labs                                          #
# ============================================================================ #

# Datetime format for filenames
DT_FORMAT_FILENAMES = "%Y%m%d_%H%M%S"


def proper(x):
    x = x.replace("_", " ")
    x = x.capitalize()
    x = x.replace("Nan", "NaN")
    return x


def titlelize(x, allcaps: bool = False):
    x = x.replace("_", " ")
    if allcaps:
        x = x.upper()
    else:
        x = x.title()
    x = x.replace("Nan", "NaN")
    return x


def titlelize_df(df):
    new_cols = []
    cols = df.columns
    for col in cols:
        new_cols.append(titlelize(col))
    df.columns = new_cols
    return df


def s_to_dict(df):
    s = df.to_dict("list")
    for k, v in s.items():
        if isinstance(v, str):
            s[k] = v[0].split()[0]
        else:
            s[k] = v[0]
    return s
