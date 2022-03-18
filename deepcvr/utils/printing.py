#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /printing.py                                                                          #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, March 4th 2022, 3:27:37 pm                                                    #
# Modified : Thursday, March 17th 2022, 7:46:32 am                                                 #
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
# File     : \printing.py                                                      #
# Language : Python 3.7.12                                                     #
# ---------------------------------------------------------------------------- #
# Author   : John James                                                        #
# Email    : john.james.ai.studio@gmail.com                                    #
# URL      : https://github.com/john-james-ai/cvr                              #
# ---------------------------------------------------------------------------- #
# Created  : Friday, December 24th 2021, 12:27:22 pm                           #
# Modified : Friday, February 4th 2022, 3:50:40 pm                             #
# Modifier : John James (john.james.ai.studio@gmail.com)                       #
# ---------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2022 Bryant St. Labs                                          #
# ============================================================================ #
""" Print utilities."""
import math
import statistics
import numbers
import pandas as pd

pd.options.display.float_format = "{:,.2f}".format


# --------------------------------------------------------------------------- #
#                                Print                                        #
# --------------------------------------------------------------------------- #


class Printer:
    """Printint behavior."""

    def __init__(self, line_length=80, title_separator="_"):
        self._line_length = min(line_length, 80)
        self._default_anchor_pos = int(math.floor(self._line_length / 2))
        self._default_anchor_style = ":"
        self._default_anchor_lhs_pad = 1
        self._default_anchor_rhs_pad = 1
        self._title_separator = title_separator

    def _compute_budgets(self, anchor):
        """Computes available space on left and right sides."""
        anchor["lhs_budget"] = anchor["pos"] - anchor["lhs_pad"]
        anchor["rhs_budget"] = self._line_length - anchor["rhs_pad"] - anchor["pos"]
        return anchor

    def _set_anchor(self, content):
        """Sets vertical anchor point for text alignment."""

        # Initialize anchor, padding and budget with default values
        anchor = {}
        anchor["style"] = self._default_anchor_style
        anchor["pos"] = self._default_anchor_pos
        anchor["lhs_pad"] = self._default_anchor_lhs_pad
        anchor["rhs_pad"] = self._default_anchor_rhs_pad
        # Compute budgets given default anchor position
        anchor = self._compute_budgets(anchor)

        # Get line lengths from content
        lhs_lens = [len(str(k)) for k in content.keys()]
        rhs_lens = [len(str(v)) for v in content.values()]

        # If all lengths are within budget return self._anchor_pos
        if max(lhs_lens) <= anchor["lhs_budget"] and max(rhs_lens) <= anchor["rhs_budget"]:
            return anchor

        # Otherwise adjust anchor by 1/2 (avg_rhs - avg_lhs)
        lhs_avg_len = statistics.mean(lhs_lens)
        rhs_avg_len = statistics.mean(rhs_lens)
        anchor["pos"] -= 1 / 2 * (rhs_avg_len - lhs_avg_len)
        # Adjust the budgets for lhs and rhs accordingly
        anchor = self._compute_budgets(anchor)

        return anchor

    def print_title(self, title, subtitle=None):
        """Prints a title and optional subtitle below and centered.

        Args:
            title (str): The title to print centered
            subtitle (str): The subtitle to print centered below the title
        """
        if not isinstance(self._line_length, (int, float)):
            raise TypeError("Invalid line length. Must be an integer or float.")
        print("\n")
        print(title.center(self._line_length))
        if subtitle:
            title_separator = self._title_separator * max(len(title), len(subtitle))
            print(subtitle.center(self._line_length))
        else:
            title_separator = self._title_separator * len(title)

        print(title_separator.center(self._line_length))

    def _print_line(self, anchor, k, v):
        v = (
            format(v, ",")
            if isinstance(v, (int, float, numbers.Integral, numbers.Real))
            else str(v)
        )
        lhs_pad = " " * int(anchor["pos"] - anchor["lhs_pad"] - len(k))
        line = lhs_pad + k + " " * anchor["lhs_pad"] + anchor["style"] + " " * anchor["rhs_pad"] + v
        print(line)

    def print_blank_line(self):
        """Prints a blank line."""
        print("\n")

    def print_dictionary(self, content, title=None):
        """Pretty prints a title and dictionary.

        Args:
            content (dict): The content in dictionary format.
            title (str): Optional title to print above the data.
        """
        anchor = self._set_anchor(content)
        if title:
            self.print_title(title)
        for k, v in content.items():
            self._print_line(anchor, k, v)

    def print_dataframe(
        self, content, title=None, precision: int = 2, thousands: str = ",", index: bool = False,
    ):
        """Prints a pandas DataFrame

        Args:
        title (str):  The title to be printed above the table.
        content (pd.DataFrame):  DataFrame to print.
        """
        if title:
            self.print_title(title)
        print(content)

    def print_header(self, header, top: bool = True, bottom: bool = True, dash: str = "=") -> None:
        header = header.replace("_", " ")
        header = header.title()
        available = self._line_length - 4
        print("\n")
        if top:
            print("# ", available * dash, " #")
        spaces = available - len(header)
        if spaces % 2 == 0:
            left = int(spaces / 2) - 1
            right = int(spaces / 2) - 1
        else:
            left = int(spaces / 2) - 1
            right = int(spaces / 2)
        print("  ", left * " ", header, right * " ", "  ")
        if bottom:
            print("# ", available * dash, " #")

    def print_footer(self, dash: str = "=") -> None:
        available = self._line_length - 4
        print("# ", available * dash, " #")
