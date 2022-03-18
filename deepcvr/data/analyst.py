#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project  : DeepCVR: Deep Learning for Conversion Rate Prediction                                 #
# Version  : 0.1.0                                                                                 #
# File     : /analyst.py                                                                           #
# Language : Python 3.8.12                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author   : John James                                                                            #
# Email    : john.james.ai.studio@gmail.com                                                        #
# URL      : https://github.com/john-james-ai/cvr                                                  #
# ------------------------------------------------------------------------------------------------ #
# Created  : Friday, March 18th 2022, 1:09:47 am                                                   #
# Modified : Friday, March 18th 2022, 7:40:13 am                                                   #
# Modifier : John James (john.james.ai.studio@gmail.com)                                           #
# ------------------------------------------------------------------------------------------------ #
# License  : BSD 3-clause "New" or "Revised" License                                               #
# Copyright: (c) 2022 Bryant St. Labs                                                              #
# ================================================================================================ #
from abc import ABC
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns

plt.style.use(["denim"])
# ------------------------------------------------------------------------------------------------ #


class FeatureAnalyst(ABC):
    """Base class for univariate analysis of features

    Analyses have three primary components: the data, descriptive statistics for the data, and
    a graphics component.

    Args:
        feature_name (str): The name for the feature to be analyzed.

        tablename (str): The table in which the feature exists. Valid values include
            the 'features' table or the 'common_features' table.

        connection_string (str): Connection string to the database.

        ax (plt.Axes): The axes upon which visualizations will be rendered

        fig (plt.Figure): The figure upon which the plot will be rendered

        kwargs (dict): Optional parameters that specify visual aspects of the plots.
            Optional keyword arguments include:

            ======================  ==============================================================
            Property                Description
            ----------------------  --------------------------------------------------------------
            figsize (float, float)  The (width, height) of the figure in inches
            facecolor (str)         The figure patch face color. Default is 'white'
            edgecolor (str)         The figure patch edge color. Default is 'white'
            linewidth (float)       The linewidth of the frame
            tight_layout (bool)     Indicates whether to use the tight layout mechanism
            alpha (float)           Scalar in [0-1] range to specify transparency.


    """

    def __init__(
        self,
        feature_name: str,
        tablename: str,
        connection_string: str,
        fig: plt.Figure,
        ax: plt.Axes,
        **kwargs: dict
    ) -> None:

        self._feature_name = feature_name
        self._tablename = tablename

        from deepcvr.data.database import DAO

        self._dao = DAO(connection_string)
        self._fig = fig
        self._ax = ax
        self._figsize = kwargs.pop("figsize", [6.4, 4.8])
        self._facecolor = kwargs.pop("facecolor", "152A38")
        self._edgecolor = kwargs.pop("edgecolor", "152A38")
        self._linewidth = kwargs.pop("linewidth", 1.5)
        self._tight_layout = kwargs.pop("tight_layout", True)

        self._data = None
        self._value_counts = None

    # -------------------------------------------------------------------------------------------- #
    #                                        PROPERTIES                                            #
    # -------------------------------------------------------------------------------------------- #

    @property
    def feature_name(self) -> str:
        """Returns the name of the feature being analyzed"""
        return self._feature_name

    @feature_name.setter
    def feature_name(self, feature_name: str) -> None:
        """Sets the feature to be analyzed"""
        self._feature_name = feature_name
        self._reset_data()

    # -------------------------------------------------------------------------------------------- #
    @property
    def tablename(self) -> str:
        """Returns the name of the feature being analyzed"""
        return self._tablename

    @tablename.setter
    def tablename(self, tablename: str) -> None:
        """Sets the table for the feature being analyzed."""
        self._tablename = tablename
        self._reset_data()

    # -------------------------------------------------------------------------------------------- #
    @property
    def fig(self) -> plt.Figure:
        """Returns the current figure or generates one if no figure exists."""
        if not hasattr(self, "_fig") or self._fig is None:
            self._fig = plt.gcf()
        return self._fig

    @fig.setter
    def fig(self, fig) -> None:
        """Sets the figure object"""
        self._fig = fig

    # -------------------------------------------------------------------------------------------- #
    @property
    def figsize(self) -> tuple:
        """ Returns the size of the figure in inches."""
        if not hasattr(self, "_figsize") or self._figsize is None:
            self._figsize = self._fig.get_size_inches()
        return self._figsize

    @figsize.setter
    def figsize(self, figsize) -> None:
        """Sets the figure size"""
        self._figsize = figsize

    # -------------------------------------------------------------------------------------------- #
    #                                        DATA ACCESS                                           #
    # -------------------------------------------------------------------------------------------- #
    def get_data(self) -> pd.DataFrame:
        """Returns the feature name, id, and value, from the dataset."""
        if self._data is not None:
            return self._data
        else:
            if "common" in self._tablename:
                statement = "SELECT * FROM common_features WHERE feature_name=%s;"
            else:
                statement = "SELECT * FROM features WHERE feature_name=%s;"
            self._data = self._dao.select(statement=statement, params=(self._feature_name))
        return self._data

    # -------------------------------------------------------------------------------------------- #
    def sample_data(self, n: int = 5, random_state: int = None) -> pd.DataFrame:
        """Returns a random sampling of n rows containing the feature being analyzed.

        Args:
            n (int): The number of random samples to return

            random_state (int): Seed for pseudo random number generation
        """

        df = self.get_data()
        return df.sample(n=n, replace=False, axis=0, ignore_index=False, random_state=random_state)

    # -------------------------------------------------------------------------------------------- #
    def reset_data(self) -> None:
        self._data = None
        self._value_counts = None

    # -------------------------------------------------------------------------------------------- #
    #                                      STATISTICS                                              #
    # -------------------------------------------------------------------------------------------- #
    def get_unique(self) -> list:
        """Returns a list of unique values for a feature"""
        df = self.get_data()
        return df["feature_value"].unique()

    # -------------------------------------------------------------------------------------------- #
    def get_nunique(self) -> int:
        """Returns the count of unique values for the feature"""
        return len(self.get_unique())

    # -------------------------------------------------------------------------------------------- #
    def get_uniqueness(self) -> float:
        """Returns number of unique values versus total observations as a percentage"""
        df = self.get_data()
        return self.get_nunique() / df.shape[0] * 100

    # -------------------------------------------------------------------------------------------- #
    def get_value_counts(self) -> pd.DataFrame:
        if self._value_counts is not None:
            return self._value_counts
        else:
            return self._compute_value_counts()

    # -------------------------------------------------------------------------------------------- #
    def _compute_value_counts(self) -> pd.DataFrame:
        df = self.get_data()
        vc = (
            df["feature_value"]
            .value_counts(sort=True, ascending=False, dropna=True)
            .to_frame()
            .reset_index()
        )
        vc.columns = ["Value", "Count"]
        vc["Cumulative"] = vc["Count"].cumsum()
        vc["Percent of Data"] = vc["Cumulative"] / len(vc.dropna()) * 100
        vc["Rank"] = np.arange(1, len(vc) + 1)
        vc["Value Rank"] = vc["Rank"].astype("category")
        self._value_counts = vc
        return self._value_counts

    # -------------------------------------------------------------------------------------------- #
    def get_missing_count(self) -> int:
        """Returns the number of observations with missing feature values"""
        df = self.get_data()
        return len(df.loc[df["feature_value"].isna()])

    # -------------------------------------------------------------------------------------------- #
    def get_missingness(self) -> float:
        """Returns the percentage of the feature values that are missing """
        df = self.get_data()
        return self.get_missing_count() / df.shape[0] * 100

    # -------------------------------------------------------------------------------------------- #
    def describe(self) -> pd.DataFrame:
        """Return descriptive statistics for the feature"""
        df = self.get_data()
        return df.describe()

    # -------------------------------------------------------------------------------------------- #
    def _finalize(
        self, title: str = None, filepath: str = None, show: bool = True, **kwargs
    ) -> plt.Axes:

        if title:
            self._ax.set_title(title)

        if filepath is not None:
            plt.savefig(filepath, **kwargs)

        if show:
            if self._tight_layout:
                plt.tight_layout()
                plt.show()
            else:
                plt.show()

        return self._ax


# ================================================================================================ #
#                                 CATEGORICAL FEATURE ANALYST                                      #
# ================================================================================================ #
class CategoricalFeatureAnalyst(FeatureAnalyst):
    """Conducts analysis on categorical features"""

    def __init__(
        self,
        feature_name: str,
        tablename: str,
        connection_string: str,
        fig: plt.Figure,
        ax: plt.Axes,
        **kwargs: dict
    ) -> None:

        super(CategoricalFeatureAnalyst, self).__init__(
            feature_name=feature_name,
            tablename=tablename,
            connection_string=connection_string,
            fig=fig,
            ax=ax,
            kwargs=kwargs,
        )

    # -------------------------------------------------------------------------------------------- #
    def barchart(
        self, title: str = None, filepath: str = None, show: bool = True, **kwargs
    ) -> plt.Axes:
        """Bar chart showing values on x axis and counts on the y axis."""
        vc = self.get_value_counts()
        self._ax = sns.barplot(x="value", y="count", data=vc, palette="Blues_d")

        return self._finalize(title=title, filepath=filepath, show=show, **kwargs)

    # -------------------------------------------------------------------------------------------- #
    def cfd(self, title: str = None, filepath: str = None, show: bool = True, **kwargs) -> plt.Axes:
        """Renders a cumulative frequency distribution plot"""

        self._ax = sns.lineplot(
            x="Rank",
            y="Percent of Data",
            palette=self._palette,
            data=self._value_counts,
            marker="o",
            ax=self._ax,
        )

        self._ax.set(xlabel="Value Rank", ylabel="Percent of Data")
        self._ax.axhline(95)

        return self._finalize(title=title, filepath=filepath, show=show, **kwargs)

    # -------------------------------------------------------------------------------------------- #
    def zipf(
        self, title: str = None, filepath: str = None, show: bool = True, **kwargs
    ) -> plt.Axes:
        """Zipf's plot of Rank vs Frequency on Log Scale"""

        self._ax = sns.lineplot(
            x=np.log(self._value_counts["Rank"]),
            y=np.log(self._value_counts["Count"]),
            palette=self._palette,
            ax=self._ax,
        )

        self._ax.set(xlabel="Value Rank", ylabel="Percent of Data")
        self._ax.axhline(95)

        return self._finalize(title=title, filepath=filepath, show=show, **kwargs)


# ================================================================================================ #
#                                  NUMERIC FEATURE ANALYST                                         #
# ================================================================================================ #
class NumericFeatureAnalyst(FeatureAnalyst):
    """Analysis of numeric features"""

    def __init__(
        self,
        feature_name: str,
        tablename: str,
        connection_string: str,
        fig: plt.Figure,
        ax: plt.Axes,
        **kwargs: dict
    ) -> None:

        super(NumericFeatureAnalyst, self).__init__(
            feature_name=feature_name,
            tablename=tablename,
            connection_string=connection_string,
            fig=fig,
            ax=ax,
            kwargs=kwargs,
        )

    # -------------------------------------------------------------------------------------------- #
    def histogram(
        self, title: str = None, filepath: str = None, show: bool = True, **kwargs
    ) -> plt.Axes:
        """Represents distribution of data of continuous variables"""

        df = self.get_data()
        sns.histplot(
            data=df["feature_value"], x="feature_value", palette=self._palette, ax=self._ax
        )

        return self._finalize(title=title, filepath=filepath, show=show, **kwargs)

    # -------------------------------------------------------------------------------------------- #
    def boxplot(
        self, title: str = None, filepath: str = None, show: bool = True, **kwargs
    ) -> plt.Axes:

        df = self.get_data()
        sns.boxplot(x="feature_value", data=df["feature_value"], palette=self._palette, ax=self._ax)

        return self._finalize(title=title, filepath=filepath, show=show, **kwargs)
