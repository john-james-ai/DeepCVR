---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.11.5
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---
# Data
The Ali-CCP (Alibaba Click and Conversion Prediction) dataset {cite}`maEntireSpaceMultiTask2018`, was collected from Taobao, one of the largest online retailer platforms in the world.  The statistics are listed in {numref}`ali-ccp-stats` below.

```{table} Alibaba Click and Conversion Prediction Dataset Statistics
:name: ali-ccp-stats

| Entity      |             Count  |
|-------------|-------------------:|
| Users       |           400,000  |
| Items       |         4,300,000  |
| Impressions |        84,000,000  |
| Clicks      |         3,400,000  |
| Conversions |            18,000  |

```
This dataset is in format $\{(x_{i},y_{i} \rightarrow z_{i})\}|^{N}_{i=1}$, whereby each sample $(x,y \rightarrow z)$ is drawn from a distribution $\mathcal{D}$ from a domain $\mathcal{X}\times\mathcal{Y}\times\mathcal{Z}$, where:
- $\mathcal{X}$ is a feature space,
- $\mathcal{Y}$ is the click label space,
- $\mathcal{Z}$ is the conversion label space, and
- $\mathcal{N}$ is the total number of impressions.

High dimensional sparse feature vector $\mathbb{x}$ contains the user and item features. The binary labels $\mathbb{y}\in\{0,1\}$ and $\mathbb{z}\in\{0,1\}$ indicate whether a click or conversion has occurred, respectively. {numref}`label-distribution` summarizes the label distribution.

```{table} Label Distribution
:name: label-distribution

|  y  | z | Interpretation                         |
|:---:|---|----------------------------------------|
|  0  | 0 | No click or conversion                 |
|  0  | 1 | Invalid                                |
|  1  | 0 | Click has occurred, but no conversion. |
|  1  | 1 | Click and conversion has occurred      |
```
The remainder of the data section describes the data ingestion, preparation and the exploratory data analysis.

