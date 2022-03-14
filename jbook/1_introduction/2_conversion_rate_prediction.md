---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: Python3
  language: python3
  name: python3
---
# Conversion Rate Prediction
Yet, predicting user behavior in a real-time, data-intensive, context has its challenges {cite}`gharibshahUserResponsePrediction2021`.

-	**Scalability**: Of the 7.7 billion people in the world, 3.5 billion are online {cite}`owidinternet` and most of us will encounter between 4,000 and 10,000 per day {cite}`simpsonCouncilPostFinding`. The top 10 online advertisers of 2020 generated over 90 billion impressions in the 1st quarter alone {cite}`TopOnlineAdvertisers`. Machine learning approaches to predict user response must be built to scale.
-	**Conversion Rarity**: Median conversion rates for eCommerce are in the range of 2.35% to 5.31% across all industries {cite}`WhatGoodConversion2014`, still average conversion rate among eCommerce companies fall between 1.84% and 3.71% {cite}`EcommerceConversionRates2022`.
-	**Data Sparsity**: Two factors contribute to the sparsity of data. First, most of the input data are binary representations of categorical features, resulting in high-dimensional vectors with few non-zero values. Second user interactions follow the power-law distribution whereby the majority of users are interacting with a relatively small number of items or products.
-	**Delayed Conversion Feedback**: Though the time between an ad impression and a click may be seconds, the time delay between a click and a conversion could be hours, days, or longer. Today, machine learning solutions must be able to predict in the context of this delayed feedback.

DeepCVR explores state-of-the-art deep learning methods to improve conversion rate prediction for the online advertising industry. Starting with baseline logistic regression models, we’ll explore supervised techniques such as factorization machines, multi-layer perceptron, convolutional neural network, recurrent neural network, and neural attention network architectures.

