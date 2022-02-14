---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---
# Preface
Digital advertising, a \$374.2 billion market {cite}`marketsGlobalDigitalAdvertising2021`, is expected to reach $786.2 Billion by 2026, a compound annual growth rate (CAGR) of 13.9% over the five-year period, according to the 2021 Research and Markets report {cite}`marketsGlobalDigitalAdvertising2021a`. At the nexus of large-scale search, text analysis and information retrieval, statistical modeling, machine learning, optimization and microeconomics, programmatic advertising refers to the automated buying and selling of massive inventories of digital ad space, facilitated by a real-time bidding (RTB) process. The digital data explosion, exponential growth in computing power, and advances in machine learning and optimization have propelled programmatic advertising to an estimated $129 billion in global spending.

In this context, conversion rate prediction becomes crucial in several respects.

First, the RTB process described above allows advertisers to tailor bidding strategies to the performance objectives of their campaigns. If the goal is brand awareness, advertisers may adopt a cost-per-impression (CPM) bidding strategy. Performance-based campaigns can algorithmically configure bidding strategies based upon a cost-per-click (CPC) and cost-per-action (CPA) also known as cost-per-conversion, pricing models. In either event, crafting such conditional bidding strategies requires advertisers to estimate the probability that an impression will lead to a click or conversion. Hence conversion rate prediction is essential to programmatic advertising.

Second, conversion rate prediction is a prerequisite for marketing decision-making. Understanding how and why users respond to digital advertising campaigns enables better resource and ad spend decisions, supports specific, measurable, achievable, realistic, and time-bound goal setting, and informs campaign channel tactics and market strategy.

Third, conversion rate prediction accuracy reflects greater customer insight and understanding of user preferences, personalities, and behavior which can be leveraged to create personalized customer journeys.

Lastly, accurate conversion rate prediction positively correlates with more accurate ROI forecasts.


## Conversion Rate Prediction Challenge
Yet, predicting user behavior in a real-time, data-intensive, context has its challenges {cite}`gharibshahUserResponsePrediction2021`.

-	**Scalability**: Of the 7.7 billion people in the world, 3.5 billion are online {cite}`owidinternet` and most of us will encounter between 4,000 and 10,000 per day {cite}`simpsonCouncilPostFinding`. The top 10 online advertisers of 2020 generated over 90 billion impressions in the 1st quarter alone {cite}`TopOnlineAdvertisers`. Machine learning approaches to predict user response must be built to scale.
-	**Conversion Rarity**: Median conversion rates for eCommerce are in the range of 2.35% to 5.31% across all industries {cite}`WhatGoodConversion2014`, still average conversion rate among eCommerce companies fall between 1.84% and 3.71% {cite}`EcommerceConversionRates2022`.
-	**Data Sparsity**: Two factors contribute to the sparsity of data. First, most of the input data are binary representations of categorical features, resulting in high-dimensional vectors with few non-zero values. Second user interactions follow the power-law distribution whereby the majority of users are interacting with a relatively small number of items or products.
-	**Delayed Conversion Feedback**: Though the time between an ad impression and a click may be seconds, the time delay between a click and a conversion could be hours, days, or longer. Today, machine learning solutions must be able to predict in the context of this delayed feedback.

DeepCVR explores state-of-the-art deep learning methods to improve conversion rate prediction for the online advertising industry. Starting with baseline logistic regression models, we’ll explore supervised techniques such as factorization machines, multi-layer perceptron, convolutional neural network, recurrent neural network, and neural attention network architectures.

