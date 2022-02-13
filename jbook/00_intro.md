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
# Introduction
Digital advertising, a \$374.2 billion market {cite}`marketsGlobalDigitalAdvertising2021`, delivers advertising and promotional content to users through a range of online and digital channels such as search engines, mobile- apps, social media, email, and websites. Yet, digital marketing expenditures are expected to yield a predictable and positive return on investment (ROI) in terms of sales. Conversion rate, arguably the leading predictor of ROI, is defined as the number of conversions per visitor, or:

$$\text{CVR}=\frac{\text{Number of Conversions}}{\text{Number of Visitors}}* 100$$

What's more, investment in conversion rate optimization (CRO), the process of increasing the percentage of conversions, is significant and for good reason. It pays off! As shown in {numref}`cvr_spend_rates`, the proportion of marketing spend allocated to conversion optimization positively correlates with higher conversion rates.

```{table} Conversion Optimization Spend vs Conversion Rates
:name: cvr_spend_rates

|     Average Conversion Rate    |     Up to 25%    |     More than 25%    |
|--------------------------------|------------------|----------------------|
|     Less than 0.5%             |     21%          |     7%               |
|     0.5% - 0.9%                |     16%          |     10%              |
|     1% - 1.9%                  |     21%          |     16%              |
|     2% - 4.9%                  |     26%          |     28%              |
|     5% - 8.9%                  |     8%           |     18%              |
|     9% and above               |     8%           |     21%              |
```

Source: [eConsultancy](https://econsultancy.com/most-companies-spend-less-than-5-of-marketing-budgets-on-conversion-optimization/)

These data {cite}`mothMostCompaniesSpend2013` show that CRO spending has its benefits. Firms investing over 25% of their marketing budgets on CRO are twice as likely to enjoy greater conversion rates.

Moreover, the explosive growth of customer behavior data has stimulated a need for efficient and effective computational methods to understand, influence, and predict customer behavior. To address this need, research and industry have developed a range of deep learning and machine learning approaches to tackle this problem; including supervised and unsupervised neural networks, ensemble methods, factorization machines, and others. Yet, predicting user behavior in a real-time, data-intensive, context has its challenges {cite}`gharibshahUserResponsePrediction2021`.

-	**Scalability**: Of the 7.7 billion people in the world, 3.5 billion are online {cite}`owidinternet` and most of us will encounter between 4,000 and 10,000 per day {cite}`simpsonCouncilPostFinding`. The top 10 online advertisers of 2020 generated over 90 billion impressions in the 1st quarter alone {cite}`TopOnlineAdvertisers`. Machine learning approaches to predict user response must be built to scale.
-	**Conversion Rarity**: Median conversion rates for eCommerce are in the range of 2.35% to 5.31% across all industries {cite}`WhatGoodConversion2014`, still average conversion rate among eCommerce companies fall between 1.84% and 3.71% {cite}`EcommerceConversionRates2022`.
-	**Data Sparsity**: Two factors contribute to the sparsity of data. First, most of the input data are binary representations of categorical features, resulting in high-dimensional vectors with few non-zero values. Second user interactions follow the power-law distribution whereby the majority of users are interacting with a relatively small number of items or products.
-	**Delayed Conversion Feedback**: Though the time between an ad impression and a click may be seconds, the time delay between a click and a conversion could be hours, days, or longer. Today, machine learning solutions must be able to predict in the context of this delayed feedback.

DeepCVR explores state-of-the-art deep learning methods to improve conversion rate prediction for the online advertising industry. Starting with baseline logistic regression models, we’ll explore supervised techniques such as factorization machines, multi-layer perceptron, convolutional neural network, recurrent neural network, and neural attention network architectures.

