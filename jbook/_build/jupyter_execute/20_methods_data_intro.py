#!/usr/bin/env python
# coding: utf-8

# # Data
# The Criteo Sponsored Search Conversion Log Dataset is provided by Criteo Labs {cite}`tallisReactingVariationsProduct2018` and contains a 90-day sample of logs obtained from Criteo Predictive Search (CPS). Each row in the dataset represents an action (i.e. click) performed by the user on a product related advertisement. The product advertisement was shown to the user, post the user expressing an intent via an online search engine.  Each row in the dataset, contains product characteristics (age, brand, gender, price), time of the click ( subject to uniform shift), user characteristics and device information. The logs also contain information on whether the clicks eventually led to a conversion (product was bought) within a 30 day window and the time between click and the conversion.
# 
# **Delimited**: \t (tab separated)
# 
# **Missing Value Indicator**: -1 ( Missing value indicator is 0 for click_timestamp)
# 
# **Outcome/Labels**
# - Sale : Indicates 1 if conversion occurred and 0 if not).
# - SalesAmountInEuro : Indicates the revenue obtained when a conversion took place. This might be different from product-price, due to attribution issues. It is -1, when no conversion took place.
# - Time_delay_for_conversion : This indicates the time between click and conversion. It is -1, when no conversion took place.
# 
# **Features**
# - click_timestamp: Timestamp of the click. The dataset is sorted according to timestamp.
# - nb_clicks_1week: Number of clicks the product related advertisement has received in the last 1 week.
# - product_price: Price of the product shown in the advertisement.
# - product_age_group: The intended user age group of the user, the product is made for.
# - device_type: This indicates whether it is a returning user or a new user on mobile, tablet or desktop.
# - audience_id:  Meaning of this feature not disclosed
# - product_gender: The intended gender of the user, the product is made for.
# - product_brand: Categorical feature about the brand of the product.
# - product_category(1-7): Categorical features associated to the product. Meaning of features not disclosed.
# - product_country: Country in which the product is sold.
# - product_id: Unique identifier associated with every product.
# - product_title: Hashed title of the product.
# - partner_id: Unique identifier associated with the seller of the product.
# - user_id: Unique identifier associated with every user.
# 
# Note : All categorical features have been hashed.
# 
# **Dataset Size**
#   - Compressed: 310 MB
#   - ExtractRawDataed: 6.4 GB
# 
# **Data Access**
# 
# As of this writing the dataset may be obtained from the [Criteo AI Lab](http://ailab.criteo.com/criteo-sponsored-search-conversion-log-dataset/) website.
