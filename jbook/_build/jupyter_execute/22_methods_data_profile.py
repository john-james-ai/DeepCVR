#!/usr/bin/env python
# coding: utf-8

# # Data Profile
# This first examination of the data seeks to characterize data quality in its (near) raw form. Here, we will discover the scope and breadth of data preprocessing that will be considered before advancing to the exploratory analysis effort. The remainder of this section is organized as follows:
# 
#    1. Dataset Overview    
#       1.0. Dataset Summary Statistics
#       1.1. Dataset Columns Datatypes    
#       1.2. Missing Data Analysis   
#       1.3. Cardinality Analysis   
# 
#   
#    2. Qualitative Variable Analysis   
#       2.0. Descriptive Statistics     
#       2.1. Frequency Distribution Analysis     
#     
#    3. Quantitative Variable Analysis       
#       3.0. Descriptive Statistics     
#       3.1. Distribution Analysis     
#   
#    4. Summary and Recommendations    

# In[1]:


# IMPORTS
from myst_nb import glue
from cvr.core.lab import Lab, Project
import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.width', 1000)


# First, we'll obtain the 'criteo' 'preprocessed' dataset from the 'Vesuvio' lab created during data acquisition.

# In[2]:


wsm = Project()
lab = wsm.get_lab('Vesuvio')
dataset = lab.get_dataset(name='criteo', stage='preprocessed')


# ## Dataset Overview
# ### Dataset Summary Statistics

# In[8]:


summary = dataset.summary()


# In[3]:


# GLUE
_ = glue("profile_rows",summary["Rows"], display=False)
_ = glue("profile_columns", summary["Columns"], display=False)
_ = glue("profile_missing", summary["Missing Cells %"], display=False)
_ = glue("profile_size", summary["Size (Mb)"], display=False)
_ = glue("profile_dups", summary["Duplicate Rows"], display=False)
_ = glue("profile_dup_pct", summary["Duplicate Rows %"], display=False)


# This dataset contains some {glue:}`profile_rows` observations, each with {glue:}`profile_columns` columns for a size of {glue:}`profile_size` Mb.  Some {glue:}`profile_missing`% of the data are missing which reflects the sparsity of user behavior logs. Further, we have some {glue:}`profile_dups` duplicate rows which, one might consider a large number, although that makes just {glue:}`profile_dup_pct`% of the sample.
# 
# ### Dataset Columns and Datatypes

# In[10]:


dataset.info()


# Converting the pandas object variables to category data types may bring some computational efficiencies which may be material for a dataset of this size. Still, the number that stands out so far is the 45% missing rate. 

# In[12]:


_ = dataset.missing


# Here, we get a better sense of the nature of the data sparsity challenge. Nine columns have missing rates over 50%; five of which have missing rates of 90% or more. Notably, the diversity and data sparsity reflect the nature of buying behavior and are common challenges in customer segmentation and analytics. 
# 
# Still, the sparsity (and masking) of the data leaves us with few meaningful imputation strategies. In fact, one might replace the term 'missing' with 'absent'. Missing implies an error or omission in the data which may not comport with the underlying patterns. For instance, 91% of the observations have no value for product_category_5. It's possible that the data are missing at random; however, it is also possible that most products don't have 5 product categories. 
# 
# Let's take a closer look at the frequency distributions of the categories.
# ## Frequency Distribution
# Let's get an overall sense of cardinality of the data.

# In[14]:


_ = dataset.cardinality

