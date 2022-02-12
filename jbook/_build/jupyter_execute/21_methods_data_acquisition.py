#!/usr/bin/env python
# coding: utf-8

# # Data Acquisition
# The Criteo Sponsored Search Conversion Log Dataset contains 90 days of live click and conversion traffic, twenty-three product features for over 16m observations. This preliminary segment will extract the data from the Criteo Labs website and stage it for analysis and downstream processing.

# In[1]:


# IMPORTS
from myst_nb import glue
import pandas as pd
from datetime import datetime
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:,.2f}'.format

from cvr.data import criteo_columns, criteo_dtypes
from cvr.core.asset import AssetPassport
from cvr.core.pipeline import DataPipelineBuilder, PipelineConfig
from cvr.core.atelier import AtelierFabrique
from cvr.utils.config import CriteoConfig 
from cvr.core.dataset import Dataset
from cvr.data.io import DatasetWriter
from cvr.data.etl import Extract, Transform
from cvr.core.task import DatasetFactory


# Yet, there are a few apriori data preprocessing technicalities worth addressing upfront as they will facilitate the profiling and analysis ahead. We'll add the column names, and convert the target variable 'sale', to a binary categorical variable. Non-numeric variables, currently coded as strings, will be convert to pandas category data types for computational and memory efficiencies. Finally, missing data are encoded with '-1'. We'll convert this missing value indicator to Pandas NA values for analysis and processing.
# 
# ## Data Pipeline
# To download, extract, and preprocess the data in a reproducible adjacent manner, we’ll construct a mini extract-transform-load (ETL) data pipeline. Once we extract the data from the Criteo Labs website, we'll persist the raw data, create a new dataset upon which we’ll perform the data preprocessing steps described above. Next, we'll create a Dataset object that exposes useful data profiling methods, then pickle the Dataset in a staging area for further data profiling and exploratry data analysis. Allons-y!
# 
# As a preliminary step, let's establish a workspace, a studio, that will support experimentation and object persistence.

# In[ ]:


name = 'incept'
description = 'là où tout commence'
factory = AtelierFabrique()
studio = factory.create(name=name, 
                        description=description, 
                        logging_level='info')


# ### Datasource
# All data pipelines begin with a data source. The configuration details below provide the data source URL and local storage filepaths.

# In[ ]:


source = CriteoConfig()
source.print()


# ### Extract
# Downloading the data the Criteo Labs site rate limits to approximately 5 Mbps. This is the most time-consuming step of the pipeline and can take upwards of 15 minutes of download time.

# In[ ]:


passport = AssetPassport(
    aid= studio.next_aid,
    asset_type = 'task',
    name = 'extract',
    description = 'Extract step of the Criteo Data ETL pipeline',
    stage = 'raw')

extract = Extract(passport=passport,
                  url=source.url,
                  download_filepath=source.download_filepath,
                  extract_filepath=source.extract_filepath,
                  destination=source.destination,
                  chunk_size=20,        # Download parameters
                  n_groups=20)


# ### Transform
# The Transform step will add column names, convert strings to category data types, replace the missing value indicators with NaNs and convert the target variable, 'sale', to a binary indicator data type.

# In[ ]:


passport = AssetPassport(
    aid = studio.next_aid,
    asset_type = 'task',
    name = 'transform',
    description = 'Transform step of the Criteo Data ETL pipeline',
    stage = 'staged')

transform = Transform(passport=passport,
                      source=source.destination,
                      colnames=criteo_columns,
                      dtypes=criteo_dtypes,
                      sep='\t',
                      missing_values=['-1',-1,-1.0,'-1.00',-1.00])


# ### Create and Save Dataset Object
# Create a Dataset object in the 'stage' stage for profiling and analysis.

# In[ ]:


passport = AssetPassport(
    aid = studio.next_aid,
    asset_type = 'task',
    name = 'create_dataset',
    description = 'Create Dataset object of the Criteo Data ETL pipeline',
    stage = 'staged')

dataset_passport = AssetPassport(
    aid = studio.next_aid,
    asset_type = 'dataset',
    name = 'criteo',
    description = 'Criteo Staged Dataset Object',
    stage = 'staged')

dataset_factory = DatasetFactory(passport=passport, dataset_passport=dataset_passport)


# In[ ]:


passport = AssetPassport(
    aid = studio.next_aid,
    asset_type = 'task',
    name = 'save',
    description = 'Save Dataset object for Criteo Data ETL pipeline',
    stage = 'staged')


dataset_writer = DatasetWriter(
    passport=passport
)


# #### DataPipeline Config and Construction
# Let's configure the pipeline with logging for progress monitoring.

# In[ ]:


config = PipelineConfig(
    logger=studio.logger,       # Logging object
    verbose=True,               # Print messages
    force=False,                # If step already completed, don't force it.
    progress=False,              # No progress bar
    dataset_repo=studio.assets,   # dataset repository
    directory=studio.assets_directory   # Assets directory for the studio
)


# Finally, we construct the data pipeline and we are a 'go' for ignition.

# In[ ]:


builder = DataPipelineBuilder()

pipeline = builder.set_config(config).set_passport(passport).add_task(extract).add_task(transform).add_task(dataset_factory).add_task(dataset_writer).build().data_pipeline

pipeline.run()


# Viola! Our dataset has been secured. Before we close this task, let's verify the pipeline endpoint.

# In[ ]:


dataset = studio.get_asset(name='criteo', asset_type='dataset', stage='staged')


# In[ ]:


dataset.head()


# In[ ]:


dataset.info


# We can obtrain this dataset from the studio 'incept' by its asset id (aid) number '0003' or by its name, data_type, and stage. Data acquisition. This concludes the data acquisition segment.
