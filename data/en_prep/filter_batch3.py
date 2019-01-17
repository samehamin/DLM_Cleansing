# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 18:33:01 2019

@author: sameamin
"""

import pandas as pd
import re
from num2words import num2words


def pipeline_read_file(file = None, column = None):
    #df = pd.read_excel(file, encoding = 'utf-8')
    df = pd.read_csv(file, encoding = 'utf-8')
    return df

#=====================================================================    
# pipeline Start
#=====================================================================
batch1 = pipeline_read_file('dlm_english_10_01_19_v1.csv')
batch2 = pipeline_read_file('12_01_19_source_en.csv')
all_en = pipeline_read_file('dlm_en.csv')

# filter batch 1 - 177240
batchs_merge = batch1.merge(batch2, how='outer')
batchs_merge = batchs_merge.drop_duplicates()
batches_filtered = all_en[~all_en['Entity Name'].isin(batchs_merge['Entity Name'])]
batches_filtered.describe()

#export
batches_filtered.to_csv('dlm_en_filtered.csv', index=False)