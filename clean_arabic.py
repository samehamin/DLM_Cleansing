# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 18:33:01 2019

@author: sameamin
"""

import pandas as pd
import re

def pipeline_read_file(exten, file = None, column = None):
    df = None
    if exten == 'csv':
        df = pd.read_csv(file, encoding = 'utf-8')
    elif exten == 'excel':
        df = pd.read_excel(file, column, encoding = 'utf-8')
    return df

def pipeline_remove_duplicates_empty(tokens):
    tokens = list(filter(None, tokens))
    tokens = list(set(tokens))
    return tokens

def pipeline_remove_specials_spaces(tokens):
    tokens = [ re.sub("[^\u0621-\u06FF0-9/]+", ' ', str(token)) for  token in tokens]
    tokens = [re.sub("[ۭ]+", ' ', token) for  token in tokens]
    tokens = [token.strip() for  token in tokens if len(token.strip()) > 1]
    return tokens

def pipeline_remove_abbrev(tokens):
    # remove single chars
    for i in range(len(tokens)):
        token = tokens[i]
        token = ' '.join([w for w in str(token).split() if len(w) > 1])
        tokens[i] = token

    return tokens
    
def get_term_frequently(df):
    df = df['Entity Name'].str.split(expand=True).stack().value_counts()
    return df

#=====================================================================    
# pipeline Start
#=====================================================================
source_file_name = '101/all-ar.txt'

# read the file
data = pipeline_read_file('csv', source_file_name)
data.describe()

tokens = data['Utterances'].values

# special charachters and spaces
tokens = pipeline_remove_specials_spaces(tokens)

# remove abbreviations
tokens = pipeline_remove_abbrev(tokens)

# remove duplicates
tokens = pipeline_remove_duplicates_empty(tokens)


data = pd.DataFrame({'Utterances': tokens})
data.describe()

# check term frequently 
#df_term_freq = get_term_frequently(data)

# write the file
data.to_csv("101/all-ar-cleaned.txt", encoding='utf-8', index=False, header=False)


#df = pd.read_csv("uc5+101-ar.txt", encoding = 'utf-8')
## sorting by first name 
#df.sort_values("Utterances", inplace = True) 
#  
## dropping ALL duplicte values 
#df = df.drop_duplicates(subset ="Utterances") 
#  
## displaying data 
#df.describe()
#df.to_csv("uc5+101-ar-cleaned.txt", encoding='utf-8', index=False, header=False)
