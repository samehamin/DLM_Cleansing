# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 18:33:01 2019

@author: sameamin
"""

import pandas as pd
import re

def pipeline_read_excel(file = None, column = None):
    df = pd.read_csv(file, encoding = 'utf-8')
    return df

def pipeline_remove_duplicates_empty(tokens):
    tokens = list(filter(None, tokens))
    tokens = list(set(tokens))
    return tokens

def pipeline_remove_specials_spaces(tokens):
    tokens = [ re.sub("[^\u0621-\u06FF]+", ' ', str(token)) for  token in tokens]
    tokens = [re.sub("[ۭ]+", ' ', token) for  token in tokens]
    tokens = [token.strip() for  token in tokens if len(token.strip()) > 1]
    return tokens

def pipeline_remove_abbrev(tokens):
    # remove single chars
    for i in range(len(tokens)):
        token = tokens[i]
        token = ' '.join([w for w in str(token).split() if len(w) > 1])
        tokens[i] = token
    
    # remove abbreviations
    for i in range(len(tokens)):
        token = tokens[i]
        token = re.sub(" ذم ", ' ', token)
        token = re.sub(" م ", ' ', token)
        token = re.sub(" ار ", ' ', token)
        token = re.sub(" هه ", ' ', token)
        tokens[i] = token

    return tokens
    
def get_term_frequently(df):
    df = df['Entity Name'].str.split(expand=True).stack().value_counts()
#    df = pd.DataFrame({'terms': df.index})
#    s = df.terms.str.len().sort_values().index
#    pipeline_remove_duplicates_empty(s)
#    df = df.reindex(s)
    return df

def export_to_excel(df):
    writer = pd.ExcelWriter('dlm_arabic.xlsx')
    df.to_excel(writer,'Sheet1')
    writer.save()
    
# pipeline Start

# read the file
data = pipeline_read_excel("training_set_all_dq_ar.xlsx")
data.describe()

tokens = data['Entity Name'].values

# special charachters and spaces
tokens = pipeline_remove_specials_spaces(tokens)

# remove abbreviations
tokens = pipeline_remove_abbrev(tokens)

# remove duplicates
tokens = pipeline_remove_duplicates_empty(tokens)


data = pd.DataFrame({'Entity Name': tokens})
data.head()

# check term frequently 
df_term_freq = get_term_frequently(data)

# write the file
export_to_excel(data)