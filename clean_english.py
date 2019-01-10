# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 18:33:01 2019

@author: sameamin
"""

import pandas as pd
import re
#from num2words import num2words


def pipeline_read_file(file = None, column = None):
    #df = pd.read_excel(file, encoding = 'utf-8')
    df = pd.read_csv(file)
    return df

def pipeline_remove_duplicates_empty(tokens):
    tokens = list(filter(None, tokens))
    #tokens = [ token.strip() for  token in tokens if len(token.strip()) > 0]
    tokens = list(set(tokens))
    return tokens

def pipeline_remove_specials_spaces(tokens):
    tokens = [ re.sub("[^A-Za-z']+", ' ', str(token)) for  token in tokens]
    #tokens = [token.strip() for  token in tokens if len(token.strip()) > 1]
    return tokens

def pipeline_remove_abbrev(tokens):
    
    for i in range(len(tokens)):
        token = tokens[i]
        
        # remove single chars
        token = ' '.join([w for w in str(token).split() if len(w) > 1])
        
        # remove wrong abbreviations
        abbrevs_wrong = ['gen', 'gen\'s', 'llc', 'co', 'tr', 'fze', 'est', 'nt', 'br', 'fzco',
                   'ltd', 'dmcc', 'fzc', 'bldg', 'fz', 'cont', 'mohd', 'ind',
                   'sh', 'dwc', 'limited', 'eng', 'rep', 'inc', 'difc', 'dsg', 'contg', 'lc',
                   'trd', 'int', 'emb', 'const', 'trd', 'll', 'px', 'adv', 'pvt', 'ind', 
                   'gmbh', 'con', 'ap', 'mea', 'llp', 'dx', 'pte', 'ag', 'sa', 'md', 'mrw',
                   'corp', 'pub', 'fzd', 'psc', 'es', 'contr', 'estb', 'eqpt', 'bu', 'hh',
                   'dsoa', 'ghq', 'lcc', 'ent', 'exhb', 'serv', 'ink', 'dist', 'ab', 'kg',
                   'hq', 'cons', 'bv', 'tra', 'wll', 'svc', 'nd', 'ad', 'lle', 'caf', 'comp',
                   'sal', 'slc', 'pdxb']
        token = ' '.join([w for w in str(token).split() if w.lower() not in abbrevs_wrong])
              
        # expand abbreviations
        abbrev_expand = {
                'dr': 'doctor', 'eng': 'engineer', 'shj': 'Sharjah', 'sh': 'sheikh',
                'shk': 'sheikh', 'Intl': 'international', 'int': 'international', 
                'govt': 'government', 'min': 'ministry', 'po': 'post office', 'maf': 'Majid Al Futtaim',
                'tec': 'tech', 'st': 'saint', 'fuj': 'Al Fujairah', 'mr': 'mister',
                'elife': 'E Life'
                }
        for word in token.split():
            if word.lower() in abbrev_expand:
                token = ' '.join([ re.sub(word, abbrev_expand[word.lower()], word)])
        
        # capitalize token
        token = ' '.join( [word.capitalize() for word in token.lower().split()] )
#        token = token.lower()
#        token = token.capitalize()
        
        # Exclude real abbreviations
        abbrevs_real = { 'i': 'I', 'kfc':'KFC', 'rak': 'RAK', 'it': 'IT', 'atm': 'ATM', 
                        'jlt': 'JLT', 'id': 'ID', 'smb': 'SMB', 'dxb': 'DXB', 
                        'auh': 'AUH', 'uk': 'UK', 'sas': 'SAS', 'vip': 'VIP', 
                        'hr': 'HR', 'us': 'US', 'usa': 'USA', 'ip': 'IP', 'sms': 'SMS', 
                        'sim': 'SIM', 'vpn': 'VPN', 'fab': 'FAB', 'fgb': 'FGB', 
                        'cctv': 'CCTV', 'ndc': 'NDC', 'acme': 'ACME', 'tv': 'TV', 
                        'usb': 'UBS',
                        'gm': 'GM',
                        'ne': 'NE'}
        for word in token.split():        
            if word.lower() in abbrevs_real:
                token = token.replace(word, abbrevs_real[word.lower()])

    
        tokens[i] = token

    return tokens

def pipeline_convert_numbers(tokens):
    # TODO: convert numbers
#    for i in range(len(tokens)):
#        tmp_token = ''
#        for w in tokens[i].split():
#            if w.isnumeric():
#                tmp_token += (num2words(int(w)) + ' ')
#            else:
#                tmp_token += (w + ' ')
#        tokens[i] = tmp_token.strip()
    return tokens


def get_term_frequently(df):
    df = df['Entity Name'].str.split(expand=True).stack().value_counts()
    return df

def export_to_excel(df):
    writer = pd.ExcelWriter('dlm_english.xlsx')
    df.to_excel(writer,'Sheet1')
    writer.save()

#=====================================================================    
# pipeline Start
#=====================================================================
# read the file
original_data = pipeline_read_file("dlm_english_10_01_19.csv")
original_data.describe()

tokens = original_data['Entity Name'].values

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
