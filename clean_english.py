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

def pipeline_remove_duplicates_empty(tokens):
    tokens = list(filter(None, tokens))
    #tokens = [ token.strip() for  token in tokens if len(token.strip()) > 0]
    tokens = list(set(tokens))
    return tokens

def pipeline_remove_specials_spaces(tokens):
    tokens = [ re.sub("[^A-Za-z0-9']+", ' ', str(token)) for  token in tokens]
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
                   'sal', 'slc', 'pdxb', 'trdg', 'trdgco', 'cowll', 'srl', 'coltd',
                   'zllcf', 'mfg', 'jv', 'pjsc', 'wwl', 'ser', 'pmdc', 'lda',
                   'dept', 'trllc', 'fzllc', 'collc', 'foodstuff', 'catering', 'indllc',
                   'trdcoltd', 'trdgest', 'fzoc', 'ltdco', 'lllc', 'col', 'tradcollc', 
                   'corpn', 'trdllc', 'trdest', 'indltd', 'llcc', 'equiptrest', 'contcollc', 
                   'servicesllc', 'llcbr', 'ltdd', 'contllc', 'eastfze', 'llco', 'lll',
                   'icd', 'tradg', 'fzer']
        token = ' '.join([w for w in str(token).split() if w.lower() not in abbrevs_wrong])

        # expand abbreviations
        abbrev_expand = {
                'dr': 'Doctor', 'eng': 'Engineer', 'shj': 'Sharjah', 'sh': 'Sheikh',
                'shk': 'Sheikh', 'intl': 'International',
                'govt': 'Government', 'min': 'Ministry', 'po': 'Post Office', 
                'maf': 'Majid Al Futtaim',
                'tec': 'Technical', 'st': 'Saint', 'fuj': 'Al Fujairah', 'mr': 'Mister',
                'elife': 'E Life', '&': 'and', '2nd': 'Second', 'GENL': 'General',
                'hi': 'High',
                'egov': 'E Government',
                'govtof': 'Government of',
                'maint': 'Maintenance',
                'mant': 'Maintenance',
                'elect': 'electronic', 
                'ajm': 'Ajman'
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
                        'ne': 'NE',
                        'uae': 'UAE',
                        'aed': 'AED',
                        'ss': 'SS',
                        'dhl': 'DHL', 
                        'tnt': 'TNT', 
                        'jw': 'JW',
                        'ibm': 'IBM',
                        'adcb': 'ADCB',
                        'adib': 'ADIB',
                        'mbc': 'MBC',
                        'itl': 'ITL', 'dib': 'DIB', 'att': 'AT and T'
                        }
        for word in token.split():
            if word.lower() in abbrevs_real:
                token = token.replace(word, abbrevs_real[word.lower()])

    
        tokens[i] = token

    return tokens

def pipeline_convert_numbers(tokens):
    # TODO: convert numbers
    for i in range(len(tokens)):
        
        tmp_token = ''
        for w in tokens[i].split():
            if w.isnumeric():
                tmp_token += (num2words(int(w)) + ' ')
            else:
                tmp_token += (w + ' ')
        tokens[i] = tmp_token.strip()

    return tokens

def separate_al_char(tokens):
    exclude_al = ['ali', 'al', 'alarm', 'alia']
    
    for i in range(len(tokens)):
        token = tokens[i]

        for word in token.split():
            if word.lower() not in exclude_al:
                word_temp = re.sub(r'^al+\w*', 'al ' + str(word[2:]), word.lower())
                token = token.replace(word, word_temp)

        tokens[i] = token
    return tokens

def get_term_frequently(df):
    df = df['Entity Name'].str.split(expand=True).stack().value_counts()
    return df

def export_to_file(exten, df, file_name):
    if exten == 'excel':
        writer = pd.ExcelWriter(file_name)
        df.to_excel(writer,'Sheet1')
        writer.save()
    elif exten == 'csv':
        df.to_csv(file_name, encoding='utf-8', index=False)

#=====================================================================    
# pipeline Start
#=====================================================================
# read the file
original_data = pipeline_read_file("data/dlm_12_01_19/12_01_19_source_en.csv")

tokens = original_data['Entity Name'].values

# convert numbers to text
tokens = pipeline_convert_numbers(tokens)

# special charachters and spaces
tokens = pipeline_remove_specials_spaces(tokens)

# separate Al arabic char
tokens = separate_al_char(tokens)

# should be the last step - remove abbreviations 
tokens = pipeline_remove_abbrev(tokens)

# remove duplicates
tokens = pipeline_remove_duplicates_empty(tokens)


data = pd.DataFrame({'Entity Name': tokens})
data.head()

# check term frequently 
df_term_freq = get_term_frequently(data)

# write the file
export_to_file('csv', data, 'data/dlm_12_01_19/dlm_en_12_01_19_v1_result.csv')