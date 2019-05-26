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
        abbrevs_wrong = ['gen', 'gen\'s', 'llc', 'co', 'co.', 'tr', 'fze', 'est', 'nt', 'br', 'fzco',
                   'ltd', 'dmcc', 'fzc', 'fz', 'cont', 'mohd', 'ind',
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
                   'icd', 'tradg', 'fzer', 'sgj', 'llcons', 'bldconco', 'jlt',
                   'jafza', 'tradin']
        token = ' '.join([w for w in str(token).split() if w.lower() not in abbrevs_wrong])

        # capitalize token
        # token = ' '.join( [word.capitalize() for word in token.lower().split()] )
    return tokens

#def pipeline_convert_numbers(tokens):
#    # TODO: convert numbers
#    for i in range(len(tokens)):
#        
#        tmp_token = ''
#        for w in tokens[i].split():
#            if w.isnumeric():
#                tmp_token += (num2words(int(w)) + ' ')
#            else:
#                tmp_token += (w + ' ')
#        tokens[i] = tmp_token.strip()
#
#    return tokens

#=====================================================================    
# pipeline Start
#==============Tarek-26_3-101-800=Watson-Utterances-Missing-EN.xlsx=============
source_file_name = '101/all-en.txt'

# read the file
original_data = pipeline_read_file(source_file_name)

tokens = original_data['Utterances'].values

# convert numbers to text
#tokens = pipeline_convert_numbers(tokens)

# special charachters and spaces
tokens = pipeline_remove_specials_spaces(tokens)

# separate Al arabic char
#tokens = separate_al_char(tokens)

# should be the last step - remove abbreviations 
tokens = pipeline_remove_abbrev(tokens)

# remove duplicates
tokens = pipeline_remove_duplicates_empty(tokens)


data = pd.DataFrame({'Utterances': tokens})
data.head()
data.describe()

# check term frequently 
#df_term_freq = get_term_frequently(data)

# write the file
data.to_csv("101/all-en-cleaned.txt", encoding='utf-8', index=False, header=False)
