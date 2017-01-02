#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Helper functions for API access.


"""

import config
import os

from api import login, query


def get_prod_name(prod_id, lang='en'):
    '''Get product name by product id.
    
    Args:
        prod_id: prod_id.
        lang: Language of product name (default: en).
        
    Returns:
        Product name (empty string if unavailable).
        
    '''
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    endpoint = os.path.join('products', prod_id)
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    name = ''
    try:
        name = data_json['masterData']['current']['name'][lang]
    except:
        pass
    return name
    

def get_cat_name(cat_id, lang='en'):
    '''Get product name by category id.
    
    Args:
        cat_id: cat_id.
        lang: Language of product name (default: en).
        
    Returns:
        Category name (empty string if unavailable).
        
    '''
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    endpoint = os.path.join('categories', cat_id)
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    name = ''
    try:
        name = data_json['name'][lang]
    except:
        pass
    return name
    
 
def get_categories(prod_id):
    '''Get all categories for a product via a product id.
    
    Args:
        prod_id: prod_id.
        
    Returns:
        List of categories.
        
    '''
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    endpoint = os.path.join('products', prod_id)
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    
    cats = []
    try:
        for cat in data_json['masterData']['current']['categories']:
            cats.append(cat['id'])
    except:
        pass
    
    return cats

    
def get_ancestors(cat_id):
    '''Get all ancestor categories of a target category via a category id.
    
    Args:
        cat_id: cat_id.
        
    Returns:
        List of ancestors.
        
    '''
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    endpoint = os.path.join('categories', cat_id)
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    
    ancs = []
    try:
        for anc in data_json['ancestors']:
            ancs.append(anc['id'])
    except:
        pass
        
    return ancs
    

def get_category_paths(prod_id, output='str', restrict=True):
    '''Get all category paths for a target product via a product id.
    
    Args:
        prod_id: prod_id.
        output: Specifies the output format ('str' or 'dict').
        restrict: If true, only one category path is returned.
        
    Returns:
        Category paths (either as 'str' or 'dict').
        
    '''
    cats_ids = get_categories(prod_id)
    if cats_ids != []:
        cats_names = [get_cat_name(cat_id) for cat_id in cats_ids]
                      
        # Create dictionary that assigns list of ancestors to a category
        ancs_ids = {cat_id: [] for cat_id in cats_ids}
        ancs_names = {cat_name: [] for cat_name in cats_names}
        for cat_id in cats_ids:
            ancs_ids[cat_id] = get_ancestors(cat_id)
            cat_name = get_cat_name(cat_id)
            ancs_names[cat_name] = [get_cat_name(anc_id) for anc_id in ancs_ids[cat_id]]
        
        # Create list of category paths out of dict
        cat_paths = []
        for cat, ancs in ancs_names.items():
            cat_path = []
            for anc in ancs:
                cat_path.append(anc)
            cat_path.append(cat)
            cat_paths.append(cat_path)
            
        cat_paths = [cat_paths[0]] if restrict else cat_paths

        if output == 'str':
            cat_paths_str = ''
            for iter1, cat_path in enumerate(cat_paths):
                if iter1 != 0:
                    cat_paths_str += '; '
                for iter2, cat in enumerate(cat_path):
                    if iter2 == len(cat_path)-1:
                        cat_paths_str += cat
                    else:
                        cat_paths_str += cat + ' > '
            return str(cat_paths_str, 'utf-8')
        elif output == 'dict':
            return ancs_names
        else:
            print('Output format is undefined.')
            
    else:
        if output == 'str':
            return ''
        elif output == 'dict':
            return []
        else:
            print('Output format is undefined.')
                
    
    
