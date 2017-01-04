#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Queries the commercetools API to creates DataFrames from json-formatted data 
of the following objects:
    
    - Products (from Product Projections, default staged='false')
    - Customers
    - Orders
    - Categories
    
Functions always return the whole available data. 
For querying specific subsets, use functions in make_df.py.
    
@author: amagrabi


"""

import pandas as pd

import config
from api import login, query
from api_util import get_product_price


def products(staged='false', size_chunks=250, 
             languages=['en','de'], currencies=['USD','EUR'],
             verbose=True):
    '''Queries the commercetools API to create a DataFrame of products.
    
    Args:
        nr_items: Maximum number of retrieved items.
        staged: Flag to get staged or non-staged items.
        offset: offset of retrieved items (i.e. offset=5 will omit the first 6 items).
        
    Returns:
        DataFrame of products.
        
    '''
    
    if staged not in ['true','false']:
        raise Exception('Parameter staged has to be either true or false.')
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)   
    
    cols = ['id','sku','categoryIds','img','createdAt']
    
    # Language-dependent variables
    ld_vars = ['name', 'slug', 'description']
    [cols.append(ld_var + '_' + language) for ld_var in ld_vars for language in languages]
    # Currency-dependent variables
    cd_vars = ['price']
    [cols.append(cd_var + '_' + currency) for cd_var in cd_vars for currency in currencies]
    
    df = pd.DataFrame(index=[], columns=cols)
    
    last_id = None
    proceed = True
    progress = 0

    while(proceed):
        
        if last_id == None:
            endpoint = 'product-projections?limit={}&sort=id&staged={}'.format(size_chunks, staged)
        else:
            endpoint = ('product-projections?limit={}&sort=id&staged={}&where=id%3E%22' + last_id + '%22').format(size_chunks, staged)
            
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        df_chunk = pd.DataFrame(index=[], columns=cols)
        results = data_json['results']

        proceed = len(results)==size_chunks
        last_id = results[-1]['id']

        progress += size_chunks
        if verbose:
            print('Loading products chunk (imported: {}, chunk size = {})'.format(progress, size_chunks))
        
        for i, product in enumerate(results):
            
            # Mandatory fields
            df_chunk.loc[i, 'id'] = product['id']
            df_chunk.loc[i, 'createdAt'] = product['createdAt']

            # Optional fields
            try:
                df_chunk.loc[i, 'sku'] = product['masterVariant']['sku']
            except:
                df_chunk.loc[i, 'sku'] = ''
                
            try:
                df_chunk.loc[i, 'img'] = product['masterVariant']['images'][0]['url']
            except:
                df_chunk.loc[i, 'img'] = ''
            
            # Language-dependent variables
            for ld_var in ld_vars:
                for language in languages:
                    var = ld_var + '_' + language
                    try:
                        df_chunk.loc[i, var] = product[ld_var][language]
                    except:
                        df_chunk.loc[i, var] = ''
                    
            # Currency-dependent variables
            for cd_var in cd_vars:
                for currency in currencies:
                    var = cd_var + '_' + currency
                    df_chunk.loc[i, var] = get_product_price(product['masterVariant']['prices'], currency)

            # Categories
            cats_json = product['categories']
            cats_list = []
            for cat_json in cats_json:
                cats_list.append(cat_json['id'])
            df_chunk.loc[i, 'categoryIds'] = cats_list
        
        # Append chunk to DataFrame     
        df = df.append(df_chunk, ignore_index=True)
            
    return df


def customers(size_chunks=250, verbose=True):
    '''Queries the commercetools API to create a DataFrame of customers.
    
    Args:
        nr_items: Maximum number of retrieved items.
        offset: offset of retrieved items (i.e. offset=5 will omit the first 6 items).
        
    Returns:
        DataFrame of customers.
        
    '''
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    
    cols = ['id', 'firstName', 'middleName', 'lastName', 'email', 
            'dateOfBirth', 'companyName', 
            'customerGroup_ids', 'customerGroup_names']
    df = pd.DataFrame(index=[], columns=cols)
    
    last_id = None
    proceed = True
    progress = 0

    while(proceed):
        
        if last_id == None:
            endpoint = 'customers?limit={}&sort=id'.format(size_chunks)
        else:
            endpoint = ('customers?limit={}&sort=id&where=id%3E%22' + last_id + '%22').format(size_chunks)
            
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        df_chunk = pd.DataFrame(index=[], columns=cols)
        results = data_json['results']

        proceed = len(results)==size_chunks
        last_id = results[-1]['id']

        progress += size_chunks
        if verbose:
            print('Loading customers chunk (imported: {}, chunk size = {})'.format(progress, size_chunks))

        for i, customer in enumerate(results):
            
            # Mandatory fields
            df_chunk.loc[i, 'id'] = customer['id']
            df_chunk.loc[i, 'createdAt'] = customer['createdAt']

            # Optional fields
            try:
                df_chunk.loc[i, 'firstName'] = customer['firstName']
            except:
                df_chunk.loc[i, 'firstName'] = ''
                
            try:
                df_chunk.loc[i, 'lastName'] = customer['lastName']
            except:
                df_chunk.loc[i, 'lastName'] = ''
                
            try:
                df_chunk.loc[i, 'middleName'] = customer['middleName']
            except:
                df_chunk.loc[i, 'middleName'] = ''
                
            try:
                df_chunk.loc[i, 'email'] = customer['email']
            except:
                df_chunk.loc[i, 'email'] = ''
                
            try:
                df_chunk.loc[i, 'dateOfBirth'] = customer['dateOfBirth']
            except:
                df_chunk.loc[i, 'dateOfBirth'] = ''
                
            try:
                df_chunk.loc[i, 'companyName'] = customer['companyName']
            except:
                df_chunk.loc[i, 'companyName'] = ''
                
            
            # Customer groups
            try:
                groups_json = customer['customerGroup']
                groups_ids = []
                groups_names = []
                for group_json in groups_json:
                    try:
                        groups_ids.append(group_json['id'])
                    except:
                        None
                    try:
                        groups_names.append(group_json['name'])
                    except:
                        None
                df_chunk.loc[i, 'customerGroup_ids'] = groups_ids
                df_chunk.loc[i, 'customerGroup_names'] = groups_names
            except:
                df_chunk.loc[i, 'customerGroup_ids'] = ''
                df_chunk.loc[i, 'customerGroup_names'] = ''
            
        # Append chunk to DataFrame     
        df = df.append(df_chunk, ignore_index=True)
            
    return df

    
def orders(size_chunks=250, languages=['en','de'], verbose=True):
    '''Queries the commercetools API to create a DataFrame of orders.
    
    Args:
        nr_items: Maximum number of retrieved items.
        offset: offset of retrieved items (i.e. offset=5 will omit the first 6 items).
        
    Returns:
        DataFrame of orders.
        
    '''
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)

    cols = ['productId','customerId','customerEmail','anonymousId','orderId',
            'createdAt','productPrice','totalPrice','currency','quantity',
            'country']
            
    # Language-dependent variables
    ld_vars = ['name']
    [cols.append(ld_var + '_' + language) for ld_var in ld_vars for language in languages]
    
    df = pd.DataFrame([], columns=cols)
    
    last_id = None
    proceed = True
    progress = 0

    while(proceed):
        
        if last_id == None:
            endpoint = 'orders?limit={}&sort=id'.format(size_chunks)
        else:
            endpoint = ('orders?limit={}&sort=id&where=id%3E%22' + last_id + '%22').format(size_chunks)
            
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        df_chunk = pd.DataFrame(index=[], columns=cols)
        results = data_json['results']

        proceed = len(results)==size_chunks
        last_id = results[-1]['id']

        progress += size_chunks
        if verbose:
            print('Loading orders chunk (imported: {}, chunk size = {})'.format(progress, size_chunks))
        
        counter = 0
        
        for i, order in enumerate(results):
            
            nr_items = len(order['lineItems'])
            
            for j in range(nr_items):
                    
                # Mandatory fields
                df_chunk.loc[counter, 'orderId'] = order['id']
                df_chunk.loc[counter, 'productId'] = order['lineItems'][j]['productId']
                df_chunk.loc[counter, 'createdAt'] = order['createdAt']
                df_chunk.loc[counter, 'totalPrice'] = order['totalPrice']['centAmount']
                df_chunk.loc[counter, 'currency'] = order['totalPrice']['currencyCode']

                # Optional fields
                try:
                    df_chunk.loc[counter, 'customerId'] = order['customerId']
                except KeyError:
                    df_chunk.loc[counter, 'customerId'] = 'anonymous'
                    
                try:
                    df_chunk.loc[counter, 'customerEmail'] = order['customerEmail']
                except KeyError:
                    df_chunk.loc[counter, 'customerEmail'] = ''
                    
                try:
                    df_chunk.loc[counter, 'anonymousId'] = order['anonymousId']
                except KeyError:
                    df_chunk.loc[counter, 'anonymousId'] = ''
                    
                try:
                    df_chunk.loc[counter, 'country'] = order['country']
                except KeyError:
                    df_chunk.loc[counter, 'country'] = ''
                    
                try:
                    df_chunk.loc[counter, 'productPrice'] = order['lineItems'][j]['price']['value']['centAmount']
                except:
                    df_chunk.loc[counter, 'productPrice'] = ''
                    
                try:
                    df_chunk.loc[counter, 'currency'] = order['lineItems'][j]['price']['value']['currencyCode']
                except:
                    df_chunk.loc[counter, 'currency'] = ''
                    
                try:
                    df_chunk.loc[counter, 'quantity'] = order['lineItems'][j]['quantity']
                except:
                    df_chunk.loc[counter, 'quantity'] = ''
                    
                # Language-dependent variables
                for ld_var in ld_vars:
                    for language in languages:
                        var = ld_var + '_' + language
                        try:
                            df_chunk.loc[i, var] = order['lineItems'][j][ld_var][language]
                        except:
                            df_chunk.loc[i, var] = ''
                
                counter += 1

        # Append chunk to DataFrame     
        df = df.append(df_chunk, ignore_index=True)
            
    return df
            

def categories(size_chunks=250, languages=['en','de'], verbose=True):
    '''Queries the commercetools API to create a DataFrame of categories.
    
    Args:
        nr_items: Maximum number of retrieved items.
        
    Returns:
        DataFrame of categories.
        
    '''
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    
    cols = ['id','createdAt']

    # Language-dependent variables
    ld_vars = ['name','slug','description']
    [cols.append(ld_var + '_' + language) for ld_var in ld_vars for language in languages]
    
    df = pd.DataFrame(index=[], columns=cols)
    
    last_id = None
    proceed = True
    progress = 0

    while(proceed):
        
        if last_id == None:
            endpoint = 'categories?limit={}&sort=id'.format(size_chunks)
        else:
            endpoint = ('categories?limit={}&sort=id&where=id%3E%22' + last_id + '%22').format(size_chunks)
            
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        df_chunk = pd.DataFrame(index=[], columns=cols)
        results = data_json['results']

        proceed = len(results)==size_chunks
        last_id = results[-1]['id']

        progress += size_chunks
        if verbose:
            print('Loading categories chunk (imported: {}, chunk size = {})'.format(progress, size_chunks))
        
        for i, category in enumerate(results):
            
            # Mandatory fields
            df_chunk.loc[i, 'id'] = category['id']
            df_chunk.loc[i, 'createdAt'] = category['createdAt']
                    
            # Language-dependent variables
            for ld_var in ld_vars:
                for language in languages:
                    var = ld_var + '_' + language
                    try:
                        df_chunk.loc[i, var] = category[ld_var][language]
                    except:
                        df_chunk.loc[i, var] = ''
                
        # Append chunk to DataFrame     
        df = df.append(df_chunk, ignore_index=True)
            
    return df
            
