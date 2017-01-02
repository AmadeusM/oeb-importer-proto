#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Queries the commercetools API to creates DataFrames from json-formatted data 
of the following objects:
    
    - Products (from Product Projections, default staged='false')
    - Customers
    - Orders
    - Categories
    
@author: amagrabi


"""

import pandas as pd
import numpy as np

import config
from api import login, query

# Size of data entries that one query will display (max: 500) 
size_chunks = 500


def products(nr_items, staged='false', offset=0):
    '''Queries the commercetools API to create a DataFrame of products.
    
    Args:
        nr_items: Maximum number of retrieved items.
        staged: Flag to get staged or non-staged items.
        offset: offset of retrieved items (i.e. offset=5 will omit the first 6 items).
        
    Returns:
        DataFrame of products.
        
    '''
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    
    if nr_items <= 0:
        raise Exception("'nr_items' has to be larger than 0.")
    if staged not in ['true','false']:
        raise Exception("'staged' has to be either 'true' or 'false'.")
    
    cols = ['id','name','name_de','name_first','img','createdAt',
            'categoryIds','categoryNamesUnique',
            'price','priceCurrency','price_us','price_ger',
            'desc','color','size','tags','brand','name_fromshop',
            'sku']
            
    df = pd.DataFrame(index=[], columns=cols)
    limit = nr_items if nr_items <= size_chunks else size_chunks
    
    while True:
        
        print('Loading products chunk (offset: {}, chunk size = {}, nr = {})'.format(offset, size_chunks, nr_items))
        
        endpoint = "product-projections?limit=%s&offset=%s&staged=%s"  % (limit, offset, staged)
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        
        df_chunk = pd.DataFrame(index=[], columns=cols)
        results = data_json['results']

        for i in range(len(results)):
            df_chunk.loc[i, 'id'] = results[i]['id']
            df_chunk.loc[i, 'createdAt'] = results[i]['createdAt']

            try:
                df_chunk.loc[i, 'name'] = results[i]['name']['en']
            except:
                df_chunk.loc[i, 'name'] = ''
            try:
                df_chunk.loc[i, 'name_de'] = results[i]['name']['de']
            except:
                df_chunk.loc[i, 'name_de'] = ''
            try:
                df_chunk.loc[i, 'name_first'] = next(iter(results[i]['name'].values()))
            except:
                df_chunk.loc[i, 'name_first'] = ''

            if not results[i]['masterVariant']['prices']:
                df_chunk.loc[i, 'price'] = ''
            else:
                df_chunk.loc[i, 'price'] = results[i]['masterVariant']['prices'][0]['value']['centAmount']

            # Find US price
            try:
                for price in results[i]['masterVariant']['prices']:
                    if price['country'] == 'US':
                        df_chunk.loc[i, 'price_us'] = price['value']['centAmount']
            except:
                df_chunk.loc[i, 'price_us'] = ''
                
            # Find GER price
            try:
                for price in results[i]['masterVariant']['prices']:
                    if price['country'] == 'GER':
                        df_chunk.loc[i, 'price_ger'] = price['value']['centAmount']
            except:
                df_chunk.loc[i, 'price_ger'] = ''
                

            if not results[i]['masterVariant']['prices']:
                df_chunk.loc[i, 'priceCurrency'] = ''
            else:
                df_chunk.loc[i, 'priceCurrency'] = results[i]['masterVariant']['prices'][0]['value']['currencyCode']

            try:
                df_chunk.loc[i, 'sku'] = results[i]['masterVariant']['sku']
            except:
                df_chunk.loc[i, 'sku'] = ''

            cats = results[i]['categories']
            cats_list = ['']*len(cats)
            for j in range(len(cats)):
                cats_list[j] = cats[j]['id']
            df_chunk.loc[i, 'categoryIds'] = cats_list
                
            try:
                df_chunk.loc[i, 'img'] = results[i]['masterVariant']['images'][0]['url']
            except:
                df_chunk.loc[i, 'img'] = ''

            # Find attributes
            attributes = []
            for j in results[i]['masterVariant']['attributes']:
                attributes.append(j['name'])
                
        if nr_items <= size_chunks:
            df = df.append(df_chunk, ignore_index=True)
            return df
        else:
            nr_items -= size_chunks
            offset += size_chunks
            limit = nr_items if nr_items <= size_chunks else size_chunks
            df = df.append(df_chunk, ignore_index=True)
            

def customers(nr_items, offset=0):
    '''Queries the commercetools API to create a DataFrame of customers.
    
    Args:
        nr_items: Maximum number of retrieved items.
        offset: offset of retrieved items (i.e. offset=5 will omit the first 6 items).
        
    Returns:
        DataFrame of customers.
        
    '''
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    
    if nr_items <= 0:
        raise Exception("'nr_items' has to be larger than 0.")
    
    cols = ['id']
    df = pd.DataFrame(index=[], columns=cols)
    limit = nr_items if nr_items <= size_chunks else size_chunks
    
    while True:
        
        print('Loading customers chunk (offset: {}, chunk size = {}, nr = {})'.format(offset, size_chunks, nr_items))
        
        endpoint = "customers?limit=%s&offset=%s"  % (limit, offset)
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        
        df_chunk = pd.DataFrame(index=[], columns=cols)
        df_chunk['id'] = [d['id'] for d in data_json['results']]

        if nr_items <= size_chunks:
            
            df = df.append(df_chunk, ignore_index=True)
            return df
            
        else:
            
            nr_items -= size_chunks
            offset += size_chunks
            limit = nr_items if nr_items <= size_chunks else size_chunks
            df = df.append(df_chunk, ignore_index=True)

    
def orders(nr_items, offset=0):
    '''Queries the commercetools API to create a DataFrame of orders.
    
    Args:
        nr_items: Maximum number of retrieved items.
        offset: offset of retrieved items (i.e. offset=5 will omit the first 6 items).
        
    Returns:
        DataFrame of orders.
        
    '''
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    
    if nr_items <= 0:
        raise Exception("'nr_items' has to be larger than 0.")

    cols = ['productId','customerId','orderId','createdAt','totalPrice','currency','channelId']
    df = pd.DataFrame([], columns=cols)
    limit = nr_items if nr_items <= size_chunks else size_chunks
    
    while True:
        
        print('Loading orders chunk (offset: {}, chunk size = {}, nr = {})'.format(offset, size_chunks, nr_items))
        
        endpoint = "orders?limit=%s&offset=%s"  % (limit, offset)
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        nr_results = len(data_json['results'])
        
        df_chunk = pd.DataFrame(np.zeros((nr_results, len(cols))), columns=cols)
        
        counter = 0
        for i in range(nr_results):
            
            nr_items_per_order = len(data_json['results'][i]['lineItems'])
            
            for j in range(nr_items_per_order):

                df_chunk.loc[counter, 'orderId'] = data_json['results'][i]['id']
                df_chunk.loc[counter, 'productId'] = data_json['results'][i]['lineItems'][j]['productId']
                df_chunk.loc[counter, 'createdAt'] = data_json['results'][i]['createdAt']
                df_chunk.loc[counter, 'totalPrice'] = data_json['results'][i]['totalPrice']['centAmount']
                df_chunk.loc[counter, 'currency'] = data_json['results'][i]['totalPrice']['currencyCode']

                try:
                    df_chunk.loc[counter, 'customerId'] = data_json['results'][i]['customerId']
                except KeyError:
                    df_chunk.loc[counter, 'customerId'] = 'anonymous'
                
                counter += 1

        if nr_items <= size_chunks:
            
            df = df.append(df_chunk, ignore_index=True)
            return df
            
        else:
            nr_items -= size_chunks
            offset += size_chunks
            limit = nr_items if nr_items <= size_chunks else size_chunks
            df = df.append(df_chunk, ignore_index=True)
            

def categories(nr_items, offset=0):
    '''Queries the commercetools API to create a DataFrame of categories.
    
    Args:
        nr_items: Maximum number of retrieved items.
        
    Returns:
        DataFrame of categories.
        
    '''
    
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY, 
                 config.SCOPE, config.HOST)
    
    if nr_items <= 0:
        raise Exception("'nr_items' has to be larger than 0.")
    
    cols = ['id', 'name', 'name_en', 'name_de','name_first']
    df = pd.DataFrame(index=[], columns=cols)
    limit = nr_items if nr_items <= size_chunks else size_chunks
    
    while True:
        
        print('Loading categories chunk (offset: {}, chunk size = {}, nr = {})'.format(offset, size_chunks, nr_items))
        
        endpoint = "categories?limit=%s&offset=%s"  % (limit, offset)
        data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
        results = data_json['results']
        
        df_chunk = pd.DataFrame(index=[], columns=cols)
        df_chunk['id'] = [d['id'] for d in results]

        for i in range(len(results)):
                    
            try:
                df_chunk.loc[i, 'name'] = results[i]['name']['en']
            except:
                df_chunk.loc[i, 'name'] = ''
            try:
                df_chunk.loc[i, 'name_en'] = results[i]['name']['en']
            except:
                df_chunk.loc[i, 'name_en'] = ''
            try:
                df_chunk.loc[i, 'name_de'] = results[i]['name']['de']
            except:
                df_chunk.loc[i, 'name_de'] = ''
            try:
                df_chunk.loc[i, 'name_first'] = next(iter(results[i]['name'].values()))
            except:
                df_chunk.loc[i, 'name_first'] = ''
                

        if nr_items <= size_chunks:
            
            df = df.append(df_chunk, ignore_index=True)
            return df
            
        else:
            
            nr_items -= size_chunks
            offset += size_chunks
            limit = nr_items if nr_items <= size_chunks else size_chunks
            df = df.append(df_chunk, ignore_index=True)
            
