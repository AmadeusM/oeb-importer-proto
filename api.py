#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Functions to make API calls.

@author: amagrabi

"""

import requests


def login(client_id, client_secret, project_key, scope, host = 'EU'):
    '''Authentification
    
    Args:
        client_id: client_id.
        client_secret: client_secret.
        project_key: project_key.
        scope: Scope of access (read, write, etc.).
        host: 'EU' or 'NA'.
        
    Returns:
        Authentification data.
        
    '''
    headers = { 'Content-Type' : 'application/x-www-form-urlencoded' }
    body = "grant_type=client_credentials&scope=%s" % scope
    if host == 'EU':
        url = "https://auth.sphere.io/oauth/token"
    elif host == 'US':
        url = "https://auth.commercetools.co/oauth/token"
    else:
        raise Exception("Host is unknown (has to be 'EU' or 'US').")
    auth = (client_id, client_secret)
    r = requests.post(url, data=body, headers=headers, auth=auth)
    if r.status_code is 200:
        return r.json()
    else:
        raise Exception("Failed to get an access token. Are you sure you have added them to config.py?")

        
def query(endpoint, project_key, auth, host = 'EU'):
    '''Fetch Data via API into Json-Format
    
    Args:
        endpoint: API endpoint (products, orders, etc.).
        project_key: project_key.
        auth: Login data.
        host: 'EU' or 'NA'.
        
    Returns:
        Query output in json.
        
    '''
    headers = { "Authorization" : "Bearer %s" % auth["access_token"] }
    if host == 'EU':
        url = "https://api.sphere.io/%s/%s" % (project_key, endpoint)
    elif host == 'US':
        url = "https://api.commercetools.co/%s/%s" % (project_key, endpoint)
    else:
        raise Exception("Host is unknown (has to be 'EU' or 'US').")
    r = requests.get(url, headers=headers)
    data_json = r.json()    # json-format as nested dict-/list-structure
    return data_json