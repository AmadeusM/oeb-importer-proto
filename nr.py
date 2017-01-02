#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Get the number of available items in a shop via the commercetools API.

@author: amagrabi

"""

import config
from api import login, query


def nr_products(staged=False):
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY,
                 config.SCOPE, config.HOST)
    endpoint = "product-projections?offset=0&staged=%s"   % staged
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    return data_json['total']


def nr_customers():
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY,
                 config.SCOPE, config.HOST)
    endpoint = "customers?&offset=0"
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    return data_json['total']


def nr_orders():
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY,
                 config.SCOPE, config.HOST)
    endpoint = "orders?&offset=0"
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    return data_json['total']


def nr_categories():
    auth = login(config.CLIENT_ID, config.CLIENT_SECRET, config.PROJECT_KEY,
                 config.SCOPE, config.HOST)
    endpoint = "categories?&offset=0"
    data_json = query(endpoint, config.PROJECT_KEY, auth, config.HOST)
    return data_json['total']