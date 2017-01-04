#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Queries the commercetools API, transforms data into pandas DataFrame, and
exports data to csv (purchases/orders) and xml (product catalog) files.

@author: amagrabi

"""


import pandas as pd
from lxml import etree

import config
import nr
import make_df_full
import api_util
import text

import os
DIR_BASE = os.getcwd()
DIR_UPLOAD = os.path.join(DIR_BASE, 'upload', config.PROJECT_KEY)
if not os.path.exists(DIR_UPLOAD):
    os.mkdir(DIR_UPLOAD)
FILE_CATALOG = os.path.join(DIR_UPLOAD, 'catalog.xml')
FILE_CHANGELIST = os.path.join(DIR_BASE, 'changelist.txt')


def make_csv():
    '''Creates a csv file of all purchases in the Beveel format (shop specified in config.py).

    '''
    
    # Get data via API
    df_orders = make_df_full.orders()
    df_products = make_df_full.products(staged='false')
    
    # Replace anonymous customer id with order id when there are at least 2 products ordered
    ind = df_orders['customerId']=='anonymous'
    order_counts = df_orders.ix[ind, 'orderId'].value_counts()
    order_ids = order_counts[order_counts>1].index.values.tolist()
    
    ind = df_orders['orderId'].isin(order_ids)
    df_orders.ix[ind, 'customerId'] = df_orders.ix[ind, 'orderId']

    # Delete remaining anonymous rows (with <1 products ordered)    
    df_orders = df_orders.ix[df_orders['customerId']!='anonymous']
    df_orders = df_orders.reset_index(drop=True)
    
    # Identify skus from df_products in df_orders
    df_orders['sku'] = -1
    df_orders_new = pd.DataFrame(index=[])
    for i in range(len(df_orders)):
        product_id = df_orders.loc[i, 'productId']
        if product_id in df_products.id.values:
            ind = df_products.id[df_products.id == product_id].index[0]
            df_orders.ix[i, 'sku'] = df_products.ix[ind, 'sku']
            df_orders_new = df_orders_new.append(df_orders.ix[i], ignore_index=True)
    df_orders = df_orders_new

    # Create csv file for purchases
    df_purchases = pd.DataFrame([], 
                                columns=['user_id','order_id','product_id',
                                             'sku_id','date_of_purchase','price',
                                             'sku_currently_in_stock','gender',
                                             'dob','site'])
    
    df_purchases['order_id'] = df_orders['orderId']
    df_purchases['product_id'] = df_orders['productId']
    df_purchases['sku_id'] = df_orders['sku']
    
    df_purchases['date_of_purchase'] = \
        df_orders['createdAt'].apply(lambda x: text.date_to_us(x))

    df_purchases['user_id'] = df_orders['customerId']
    
    df_purchases['price']  = ''
    ind = df_orders['currency']=='USD'
    
    # Convert prices (original prices are in cents)
    df_purchases.ix[ind, 'price'] = df_orders['totalPrice']/100

    df_purchases['sku_currently_in_stock'] = ''
    df_purchases['gender'] = ''
    df_purchases['dob'] = ''
    df_purchases['site'] = ''
    
    FILE_PURCHASES = os.path.join(DIR_UPLOAD, 'purchases.csv')
    df_purchases.to_csv(FILE_PURCHASES)

        
def make_xml(website, verbose=1):
    '''Creates a xml file of the product catalog in the Beveel format (shop specified in config.py).
    
    Args:
        website: Link to the shop website.
        verbose: Flag to print progress in the terminal.
        
    '''
    
    nr_products = nr.products(staged='false')
    df_products = make_df_full.products(nr_products, staged='false')
    
    root = etree.Element('rss')

    channel = etree.SubElement(root, 'channel')
    
    title = etree.SubElement(channel, 'title')
    title.text = config.PROJECT_KEY
    
    link = etree.SubElement(channel, 'link')
    link.text = website
    
    for i in range(len(df_products)):
        
        print('--- Adding products to xml: {} of {} ---'.format(i,len(df_products))) if i%verbose==0 else None

        item = etree.SubElement(channel, 'item')
        
        g_item_group_id = etree.SubElement(item, 'g_item_group_id')
        g_item_group_id.text = df_products.ix[i, 'id']
        
        g_id = etree.SubElement(item, 'g_id')
        g_id.text = df_products.ix[i, 'sku']

        g_title = etree.SubElement(item, 'g_title')
        g_title.text = df_products.ix[i, 'name']

        g_product_type = etree.SubElement(item, 'g_product_type')
        g_product_type.text = api_util.get_category_paths(df_products.ix[i, 'id'], output='str', restrict=True)

        g_brand = etree.SubElement(item, 'g_brand')
        g_brand.text = '' #str(df_products.ix[i, 'brand'])
        
        g_price = etree.SubElement(item, 'g_price')
        price = df_products.ix[i, 'price_us']/100
        price = round(price,2)
        g_price.text = str(price)
        
        g_sale_price = etree.SubElement(item, 'g_sale_price')
        price = df_products.ix[i, 'price_us']/100
        price = round(price,2)
        g_sale_price.text = str(price)
        
        g_availability = etree.SubElement(item, 'g_availability')
        g_availability.text = ''
        
        g_link = etree.SubElement(item, 'g_link')
        g_link.text = ''
        
        g_gender = etree.SubElement(item, 'g_gender')
        g_gender.text = ''
        
        g_image_link = etree.SubElement(item, 'g_image_link')
        g_image_link.text = df_products.ix[i, 'img']
        
        g_installment = etree.SubElement(item, 'g_installment')
        g_months = etree.SubElement(g_installment, 'g_months')
        g_months.text = ''
        g_amount = etree.SubElement(g_installment, 'g_amount')
        g_amount.text = ''
        
#        g_custom_attribute = etree.SubElement(item, 'g_custom_attribute')
        
    
    # Write to file
    tree = etree.ElementTree(root)
    tree.write(FILE_CATALOG, pretty_print=True, xml_declaration=False, encoding='utf-8')

    
    # Make manual changes (special characters)
    with open(FILE_CATALOG, 'r') as f:
        lines = f.readlines()
        f.close()
    
    # Adapt first line
    lines = lines[1:]
    lines[0] = '<rss xmlns:g="http://base.google.com/ns/1.0" version="2.0">\n'
    
    # Convert html in category path
#    for i, line in enumerate(lines):
#        lines[i] = lines[i].replace('&gt;','>')
    
    # Write file
    with open(FILE_CATALOG, 'w') as f:
        f.writelines(lines)
        f.close()
        
    # Workaround: Change field names to colons (g_gender -> g:gender) via 
    # static dictionary file, since lxml library does not seem to support
    # names with colons.
    text.change_textfile(FILE_CATALOG, FILE_CHANGELIST)
    

if __name__ == "__main__":
    make_csv()
    make_xml('www.testshop.com')
    
