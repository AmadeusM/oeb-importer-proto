#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Helper functions to process text.

@author: amagrabi

"""

import re
import dateutil.parser


def date_to_us(date_str):
    '''Convert date to Beveel format.
    
    Args:
        date_str: Input date.
        
    Returns:
        Converted date.
    '''
    date = dateutil.parser.parse(date_str)
    dateformat = '%d-%m-%y %I:%M:%S.%f %p'
    return date.strftime(dateformat)
        

def change_textfile(textfile, changefile):
    '''Converts strings in given textfile based on a dictionary (defined via a separate textfile).
    
    Args:
        textfile: Input textfile.
        changefile: Textfile of a dictionary indicating string transformations.
        
    '''
    changelist = {}
    with open(changefile, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
           (key, val) = line.split(': ')
           changelist[key] = val
        f.close()

    pattern = re.compile(r'\b(' + '|'.join(changelist.keys()) + r')\b')

    with open(textfile, 'r') as f:
        lines = f.readlines()
        f.close()
        
    with open(textfile, 'w') as f:
        for i, line in enumerate(lines):
            lines[i] = pattern.sub(lambda x: changelist[x.group()], line)
        f.writelines(lines)
        f.close()
        