#!/usr/bin/env python
"""Search usernames and passwords from CSVs in Elasticsearch.

Called with filename argument for one or more files. Filename argument can be a\
 list of files or wildcard (*).

CSV must have headers defined and shortened.

Usage:
    search.py [-h] [--host HOST] [--port PORT] PATH...

Options:
    -h, --help                    This help output
    --host HOST                   Elasticsearch host [default: localhost]
    --port PORT                   Elasticsearch port [default: 9200]

Examples:
    search.py test.csv
    search.py ~/samples/*.csv
    search.py --host localhost --port 9200 *.csv
"""
from __future__ import print_function

import csv
import os
import sys

from docopt import docopt
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from datacleaner import gather_files, move


ES_CONFIG = {
    'timeout': 30,
    'max_retries': 10,
    'retry_on_timeout': True
}

DIRS = {
    'verified': '../verified',
    'done': '../done'
}

def main(args):
    es_client = Elasticsearch('{}:{}'.format(args['--host'], args['--port']),
                              timeout=ES_CONFIG['timeout'],
                              max_retries=ES_CONFIG['max_retries'],
                              retry_on_timeout=ES_CONFIG['retry_on_timeout'])
    file_list = gather_files(args['PATH'])
    for filename in file_list:
        progress = print_progress(filename)
        progress('processing...')
        base = filename.rstrip('.csv')
        lines_read = 0
        not_found = 0
        verified_csv = os.path.join(DIRS['verified'], base + '-verified.csv')
        # Create verified dir if does not exist, and remove any old verified.csv
        if not os.path.exists(DIRS['verified']):
            os.mkdir(DIRS['verified'])
        elif os.path.exists(verified_csv):
            os.unlink(verified_csv)
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            if 'u' not in reader.fieldnames or 'p' not in reader.fieldnames:
                # skip file if no username and password fields
                progress('ERROR: missing "u" or "p" headers, or both', end=True)
                continue
            for row in reader:
                lines_read += 1
                if not is_in_es(es_client, row['u'], row['p']):
                    not_found += 1
                    write_row(reader.fieldnames, row, verified_csv)
                msg = 'lines read: {}  not found: {}'.format(lines_read,
                                                             not_found)
                progress(msg)
        progress('{} were not found'.format(not_found), end=True)
        print('Moving {} to {}'.format(filename, DIRS['done']))
        move(filename, DIRS['done'])


def write_row(keys, row, verified_csv):
    if os.path.exists(verified_csv):
        mode = 'a'
    else:
        mode = 'w'
    line = ','.join([row[key] for key in keys])
    with open(verified_csv, mode) as verified:
        if mode == 'w':
            # Write headers
            verified.write(','.join(keys) + '\n')
        verified.write(line + '\n')


def is_in_es(es_client, u, p):
    return Search(using=es_client) \
        .query("match", u=u) \
        .query("match", p=p) \
        .count()


def print_progress(path):
    def progress(data, end=False):
        filename = os.path.basename(path)
        msg = '{}: {}{}'.format(filename, data, ' ' * 30)
        if end:
            print(msg)
        else:
            msg += '\r\r'
            sys.stdout.write(msg)
            sys.stdout.flush()

    return progress


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__)
    main(args)
