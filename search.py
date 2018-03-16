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

MAX_SEARCH = 100

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
        to_search = []
        verified_csv = os.path.join(DIRS['verified'], base + '-verified.csv')
        # Create verified dir if does not exist, and remove any old verified.csv
        if not os.path.exists(DIRS['verified']):
            os.mkdir(DIRS['verified'])
        elif os.path.exists(verified_csv):
            os.unlink(verified_csv)
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            if 'e' in reader.fieldnames:
                account_key = 'e'
            elif 'u' in reader.fieldnames:
                account_key = 'u'
            else:
                # skip file if no email and username fields
                progress('ERROR: missing "e" and "u" headers, skipping', newline=True)
                continue
            if 'p' not in reader.fieldnames:
                # skip file if no password fields
                progress('ERROR: missing "p" header, skipping', newline=True)
                continue
            for row in reader:
                lines_read += 1
                msg = 'lines read: {}  not found: {}'.format(lines_read,
                                                             not_found)
                to_search.append({})
                to_search.append({
                    'query': {
                        'bool': {
                            'must': [
                                {'match': {account_key: row[account_key]}},
                                {'match': {'p': row['p']}}
                            ]
                        }
                    },
                    'terminate_after': 1
                })
                if len(to_search) == MAX_SEARCH:
                    progress(msg + ' searching with {} items...'.format(MAX_SEARCH))
                    non_matching_rows = search(es_client, to_search)
                    for row in non_matching_rows:
                        not_found += 1
                        msg = 'lines read: {}  not found: {}'.format(lines_read,
                                                                     not_found)

                        progress(msg + ' writing rows to csv')
                        write_row(row, verified_csv)
                    to_search = []
                progress(msg)

            # After reading full csv, search for remaining items
            if to_search:
                progress('Searching for {} items...'.format(len(to_search)))
                non_matching_rows = search(es_client, to_search)
                for row in non_matching_rows:
                    not_found += 1
                    write_row(row, verified_csv)

        progress('{} entries were not found'.format(not_found), newline=True)
        print('Moving {} to {}'.format(filename, DIRS['done']))
        move(filename, DIRS['done'])


def search(es_client, to_search):
    non_matching_rows = []
    results = es_client.msearch(to_search)
    for index, result in enumerate(results['responses']):
        if not result['hits']['total']:
            row = []
            for match in to_search[1::2][index]['query']['bool']['must']:
                row.append(match['match'])
            non_matching_rows.append(row)
    return non_matching_rows


def write_row(row, verified_csv):
    if os.path.exists(verified_csv):
        mode = 'a'
    else:
        mode = 'w'
    line = ','.join([field[key] for field in row for key in field.keys()])
    with open(verified_csv, mode) as verified:
        if mode == 'w':
            # Write headers
            verified.write(','.join([key for field in row for key in field.keys()]) + '\n')
        verified.write(line + '\n')


def print_progress(path):
    def progress(data, newline=False):
        filename = os.path.basename(path)
        msg = '{}: {}{}'.format(filename, data, ' ' * 30)
        if newline:
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
