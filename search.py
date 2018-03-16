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
    databackup.py test.csv
    databackup.py ~/samples/*.csv
"""
from __future__ import print_function

import csv
import os
import sys

from docopt import docopt
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from datacleaner import gather_files


ES_TIMEOUT = 30
ES_MAX_RETRIES = 10
ES_RETRY_ON_TIMEOUT = True

def main(args):
    es_client = Elasticsearch('{}:{}'.format(args['--host'], args['--port']),
                              timeout=ES_TIMEOUT,
                              max_retries=ES_MAX_RETRIES,
                              retry_on_timeout=ES_RETRY_ON_TIMEOUT)
    file_list = gather_files(args['PATH'])
    for filename in file_list:
        progress = print_progress(filename)
        progress('processing...')
        base = filename.rstrip('.csv')
        lines_read = 0
        matches_found = 0
        success_csv = base + '-success.csv'
        if os.path.exists(success_csv):
            os.unlink(success_csv)
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            if 'u' not in reader.fieldnames or 'p' not in reader.fieldnames:
                # skip file if no username and password fields
                progress('ERROR: missing "u" or "p" headers, or both', end=True)
                continue
            for row in reader:
                lines_read += 1
                if is_in_es(es_client, row['u'], row['p']):
                    matches_found += 1
                    write_row(reader.fieldnames, row, success_csv)
                msg = 'lines read: {}  matches found: {}'.format(lines_read, matches_found)
                progress(msg)
        progress('{} matches were found'.format(matches_found), end=True)


def write_row(keys, row, success_csv):
    if os.path.exists(success_csv):
        mode = 'a'
    else:
        mode = 'w'
    line = ','.join([row[key] for key in keys])
    with open(success_csv, mode) as success:
        if mode == 'w':
            # Write headers
            success.write(','.join(keys) + '\n')
        success.write(line + '\n')


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
