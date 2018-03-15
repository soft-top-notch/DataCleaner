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

from docopt import docopt
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from datacleaner import gather_files


def main(args):
    es_client = Elasticsearch('{}:{}'.format(args['--host'], args['--port']))
    file_list = gather_files(args['PATH'])
    for filename in file_list:
        base = filename.rstrip('.csv')
        success_csv = base + '-success.csv'
        if os.path.exists(success_csv):
            os.unlink(success_csv)
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            if 'u' not in reader.fieldnames or 'p' not in reader.fieldnames:
                # skip file if no username and password fields
                continue
            for row in reader:
                if is_in_es(es_client, row['u'], row['p']):
                    write_row(reader.fieldnames, row, success_csv)


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


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__)
    main(args)
