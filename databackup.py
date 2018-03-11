#!/usr/bin/env python
"""Backup files.

Called with filename argument for one or more files. Filename argument can be a\
 list of files or wildcard (*).

Usage:
    databackup.py [-hz] PATH...

Options:
    -h, --help                    This help output
    -z, --zip                     Zip list of files

Examples:
    databackup.py -z test.sql
    databackup.py -z ~/samples/*.sql
"""
from __future__ import print_function

import os
import zipfile

from docopt import docopt


def main(args):
    if args['--zip']:
        for filename in args['PATH']:
            zip(filename)
    else:
        print('Nothing to do. Exiting...')


def zip(filename):
    with zipfile.ZipFile(filename + '.zip', 'w') as z:
        z.write(filename)
    os.unlink(filename)


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__)
    main(args)
