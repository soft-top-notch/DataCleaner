#!/usr/bin/env python
"""Backup files.

Called with filename argument for one or more files. Filename argument can be a\
 list of files or wildcard (*).

Usage:
    databackup.py [-h] --bi SOURCE_PATH DEST_DIR
    databackup.py [-h] --bz BACKUP_PATH
    databackup.py [-h] --z PATH...

Options:
    -h, --help                    This help output
    --bi                          Syncs indexes
    --bz                          Copies zip files
    --z                           Zip list of files

Examples:
    databackup.py -z test.sql
    databackup.py -z ~/samples/*.sql
"""
from __future__ import print_function

import subprocess
import sys
import zipfile

from docopt import docopt
from tqdm import tqdm

from dc import move

AFTER_ZIP_DIR = '../zip_done'
RCLONE_PATH = 'rclone'


def main(args):
    if args['--bi']:
        dest_dir = 'ViperElastic/{}'.format(args['DEST_DIR'])
        rclone('sync', args['SOURCE_PATH'], dest_dir)
    elif args['--bz']:
        rclone('copy', args['BACKUP_PATH'], 'ViperStorage/datadumps')
    elif args['--z']:
        pbar = tqdm(desc='zipping', unit=' files', total=len(args['PATH']))
        for filename in args['PATH']:
            zip(filename)
            pbar.update(1)
        pbar.close()


def rclone(mode, source_path, dest_dir):
    dest_path = 'b2:{}'.format(dest_dir)
    print('Using rclone to {} {} to {}\n'.format(mode, source_path, dest_path))
    command = [RCLONE_PATH, '-v', mode, source_path, dest_path]
    try:
        output = subprocess.check_output(
            ' '.join(command), stderr=subprocess.STDOUT, shell=True)
        print(output)
    except subprocess.CalledProcessError as e:
        print('ERROR! CMD USED: {}'.format(' '.join(command)))
        sys.exit(e.output)


def zip(filename):
    with zipfile.ZipFile(
            filename + '.zip', 'w', zipfile.ZIP_DEFLATED,
            allowZip64=True) as z:
        z.write(filename)
    move(filename, AFTER_ZIP_DIR)


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__)
    main(args)
