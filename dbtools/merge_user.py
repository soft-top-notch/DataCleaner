#!/usr/bin/env python2
"""Add username column after userid in CSV files.

Called with CSV filename argument with users and one or more CSV files.
Will output new CSV file for each file.

CSV filename argument can be a list of files or wildcard (*).

Usage:
    merge_user.py [-hV] [--exit-on-error] USERFILE CSVFILES...

Options:
    --exit-on-error               Exit on error, do not continue
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    merge_user.py users.csv posts.csv
"""
from __future__ import division, print_function

import csv
import os
import re
import sys

from docopt import docopt

__version__ = '0.1.0'
__license__ = """
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

id_re = re.compile(r'^((?:user|member)_?id|id_?(?:user|member))$', re.I)
name_re = re.compile(r'^((?:user|member)_?name|name_?(?:user|member))$', re.I)


def main(args):
    """Executes main code."""
    users, column_name = read_users(args['USERFILE'])
    if not users:
        sys.exit(1)

    for filepath in args['CSVFILES']:
        try:
            merge_users(filepath, users, column_name)
        except KeyboardInterrupt:
            print('Control-C pressed...')
            sys.exit(138)
        except Exception as error:
            if args['--exit-on-error']:
                raise
            else:
                print('{} ERROR:{}'.format(filepath, error))


def read_users(filepath):
    """Read userid and username from CSV file."""
    users = {}
    with open(filepath, 'rb') as csvfile:
        reader = csv.reader(csvfile, quotechar='\\')
        fieldnames = next(reader)

        ids = filter(id_re.match, fieldnames)
        if not ids:
            print('ERROR: Column userid not found in file {}'.format(filepath))
            return None, None
        id_no = fieldnames.index(ids[0])

        names = filter(name_re.match, fieldnames)
        if not names:
            print('ERROR: Column username not found in file {}'
                  .format(filepath))
            return None, None
        name_no = fieldnames.index(names[0])

        for row in reader:
            row = fix_comma(row)
            users[row[id_no]] = row[name_no]

    return users, names[0]


def merge_users(filepath, users, column_name):
    """Add username column after userid in new CSV file."""
    with open(filepath, 'rb') as infile:
        reader = csv.reader(infile, quotechar='\\')
        fieldnames = next(reader)

        names = filter(name_re.match, fieldnames)
        if names:
            print('WARN: Column username already exists in file {}'
                  .format(filepath))
            return

        ids = filter(id_re.match, fieldnames)
        if not ids:
            print('WARN: Column userid not found in file {}'.format(filepath))
            return
        id_no = fieldnames.index(ids[0])

        basepath = os.path.splitext(filepath)[0]
        with open(basepath + '.merged.csv', 'wb') as outfile:
            fieldnames.insert(id_no + 1, column_name)
            outfile.write(','.join(fieldnames) + '\n')

            for row in reader:
                row = fix_comma(row)
                username = users.get(row[id_no])
                if username is None:
                    username = ''
                    users[row[id_no]] = username
                    print('WARN: Username not found for userid={}'
                          .format(row[id_no]))
                row.insert(id_no + 1, username)
                outfile.write(','.join(row) + '\n')


def fix_comma(row):
    result = []
    quote = None
    for value in row:
        if quote:
            result[-1] += value
            if len(value) and value[-1] == quote:
                quote = None
        else:
            result.append(value)
            if len(value) and value[0] in ("'", '"'):
                quote = value[0]
                if value[-1] == quote:
                    quote = None
                else:
                    result[-1] += ','
    return result


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
