#!/usr/bin/env python2.7
"""Add username column after userid in CSV files.

Called with CSV filename argument with users and one or more CSV files.
Will output new CSV file for each file.

CSV filename argument can be a list of files or wildcard (*).

Usage:
    merge_user.py [-hV] [--exit-on-error] USERFILE CSVFILE...

Options:
    --exit-on-error               Exit on error, do not continue
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    full_db.py users.csv posts.csv
"""
import os
import re
import shlex
import sys

from docopt import docopt

from dc import c_error, c_warning

__version__ = '0.5.0'
__license__ = """
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

# How many bytes to read at a time
READ_BUFFER = 10485760

id_re = re.compile(r'^((?:user|member)_?id|id_?(?:user|member))$', re.I)
name_re = re.compile(r'^((?:user|member)_?name|name_?(?:user|member))$', re.I)


def main(args):
    """Executes main code."""
    users, column_name = read_users(args['USERFILE'])
    if not users:
        sys.exit(1)

    for filepath in args['CSVFILE']:
        try:
            merge_users(filepath, users, column_name)
        except KeyboardInterrupt:
            print('Control-C pressed...')
            sys.exit(138)
        except Exception as error:
            if args['--exit-on-error']:
                raise
            else:
                c_error('{} ERROR:{}'.format(filepath, error))


def read_users(filepath):
    """Read userid and username from CSV file."""
    users = {}
    with open(filepath, 'rb') as csvfile:
        fieldnames = parse_row(csvfile.readline())

        ids = filter(id_re.match, fieldnames)
        if not ids:
            c_error('Column userid not found in file {}'.format(filepath))
            return None, None
        id_no = fieldnames.index(ids[0])

        names = filter(name_re.match, fieldnames)
        if not names:
            c_error('Column username not found in file {}'.format(filepath))
            return None, None
        name_no = fieldnames.index(names[0])

        for line in csvfile.readlines():
            row = parse_row(line)
            users[row[id_no]] = row[name_no]

    return users, names[0]


def merge_users(filepath, users, column_name):
    """Add username column after userid in new CSV file."""
    with open(filepath, 'rb') as infile:
        fieldnames = parse_row(infile.readline())

        names = filter(name_re.match, fieldnames)
        if names:
            c_warning('Column username already exists in file {}'
                .format(filepath))
            return

        ids = filter(id_re.match, fieldnames)
        if not ids:
            c_warning('Column userid not found in file {}'.format(filepath))
            return
        id_no = fieldnames.index(ids[0])

        basepath = os.path.splitext(filepath)[0]
        with open(basepath + '.merged.csv', 'wb') as outfile:
            fieldnames.insert(id_no + 1, column_name)
            outfile.write(','.join(fieldnames) + '\n')

            for line in infile.readlines(READ_BUFFER):
                row = parse_row(line)
                username = users.get(row[id_no])
                if username is None:
                    username = ''
                    users[row[id_no]] = username
                    c_warning('Username not found for userid={}'
                        .format(row[id_no]))
                row.insert(id_no + 1, username)
                outfile.write('"' + '","'.join(row) + '"\n')


def parse_row(line):
    """Correct parsing CSV row with binary data, returns list."""
    lex = shlex.shlex(line.rstrip(), posix=True)
    lex.whitespace = ','
    return list(lex)


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
