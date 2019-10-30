#!/usr/bin/env python2.7
"""Convert personal messages from CSV to JSON format.

Will output new JSON file for each file.

Usage:
    msg_to_json.py [-hV] [--exit-on-error] CSVFILE...

Options:
    --exit-on-error               Exit on error, do not continue
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    pm_to_json.py personal_messages.csv
"""
from __future__ import division, print_function

import csv
import os
import re
import sys
from collections import Counter

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

date_re = re.compile(r'^(msgtime)$', re.I)
msg_re = re.compile(r'^(body)$', re.I)
pid_re = re.compile(r'^(id_pm_head)$', re.I)
subj_re = re.compile(r'^(subject)$', re.I)
user_re = re.compile(r'^(from_name)$', re.I)


def main(args):
    """Executes main code."""
    for filepath in args['CSVFILE']:
        try:
            msg_to_json(filepath)
        except KeyboardInterrupt:
            print('Control-C pressed...')
            sys.exit(138)
        except Exception as error:
            if args['--exit-on-error']:
                raise
            else:
                print('{} ERROR:{}'.format(filepath, error))


def msg_to_json(filepath):
    """Convert personal messages from CSV to JSON format."""
    post_comments = Counter()

    with open(filepath, 'rb') as infile:
        reader = csv.reader(infile, quotechar='\\')
        fieldnames = next(reader)

        dates = filter(date_re.match, fieldnames)
        if not dates:
            print('ERROR: Column date not found in file {}'.format(filepath))
            return
        date_no = fieldnames.index(dates[0])

        msgs = filter(msg_re.match, fieldnames)
        if not msgs:
            print('ERROR: Column msg not found in file {}'.format(filepath))
            return
        msg_no = fieldnames.index(msgs[0])

        pids = filter(pid_re.match, fieldnames)
        if not pids:
            print('ERROR: Column pid not found in file {}'.format(filepath))
            return
        pid_no = fieldnames.index(pids[0])

        subjs = filter(subj_re.match, fieldnames)
        if not subjs:
            print('ERROR: Column subj not found in file {}'.format(filepath))
            return
        subj_no = fieldnames.index(subjs[0])

        users = filter(user_re.match, fieldnames)
        if not users:
            print('ERROR: Column user not found in file {}'.format(filepath))
            return
        user_no = fieldnames.index(users[0])

        basepath = os.path.splitext(filepath)[0]
        with open(basepath + '.json', 'wb') as outfile:
            for row in reader:
                row = fix_comma(row)
                row = map(remove_quotes, row)
                pid = row[pid_no]

                outfile.write('{'
                    '"_type":"forums","_source":{'
                        '"type":"pm",'
                        '"subject":"%s",'
                        '"author":"%s",'
                        '"date":"%s",'
                        '"message":"%s",'
                        '"pid":"%s",'
                        '"cid":"%s"'
                    '}'
                '}\n' % (
                    esc(row[subj_no]),
                    esc(row[user_no]),
                    row[date_no],
                    esc(row[msg_no]),
                    pid,
                    post_comments[pid]
                ))
                post_comments[pid] += 1


def esc(s):
    """Escape special characters."""
    return s.replace('\\"', '"').replace('"', '\\"')


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


def remove_quotes(s):
    if s[0] in ('`', '"', "'") and s[0] == s[-1]:
        s = s[1:-1]
    return s


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
