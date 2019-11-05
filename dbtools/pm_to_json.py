#!/usr/bin/env python2
"""Convert personal messages from CSV to JSON format.

The recipients CSV file should have merged user names. Sometimes need
additional parameter "--pm" for matching "pm_id" and "pmtext_id".
Will output new JSON file for each file.

Usage:
    msg_to_json.py [-hV] [--exit-on-error] [--recipients=CSV] [--pm=CSV] \
PMTEXT_CSV...

Options:
    --exit-on-error               Exit on error, do not continue
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    pm_to_json.py --recipients=pm_recipients.merged.csv personal_messages.csv
    pm_to_json.py --recipients=pmreceipt.csv --pm=pm.merged.csv pmtext.csv
"""
from __future__ import division, print_function

import os
import re
import sys
from collections import Counter

from docopt import docopt

from utils import csv_reader, replace_quotes

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

id_re = re.compile(r'^(id)$', re.I)

pm_id_re = re.compile(r'^(id_?pm|pm_?id)$', re.I)
pmtext_id_re = re.compile(r'^(id_?pm_?text|pm_?text_?id)$', re.I)
to_re = re.compile(r'^(member_?name|user_?name|to_?user_?name)$', re.I)

date_re = re.compile(r'^(msgtime|dateline)$', re.I)
msg_re = re.compile(r'^(body|message)$', re.I)
pid_re = re.compile(r'^(id_pm_head|pm_?text_?id)$', re.I)
subj_re = re.compile(r'^(subject|title)$', re.I)
from_re = re.compile(r'^(from_?name|from_?user_?name)$', re.I)


def main(args):
    """Executes main code."""
    pm_recipients = None
    if args['--recipients']:
        pm_recipients = read_pm_recipients(args['--recipients'])

    pmtext_pms = None
    if args['--pm']:
        pmtext_pms = read_pm(args['--pm'])

    for filepath in args['PMTEXT_CSV']:
        try:
            msg_to_json(filepath, pm_recipients, pmtext_pms)
        except KeyboardInterrupt:
            print('Control-C pressed...')
            sys.exit(138)
        except Exception as error:
            if args['--exit-on-error']:
                raise
            else:
                print('{} ERROR:{}'.format(filepath, error))


def read_pm_recipients(filepath):
    """Read msg id and user name from CSV file."""
    pm_recipients = {}
    with open(filepath, 'rb') as csvfile:
        reader = csv_reader(csvfile)
        fieldnames = next(reader)

        ids = (filter(id_re.match, fieldnames) or
            filter(pm_id_re.match, fieldnames))
        if not ids:
            print('ERROR: Column pm_id not found in file {}'
                  .format(filepath))
            return
        id_no = fieldnames.index(ids[0])

        names = filter(to_re.match, fieldnames)
        if not names:
            print('ERROR: Column to_user_name not found in file {}'
                  .format(filepath))
            return
        name_no = fieldnames.index(names[0])

        for row in reader:
            pm_id = replace_quotes(row[id_no])
            name = replace_quotes(row[name_no])
            if pm_id in pm_recipients:
                pm_recipients[pm_id] += ',' + name
            else:
                pm_recipients[pm_id] = name

    return pm_recipients


def read_pm(filepath):
    """Read pm_id, pmtext_id and user_name from CSV file."""
    pmtext_pms = {}
    with open(filepath, 'rb') as csvfile:
        reader = csv_reader(csvfile)
        fieldnames = next(reader)

        ids = (filter(id_re.match, fieldnames) or
            filter(pm_id_re.match, fieldnames))
        if not ids:
            print('ERROR: Column pm_id not found in file {}'
                  .format(filepath))
            return
        id_no = fieldnames.index(ids[0])

        ptext_ids = filter(pmtext_id_re.match, fieldnames)
        if not ptext_ids:
            print('ERROR: Column pmtext_id not found in file {}'
                  .format(filepath))
            return
        pmtext_id_no = fieldnames.index(ptext_ids[0])

        for row in reader:
            pm_id = replace_quotes(row[id_no])
            pmtext_id = replace_quotes(row[pmtext_id_no])
            pmtext_pms[pmtext_id] = pm_id

    return pmtext_pms


def msg_to_json(filepath, pm_recipients, pmtext_pms):
    """Convert personal messages from CSV to JSON format."""
    post_comments = Counter()

    with open(filepath, 'rb') as infile:
        reader = csv_reader(infile)
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

        users = filter(from_re.match, fieldnames)
        if not users:
            print('ERROR: Column user not found in file {}'.format(filepath))
            return
        user_no = fieldnames.index(users[0])

        basepath = os.path.splitext(filepath)[0]
        with open(basepath + '.json', 'wb') as outfile:
            for row in reader:
                row = map(replace_quotes, row)
                pid = row[pid_no]

                if pmtext_pms:
                    pm_id = pmtext_pms.get(pid)
                    recipients = pm_recipients.get(pm_id, '')
                else:
                    recipients = pm_recipients.get(pid, '')
                # if not recipients:
                #     print('WARN: Recipient not found for pm_id {}'.format(pid))

                outfile.write('{'
                    '"_type":"forums","_source":{'
                        '"type":"pm",'
                        '"subject":"%s",'
                        '"author":"%s",'
                        '"recipient":"%s",'
                        '"date":"%s",'
                        '"message":"%s",'
                        '"pid":"%s",'
                        '"cid":"%s"'
                    '}'
                '}\n' % (
                    row[subj_no],
                    row[user_no],
                    recipients,
                    row[date_no],
                    row[msg_no],
                    replace_quotes(pid),
                    post_comments[pid]
                ))
                post_comments[pid] += 1


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
