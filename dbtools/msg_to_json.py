#!/usr/bin/env python2
"""Convert forum messages from CSV to JSON format.

Will output new JSON file.

Usage:
    msg_to_json.py [-hV] [--exit-on-error] [--forum=NAME] \
        [--topics=CSV] POSTS_CSV...

Options:
    --exit-on-error               Exit on error, do not continue
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    msg_to_json.py --forum='Name' messages.csv
    msg_to_json.py --forum='Name' --topics=topics.csv posts.csv
"""
from __future__ import division, print_function

import io
import json
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

forum_id_re = re.compile(r'^(forum_?id|id_board)$', re.I)
forum_name_re = re.compile(r'^(name|forum_?name|title)$', re.I)
topic_id_re = re.compile(r'^(topic_?id|thread_?id)$', re.I)
topic_name_re = re.compile(r'^(name|topic_?name|subject|title)$', re.I)

date_re = re.compile(r'^(posted|poster_?time|date_?line)$', re.I)
msg_re = re.compile(r'^(message|body|pagetext)$', re.I)
pid_re = re.compile(r'^((?:topic)_?id|id_?(?:topic)|post_?id)$', re.I)
user_re = re.compile(r'^(poster|poster_?name|user_?name)$', re.I)


def main(args):
    """Executes main code."""
    topics = None
    if args['--topics']:
        topics = read_topics(args['--topics'])

    for filepath in args['POSTS_CSV']:
        try:
            msg_to_json(filepath, args['--forum'], topics)
        except KeyboardInterrupt:
            print('Control-C pressed...')
            sys.exit(138)
        except Exception as error:
            if args['--exit-on-error']:
                raise
            else:
                print('{} ERROR:{}'.format(filepath, error))


def read_topics(filepath):
    """Read topic id and name from CSV file."""
    topics = {}
    with io.open(filepath, 'r', encoding='utf-8') as csvfile:
        reader = csv_reader(csvfile)
        fieldnames = next(reader)

        ids = (filter(id_re.match, fieldnames) or
            filter(topic_id_re.match, fieldnames))
        if not ids:
            print('ERROR: Column topic_id not found in file {}'
                  .format(filepath))
            return
        id_no = fieldnames.index(ids[0])

        names = filter(topic_name_re.match, fieldnames)
        if not names:
            print('ERROR: Column topic_name not found in file {}'
                  .format(filepath))
            return
        name_no = fieldnames.index(names[0])

        for row in reader:
            topics[row[id_no]] = row[name_no]

    return topics


def msg_to_json(filepath, forum, topics):
    """Convert forum messages from CSV to JSON format."""
    post_comments = Counter()

    with io.open(filepath, 'r', encoding='utf-8') as infile:
        reader = csv_reader(infile)
        fieldnames = next(reader)

        if topics:
            topic_ids = filter(topic_id_re.match, fieldnames)
            if not topic_ids:
                print('ERROR: Column topic_id not found in file {}'
                      .format(filepath))
                return
            topic_id_no = fieldnames.index(topic_ids[0])
        else:
            topic_names = filter(topic_name_re.match, fieldnames)
            if not topic_names:
                print('ERROR: Column topic_name not found in file {}'
                      .format(filepath))
                return
            topic_name_no = fieldnames.index(topic_names[0])

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

        users = filter(user_re.match, fieldnames)
        if not users:
            print('ERROR: Column user not found in file {}'.format(filepath))
            return
        user_no = fieldnames.index(users[0])

        basepath = os.path.splitext(filepath)[0]
        with open(basepath + '.json', 'wb') as outfile:
            for row in reader:
                row = map(replace_quotes, row)
                pid = row[pid_no]

                if topics:
                    topic = topics.get(row[topic_id_no], '')
                else:
                    topic = row[topic_name_no]

                outfile.write(json.dumps({
                    "_type":"forums",
                    "_source": {
                        "type":"post",
                        "forum": forum,
                        "subject": replace_quotes(topic),
                        "author": row[user_no],
                        "date": row[date_no],
                        "message": row[msg_no],
                        "pid": replace_quotes(pid),
                        "cid": post_comments[pid]
                    }
                }, separators=(',', ':')) + '\n')
                post_comments[pid] += 1


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
