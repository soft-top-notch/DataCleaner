#!/usr/bin/env python2.7
"""Convert forum messages from CSV to JSON format.

Will output new JSON file.

Usage:
    msg_to_json.py [-hV] [--exit-on-error] [--forums=FORUMS_CSV] \
        [--topics=TOPICS_CSV] POSTS_CSV...

Options:
    --exit-on-error               Exit on error, do not continue
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    msg_to_json.py --forums=forums.csv --topics=topics.csv posts.csv
    msg_to_json.py --forums=boards.csv messages.csv
"""
import os
import re
import shlex
import sys
from collections import Counter

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
    forums = None
    if args['--forums']:
        forums = read_forums(args['--forums'])

    topics = None
    if args['--topics']:
        topics = read_topics(args['--topics'])

    for filepath in args['POSTS_CSV']:
        try:
            msg_to_json(filepath, forums, topics)
        except KeyboardInterrupt:
            print('Control-C pressed...')
            sys.exit(138)
        except Exception as error:
            if args['--exit-on-error']:
                raise
            else:
                c_error('{} ERROR:{}'.format(filepath, error))


def read_forums(filepath):
    """Read forum id and name from CSV file."""
    forums = {}
    with open(filepath, 'rb') as csvfile:
        fieldnames = parse_row(csvfile.readline())

        ids = (filter(id_re.match, fieldnames) or
            filter(forum_id_re.match, fieldnames))
        if not ids:
            c_error('Column forum_id not found in file {}'.format(filepath))
            return
        id_no = fieldnames.index(ids[0])

        names = filter(forum_name_re.match, fieldnames)
        if not names:
            c_error('Column forum_name not found in file {}'.format(filepath))
            return
        name_no = fieldnames.index(names[0])

        for line in csvfile:
            row = parse_row(line)
            forums[row[id_no]] = row[name_no]

    return forums


def read_topics(filepath):
    """Read topic id and name from CSV file."""
    topics = {}
    with open(filepath, 'rb') as csvfile:
        fieldnames = parse_row(csvfile.readline())

        ids = (filter(id_re.match, fieldnames) or
            filter(topic_id_re.match, fieldnames))
        if not ids:
            c_error('Column topic_id not found in file {}'.format(filepath))
            return
        id_no = fieldnames.index(ids[0])

        names = filter(topic_name_re.match, fieldnames)
        if not names:
            c_error('Column topic_name not found in file {}'.format(filepath))
            return
        name_no = fieldnames.index(names[0])

        forum_ids = filter(forum_id_re.match, fieldnames)
        if not forum_ids:
            c_error('Column forum_id not found in file {}'.format(filepath))
            return
        forum_id_no = fieldnames.index(forum_ids[0])

        for line in csvfile:
            row = parse_row(line)
            topics[row[id_no]] = (row[name_no], row[forum_id_no])

    return topics


def msg_to_json(filepath, forums, topics):
    """Convert forum messages from CSV to JSON format."""
    post_comments = Counter()

    with open(filepath, 'rb') as infile:
        fieldnames = parse_row(infile.readline())

        if topics:
            topic_ids = filter(topic_id_re.match, fieldnames)
            if not topic_ids:
                c_error('Column topic_id not found in file {}'.format(filepath))
                return
            topic_id_no = fieldnames.index(topic_ids[0])
        else:
            topic_names = filter(topic_name_re.match, fieldnames)
            if not topic_names:
                c_error('Column topic_name not found in file {}'.format(filepath))
                return
            topic_name_no = fieldnames.index(topic_names[0])

            if forums:
                forum_ids = filter(forum_id_re.match, fieldnames)
                if not forum_ids:
                    c_error('Column forum_id not found in file {}'.format(filepath))
                    return
                forum_id_no = fieldnames.index(forum_ids[0])
            else:
                forum_names = filter(forum_name_re.match, fieldnames)
                if not forum_names:
                    c_error('Column forum_name not found in file {}'.format(filepath))
                    return
                forum_name_no = fieldnames.index(forum_names[0])

        dates = filter(date_re.match, fieldnames)
        if not dates:
            c_error('Column date not found in file {}'.format(filepath))
            return
        date_no = fieldnames.index(dates[0])

        msgs = filter(msg_re.match, fieldnames)
        if not msgs:
            c_error('Column msg not found in file {}'.format(filepath))
            return
        msg_no = fieldnames.index(msgs[0])

        pids = filter(pid_re.match, fieldnames)
        if not pids:
            c_error('Column pid not found in file {}'.format(filepath))
            return
        pid_no = fieldnames.index(pids[0])

        users = filter(user_re.match, fieldnames)
        if not users:
            c_error('Column user not found in file {}'.format(filepath))
            return
        user_no = fieldnames.index(users[0])

        basepath = os.path.splitext(filepath)[0]
        with open(basepath + '.json', 'wb') as outfile:
            for line in infile:
                row = parse_row(line)
                pid = row[pid_no]

                if topics:
                    topic, forum_id = topics.get(row[topic_id_no], ('', ''))
                    forum = forums.get(forum_id, '')
                else:
                    topic = row[topic_name_no]
                    if forums:
                        forum = forums.get(row[forum_id_no], '')
                    else:
                        forum = row[forum_name_no]

                outfile.write('{'
                    '"_type":"forums","_source":{'
                        '"type":"post",'
                        '"forum":"%s",'
                        '"subject":"%s",'
                        '"author":"%s",'
                        '"date":"%s",'
                        '"message":"%s",'
                        '"pid":"%s",'
                        '"cid":"%s"'
                    '}'
                '}\n' % (
                    esc(forum),
                    esc(topic),
                    esc(row[user_no]),
                    row[date_no],
                    esc(row[msg_no]),
                    pid,
                    post_comments[pid]
                ))
                post_comments[pid] += 1


def parse_row(line):
    """Correct parsing CSV row with binary data, returns list."""
    lex = shlex.shlex(line.rstrip(), posix=True)
    lex.whitespace = ','
    return list(lex)


def esc(s):
    """Escape special characters."""
    return s.replace('\\"', '"').replace('"', '\\"').replace('\n', '\\n')


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
