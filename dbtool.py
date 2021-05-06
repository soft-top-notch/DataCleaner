#!/usr/bin/env python2
"""DB Tool

Usage:
  dbtool.py --showtables <file_path>
  dbtool.py --extract <file_path> --tables=tables [--encoding=enc]
  dbtool.py --extract <file_path> --tables=tables [--schema] [--encoding=enc]
  dbtool.py --mergeuser [--exit-on-error] [--user_id=user_id] [--pm_id=pm_id] [--pm_file_path=pm_file_path] <user_file_path> <csv_file_path>
  dbtool.py --json --type=type [--exit-on-error] [--forum=forum] \
    [--topic=topic] [--recipient=recipient] [--pm=pm] \
     --input=input

Options:
  -h --help                 Show this screen.
  --version                 Show version.
  --schema                  Export only schema
  --encoding=enc            Encoding for sql parser
  --exit-on-error           Exit if error happen
  --input=input             Input for csv to json
  --forum=forum             Input for post csv to json
  --topic=topic             Input for post csv to json
  --recipient=recipient     Input for pm csv to json
  --pm=pm                   Input for pm csv to json
"""
import os
import re
import logging
import sys

from docopt import docopt
from tqdm import tqdm
from dbtools.sql_to_csv import parse
from dbtools.merge_user import (
    read_users,
    read_pm_user_ids,
    merge_users,
    merge_users_from_pm
)

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

# Logging format
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(
    logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
)

# Logging handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

get_table = re.compile(
    "(?<=CREATE\sTABLE\s\`).*?(?=\`\s)",
    re.IGNORECASE
)


def post_to_json(**kwargs):
    """
    Handle convert merge csv of post to json
    :param kwargs:
    :return:
    """

    # Import msg_to_json
    from dbtools.msg_to_json import (
        read_topics,
        msg_to_json
    )

    # Load variables
    csv_path = kwargs.get("csv_path")
    exit_on_error = kwargs.get("exit_on_error")
    forum = kwargs.get("forum")

    # Read topics
    topic = kwargs.get("topic")
    if topic:
        topic = read_topics(topic)

    # Iterate csv file
    for file_path in csv_path.split(","):
        try:
            msg_to_json(
                file_path,
                forum,
                topic
            )
        except KeyboardInterrupt:
            logger.debug("Control-C pressed...")
            sys.exit(138)
        except Exception as error:
            if exit_on_error:
                raise
            logger.debug(
                "%s ERROR:%s" % (file_path, error)
            )


def msg_to_json(**kwargs):
    """
    Handle convert merge csv of message to json
    :param kwargs:
    :return:
    """
    # Import pm_to_json
    from dbtools.pm_to_json import (
        read_pm_recipients,
        read_pm,
        msg_to_json
    )

    # Load variables
    csv_path = kwargs.get("csv_path")
    exit_on_error = kwargs.get("exit_on_error")

    # Read recipients
    pm_recipients = kwargs.get("pm_recipients")
    if pm_recipients:
        pm_recipients = read_pm_recipients(pm_recipients)

    # Read pm text
    pmtext_pms = kwargs.get("pmtext_pms")
    if pmtext_pms:
        pmtext_pms = read_pm(pmtext_pms)

    # Iterate csv file
    for file_path in csv_path.split(","):
        try:
            msg_to_json(
                file_path,
                pm_recipients,
                pmtext_pms
            )
        except KeyboardInterrupt:
            logger.debug("Control-C pressed...")
            sys.exit(138)
        except Exception as error:
            if exit_on_error:
                raise
            logger.debug(
                "%s ERROR:%s" % (file_path, error)
            )


def mergeuser(**kwargs):
    """
    Handle merge user csv
    :param kwargs:
    :return:
    """

    # Load param
    user_file_path = kwargs.get(
        "user_file_path"
    )
    csv_files_path = kwargs.get(
        "csv_files_path"
    )
    pm_file_path = kwargs.get(
        "pm_file_path"
    )
    exit_on_error = kwargs.get(
        "exit_on_error"
    )
    user_id = kwargs.get(
        "user_id"
    )
    pm_id = kwargs.get(
        "pm_id"
    )

    # Load users
    users, username_column = read_users(user_file_path)
    if not users:
        sys.exit(1)

    # Load pm ids if exist
    if pm_file_path:
        pm_user_ids = read_pm_user_ids(pm_file_path)

    for csv_file in csv_files_path.split(","):
        try:
            if not pm_file_path:
                merge_users(
                    csv_file,
                    users,
                    username_column,
                    user_id
                )
            else:
                merge_users_from_pm(
                    filepath=csv_file,
                    pm_user_ids=pm_user_ids,
                    users=users,
                    username_column=username_column,
                    pm_id=pm_id
                )
        except KeyboardInterrupt:
            logger.info("Control-C pressed...")
            sys.exit(138)
        except Exception as error:
            if exit_on_error:
                raise

            logger.info("%s ERROR:%s" % (csv_file, error))


def show_tables(**kwargs):
    """
    Handle export all tables from sql dump
    :param kwargs:
        file_path => string: path to sql dump
    :return: => string: list of all tables logged
    """
    filepath = kwargs.get("<file_path>")

    # Table pool init
    tables = []

    # Load progress bar
    pbar = tqdm(
        desc="Parsing %s" % filepath,
        total=os.path.getsize(filepath),
        unit="b",
        unit_scale=True
    )

    # Load sql dump
    with open(filepath, "rb") as f:
        # Iterate over line of sql dump
        for line in f:
            # Update iteration process over sql dump
            pbar.update(len(line))

            # If line not have create table, continue
            if not line.startswith("CREATE TABLE"):
                continue

            # Parse table name
            table = get_table.search(line).group()

            # If already found this table then continue else append to tables pool
            if table in tables:
                logger.info(
                    "Found table: %s but already exist!" % table
                )
            else:
                logger.info(
                    "New table found: %s" % table
                )
                tables.append(table)

    # Close progress bar
    pbar.close()

    # Log all tables
    logger.info(
        "All tables: %s" % ", ".join(tables)
    )


def main(args):
    """
    Main process handle all parameter from command line
    :param args:
    :return:
    """
    # Handle show tables
    if args.get("--showtables"):
        show_tables(**args)

    # Handle extract detail tables
    if args.get("--extract") and args.get("--tables"):
        parse(
            filepath=args.get("<file_path>"),
            tables=args.get("--tables"),
            encoding=args.get("--encoding"),
            schema_only=args.get("--schema")
        )

    # Handle merge user csv
    if args.get("--mergeuser"):
        mergeuser(
            user_file_path=args.get("<user_file_path>"),
            csv_files_path=args.get("<csv_file_path>"),
            pm_file_path=args.get("--pm_file_path"),
            exit_on_error=args.get("--exit-on-error"),
            user_id=args.get("--user_id"),
            pm_id=args.get("--pm_id")
        )

    # Handle csv to json
    if args.get("--json"):
        csv_type = args.get("--type")

        if csv_type not in ["pm", "post"]:
            raise ValueError("Either: post or pm.")

        if csv_type == "pm":
            msg_to_json(
                csv_path=args.get("--input"),
                exit_on_error=args.get("--exit_on_error"),
                pm_recipients=args.get("--recipient"),
                pmtext_pms=args.get("--pm")
            )
        elif csv_type == "post":
            post_to_json(
                csv_path=args.get("--input"),
                exit_on_error=args.get("--exit_on_error"),
                forum=args.get("--forum"),
                topic=args.get("--topic")
            )


if __name__ == "__main__":
    args = docopt(__doc__, version=__version__)
    main(args)
