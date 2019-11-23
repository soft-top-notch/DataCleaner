#!/usr/bin/env python2
"""DB Tool

Usage:
  dbtool.py --showtables <file_path>
  dbtool.py --extract <file_path> --tables=tables [--encoding=enc]

Options:
  -h --help         Show this screen.
  --version         Show version.
  --encoding=enc    Encoding for sql parser
"""
import os
import re
import logging
import sys

from docopt import docopt
from tqdm import tqdm
from dbtools.sql_to_csv import parse

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
        desc='Parsing %s' % filepath,
        total=os.path.getsize(filepath),
        unit='b',
        unit_scale=True
    )

    # Load sql dump
    with open(filepath, 'rb') as f:
        # Iterate over line of sql dump
        for line in f:
            # Update iteration process over sql dump
            pbar.update(len(line))

            # If line not have create table, continue
            if not line.startswith('CREATE TABLE'):
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
            args.get("<file_path>"),
            args.get("--tables"),
            args.get("--encoding")
        )


if __name__ == "__main__":
    args = docopt(__doc__, version=__version__)
    main(args)
