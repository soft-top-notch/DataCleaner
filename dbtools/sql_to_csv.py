#!/usr/bin/env python2
"""Convert all or selected tables from SQL dump to CSV files.

Will output CSV file for each found table in utf-8 encoding with comma
separator. File names argument can be a list of files or wildcard.

Usage:
    sql_to_csv.py [-hV] [--encoding=ENC] [--tables=RE] SQLFILES...

Options:
    -h, --help                    This help output
    -V, --version                 Print version and exit
    --encoding=ENC                Encoding [default: latin1]
    --tables=RE                   RegExp for matching table names

Examples:
    sql_to_csv.py mysql.sql
    sql_to_csv.py --encoding=utf-8 postgres.sql
    sql_to_csv.py --tables='members?|users?' *.sql
"""
from __future__ import division, print_function

import io
import os
import re
import sys

from docopt import docopt
from sqlparse import tokens
from sqlparse.lexer import tokenize
from tqdm import tqdm

from utils import pair_quotes, replace_quotes, splitlines

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

class CreateTable():
    table = 0
    columns = 1

    @staticmethod
    def parse(sql, encoding=None):
        table = ''
        columns = []

        bracket_level = 0
        skip_to_comma = False
        waiting = CreateTable.table
        tokener = tokenize(sql, encoding)
        for t in tokener:
            if waiting == CreateTable.columns:
                if t[0] not in (tokens.Punctuation, tokens.Whitespace,
                                tokens.Newline):
                    if t[1] in ('PRIMARY', 'KEY'):
                        skip_to_comma = True
                    if not skip_to_comma:
                        columns.append(replace_quotes(t[1]))
                        skip_to_comma = True
                elif t[0] == tokens.Punctuation:
                    if t[1] == ',':
                        if bracket_level == 1:
                            skip_to_comma = False
                    elif t[1] == '(':
                        bracket_level += 1
                    elif t[1] == ')':
                        bracket_level -= 1
                    elif t[1] == ';':
                        return table, columns
            elif waiting == CreateTable.table:
                if t[0] in (tokens.Name, tokens.String.Symbol):
                    if replace_quotes(t[1]) != 'public':
                        table = replace_quotes(t[1])
                        waiting += 1


class InsertInto():
    table = 1
    columns = 2
    values = 3

    @staticmethod
    def parse(sql, encoding=None):
        table = ''
        columns = []
        values = []

        waiting = InsertInto.table
        tokener = tokenize(sql, encoding)
        for t in tokener:
            if waiting == InsertInto.values:
                if t[0] == tokens.Punctuation:
                    if t[1] == ')':
                        if len(columns) != len(values) and columns:
                            raise Exception(
                                'ERORR: Wrong length of columns in table "%s",'
                                ' should be %d for data: %s' %
                                (table, len(columns), values))
                        yield values
                        values = []
                    elif t[1] == ';':
                        raise StopIteration()
                elif t[0] not in [tokens.Whitespace, tokens.Newline]:
                    if t[0] == tokens.Operator:
                        # handle strange values like +1
                        n = next(tokener)
                        values.append(t[1] + n[1])
                    else:
                        values.append(t[1])
            elif waiting == InsertInto.columns:
                if t[0] not in (tokens.Punctuation, tokens.Whitespace,
                                tokens.Newline):
                    if t[1] == 'VALUES':
                        yield table, columns
                        waiting += 1
                    else:
                        columns.append(replace_quotes(t[1]))
            elif waiting == InsertInto.table:
                if t[0] in (tokens.Name, tokens.String.Symbol):
                    if replace_quotes(t[1]) != 'public':
                        table = replace_quotes(t[1])
                        waiting += 1


class Copy():
    table = 1
    columns = 2
    values = 3

    @staticmethod
    def parse(sql, encoding=None):
        table = ''
        columns = []
        values = []

        waiting = Copy.table
        tokener = tokenize(sql, encoding)
        for t in tokener:
            if waiting == Copy.columns:
                if t[0] not in (tokens.Punctuation, tokens.Whitespace,
                                tokens.Newline):
                    if t[1] == 'FROM':
                        yield table, columns
                        waiting += 1
                        break
                    else:
                        columns.append(replace_quotes(t[1]))
            elif waiting == Copy.table:
                if t[0] in (tokens.Name, tokens.String.Symbol):
                    if replace_quotes(t[1]) != 'public':
                        table = replace_quotes(t[1])
                        waiting += 1

        sql = sql[sql.index(';') + 1:sql.rindex('\\.')]\
            .lstrip().decode(encoding)
        for line in splitlines(sql):
            values = line.split('\t')
            if len(columns) != len(values) and columns:
                raise Exception(
                    'ERORR: Wrong length of columns in table "%s",'
                    ' should be %d for data: %s' %
                    (table, len(columns), values))

            values = map(lambda x: '"' + x + '"', values)
            yield map(lambda x: x if x != '"\\N"' else '', values)


def csv_line(row):
    row = map(lambda s: replace_quotes(s, '"'), row)
    line = ','.join(row).replace("\\'", "'")
    return line.replace('\r', '\\r').replace('\n', '\\n') + '\n'


def write_csv(sqlpath, table, columns, values, encoding='utf-8'):
    path = os.path.splitext(sqlpath)[0]
    filepath = '%s.%s.csv' % (path, table)

    if columns:
        with io.open(filepath, 'w', encoding=encoding) as f:
            f.write(csv_line(columns))

    lines = 0
    if values:
        with io.open(filepath, 'a', encoding=encoding) as f:
            for row in values:
                f.write(csv_line(row))
                lines += 1
    return lines


def parse(filepath, tables, encoding):
    if tables:
        tables_re = re.compile(r'^(?:[a-z0-9]+_)?(%s)$' % tables, flags=re.I)

    lines = ''
    prev_quote = None
    statement = None
    total_tables = 0
    total_lines = 0

    pbar = tqdm(desc='Parsing %s' % filepath, total=os.path.getsize(filepath),
                unit='b', unit_scale=True)
    with open(filepath, 'rb') as f:
        for line in f:
            pbar.update(len(line))

            if statement is None:
                if line.startswith('INSERT INTO'):
                    statement = InsertInto
                elif line.startswith('COPY'):
                    statement = Copy
                elif line.startswith('CREATE TABLE'):
                    statement = CreateTable
                else:
                    continue

            lines += line

            if statement == InsertInto:
                if not line.endswith((');\n', ');\r\n')):
                    continue
                prev_quote = pair_quotes(line, prev_quote)
                if prev_quote:
                    continue
                parser = InsertInto.parse(lines, encoding)
                table, columns = next(parser)
                if not tables or tables_re.match(table):
                    total_lines += write_csv(filepath, table, None, parser)

            elif statement == Copy:
                if not line.startswith('\\.'):
                    continue
                parser = Copy.parse(lines, encoding)
                table, columns = next(parser)
                if not tables or tables_re.match(table):
                    total_lines += write_csv(filepath, table, None, parser)

            elif statement == CreateTable:
                if not line.endswith((';\n', ';\r\n')):
                    continue
                table, columns = CreateTable.parse(lines, encoding)
                if not tables or tables_re.match(table):
                    write_csv(filepath, table, columns, None)
                    total_tables += 1

            lines = ''
            statement = None

    pbar.close()
    print('  Found %d table(s), %d value line(s).' %
          (total_tables, total_lines))


def main(args):
    for filepath in args['SQLFILES']:
        parse(filepath, args['--tables'], args['--encoding'])


if __name__ == '__main__':
    args = docopt(__doc__, version=__version__)
    main(args)
