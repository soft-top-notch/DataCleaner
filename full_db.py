#!/usr/bin/env python2.7
"""Parse all tables from SQL.

Called with filename argument for one or more sql files/dumps.  Will output
csv file for each table and move sql source file to completed or failed
directories depending on if there were any errors.

Filename argument can be a list of files or wildcard (*).

Usage:
    full_db.py [-hV] [--completed=DIR] [--failed=DIR] [--exit-on-error] \
[--tables=TABLES] SQLFILE...

Options:
    --completed=DIR               Directory to store completed sql files \
[default: completed]
    --exit-on-error               Exit on error, do not continue
    --failed=DIR                  Directory to store sql files with errors \
[default: failed]
    -h, --help                    This help output
    --tables=REGEXP               RegExp for table names
    -V, --version                 Print version and exit

Examples:
    full_db.py --completed='~/success' --failed='~/error' test.sql
    full_db.py --exit-on-error ~/samples/*.sql
    full_db.py --tables="(?:[a-z0-9]+_)?(members?|users?)" test.sql
"""
from __future__ import division, print_function

import io
import os
import re
import sys

import attr
import magic
from docopt import docopt
from pyparsing import alphanums, CaselessKeyword, CaselessLiteral, \
    Combine, Group, NotAny, nums, Optional, oneOf, OneOrMore, \
    ParseException, ParseResults, quotedString, Regex, removeQuotes, \
    Suppress, Word, WordEnd, ZeroOrMore

from dc import move, TqdmUpTo, c_success, c_action, c_action_info, \
    c_action_system, c_sys_success, c_warning, c_darkgray, c_darkgreen,\
    c_lightgreen, c_lightgray, c_lightblue, c_blue, c_error

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

MAX_FAILURE_RATE = 0.20
# How many bytes to read at a time
READ_BUFFER = 10485760


@attr.s()
class CreateTable(object):
    statement = attr.ib()
    ending = attr.ib(default=';')


@attr.s()
class InsertInto(object):
    statement = attr.ib()
    ending = attr.ib(default=');\n')


def main(args):
    """Executes main code."""
    for filepath in args['SQLFILE']:
        try:
            parse(filepath)
        except KeyboardInterrupt:
            print('Control-c pressed...')
            sys.exit(138)
        except Exception as error:
            move(filepath, args['--failed'])
            if args['--exit-on-error']:
                raise
            else:
                print('{} ERROR:{}'.format(filepath, error))
        else:
            move(filepath, args['--completed'])


def parse(filepath):
    """Opens sql file and parses statements, outputing data into CSV."""
    field_names = []
    bad_inserts = 0
    total_tables = 0
    total_inserts = 0
    total_values = 0
    table_name = None
    last_read = 0

    # Delete old bad_inserts file if exists
    if os.path.exists(filepath + '.bad_inserts.txt'):
        os.unlink(filepath + '.bad_inserts.txt')

    # Not sure what the encoding is, will try iso-8859-1
    encoding = 'iso-8859-1'
    retry = False
    # Determine encoding
    print('{}: Determining encoding'.format(filepath))
    try:
        with io.open(filepath, 'Ur', encoding=encoding) as sqlfile:
            sqlfile.read(READ_BUFFER)
    except UnicodeDecodeError:
        retry = True
    if retry:
        # Try detecting encoding
        m = magic.Magic(mime_encoding=True)
        encoding = m.from_file(filepath)
        try:
            with io.open(filepath, 'Ur', encoding=encoding) as sqlfile:
                sqlfile.read(READ_BUFFER)
        except UnicodeDecodeError:
            print('{}: Unable to determine encoding of file'.format(filepath))
            raise
    print('{}: Using {} encoding'.format(filepath, encoding))

    read_pbar = TqdmUpTo(desc='read', unit=' bytes',
                         total=os.path.getsize(filepath))
    values_pbar = TqdmUpTo(desc='processed', unit=' value lines')

    # Extract data from statements and write to csv file
    last_table_name = ''
    with io.open(filepath, 'Ur', encoding=encoding) as sqlfile:
        for table in read_file(sqlfile):
            create_table, table_name, insert, byte_num = table
            if create_table:
                match = parse_sql(create_table.statement, CREATE_FULL)
                if match and isinstance(match, ParseResults):
                    field_names = match.asDict()['field_names']
                elif isinstance(match, ParseException):
                    raise_error(match, encoding)
                else:
                    raise_error(Exception('Unknown error has occurred'))
            elif insert:
                c_warning('Getting field names from first insert')
                insert_fields = re.search('(\(`.*`\))', insert)
                if not insert_fields:
                    raise_error(ParseException(
                        'Field names not found in insert'))
                fields_only = insert_fields.group(1)
                match = parse_sql(fields_only, INSERT_FIELDS)
                if match and isinstance(match, ParseResults):
                    field_names = match.asDict().get('field_names')
                elif isinstance(match, ParseException):
                    pass
                else:
                    raise (Exception('Unknown error has occurred'))

            basepath = os.path.splitext(filepath)[0]
            csv_path = '{}.{}.csv'.format(basepath, table_name[0])
            if table_name != last_table_name:
                with io.open(csv_path, 'w', encoding=encoding) as cf:
                    if field_names:
                        cf.write(','.join(field_names))
                        cf.write(u'\n')
                    else:
                        c_error('No field names found')
                total_tables += 1
                last_table_name = table_name

            values = process_insert(insert)
            if values:
                values_list, error = process_values(values)
                if values_list:
                    value_lines = write_values(values_list, csv_path, encoding)
                    total_values += value_lines
                    values_pbar.update_to(total_values)

            read_pbar.update_to(byte_num)
            total_inserts += 1

    read_pbar.close()
    values_pbar.close()

    print(
        '{}: Processed {} table(s) {} insert(s) with {}'
        ' value lines and skipped {} errors'.
        format(filepath, total_tables, total_inserts,
               total_values, bad_inserts))


def parse_sql(line, pattern):
    try:
        return pattern.parseString(line)
    except ParseException as pe:
        return pe


def process_insert(insert):
    match = re.search('^(?:.*)?(VALUES.*;)', insert)
    if match:
        return match.group(1)


def process_values(values):
    result = parse_sql(values, VALUES_ONLY)
    if result and isinstance(result, ParseResults):
        values_list = result.asDict()['values']
        return values_list, None
    else:
        return None, result


def raise_error(exception, encoding=None):
    """Combine Exception error msg with last line processed."""
    line = getattr(exception, 'line', None)
    print()
    if line:
        if len(line) > 5000:
            line = line[0:5000] + '...OUTPUT CUT DUE TO LENGTH...'
            print(line)
    if encoding:
        print('Encoding used: ' + encoding)
    raise exception


def read_file(sqlfile):
    # Current CreateTable object
    create_table = None
    # List of insert statments
    table_name = None
    parsing = None
    # Estimate byte length based on line length
    byte_num = 0
    while True:
        lines = sqlfile.readlines(READ_BUFFER)
        if not lines:
            break
        for line in lines:
            line = line.replace('\11\12\15\40-\176', '')
            # line = line.replace('\\11\\12\\15\\40-\\176', '')
            byte_num += len(line)
            valid_insert = None
            # If not parsing a CREATE or INSERT statement, look for one
            if parsing:
                # Continue parsing the current statement
                parsing.statement.append(line)
                if parsing.ending in line:
                    no_newlines = rm_newlines(parsing.statement)
                    if isinstance(parsing, CreateTable):
                        create_table.statement = ''.join(no_newlines)
                    elif isinstance(parsing, InsertInto):
                        valid_insert = ''.join(no_newlines)
                    parsing = None
            else:
                insert = parse_sql(line, INSERT_BEGIN)
                if insert and isinstance(insert, ParseResults):
                    if not table_name:
                        table_name = insert.asDict().get('table_name')
                    insert_into = InsertInto([line])
                    if insert_into.ending in line:
                        no_newlines = rm_newlines([line])
                        valid_insert = ''.join(no_newlines)
                    else:
                        parsing = insert_into
                        continue
                else:
                    value = parse_sql(line, VALUES_ONLY)
                    if value and isinstance(value, ParseResults):
                        value_only = InsertInto([line])
                        if value_only.ending in line:
                            no_newlines = rm_newlines([line])
                            valid_insert = ''.join(no_newlines)
                        else:
                            parsing = value_only
                            continue
                    # elif not create_table:
                    else:
                        create = parse_sql(line, CREATE_BEGIN)
                        if create and isinstance(create, ParseResults):
                            # if not table_name:
                            table_name = create.asDict().get('table_name')
                            create_table = CreateTable([line])
                            if create_table.ending not in line:
                                parsing = create_table
            if valid_insert:
                yield create_table, table_name, valid_insert, byte_num


def rm_newlines(lines):
    return [x.replace('\n', '').replace('\r', '') for x in lines]


def write_bad(filepath, insert_num, error, insert):
    with open(filepath + '.bad_inserts.txt', 'a') as bad:
        bad.write('Insert #{}\n'.format(insert_num))
        bad.write('Error:{}\n'.format(error))
        bad.write('\nLine:\n{}\n\n'.format(insert))
        bad.write('******\n')


def write_values(values_list, path, encoding):
    """Write values to csv and return number of lines written."""
    lines = 0
    with io.open(path, 'a', encoding=encoding) as cf:
        for values in values_list:
            cf.write(','.join(['"%s"' % value for value in values]))
            cf.write(u'\n')
            lines += 1
    return lines


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)

    # pyparsing patterns for matching/parsing SQL
    BACKTICK = Suppress(Optional('`'))

    E = CaselessLiteral("E")
    binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
    arithSign = Word("+-", exact=1)
    realNum = Combine(
        Optional(arithSign) +
        (Word(nums) + "." + Optional(Word(nums)) | ("." + Word(nums))) +
        Optional(E + Optional(arithSign) + Word(nums)))
    intNum = Combine(
        Optional(arithSign) + Word(nums) +
        Optional(E + Optional("+") + Word(nums)))

    # Field Names
    KEYS = (CaselessKeyword('PRIMARY') | CaselessKeyword('UNIQUE')
            | CaselessKeyword('KEY'))
    FIELD_META = ZeroOrMore((Word('(' + alphanums + '_' + ',' + ')')
                            | quotedString) + NotAny(KEYS))
    FIELD_NAME = BACKTICK + Word(alphanums + '_') + BACKTICK + Suppress(
        Optional(','))
    FIELD_NAME_META = NotAny(KEYS) + FIELD_NAME + Suppress(
        Optional(FIELD_META))
    CREATE_FIELDS = Suppress('(') + Group(
        OneOrMore(FIELD_NAME_META)).setResultsName('field_names') + Suppress(
            Optional(KEYS) + Optional(')'))
    INSERT_FIELDS = Suppress('(') + Group(
        OneOrMore(FIELD_NAME)).setResultsName('field_names') + Suppress(')')

    # CREATE TABLE
    TABLE_NAME = Combine(
        BACKTICK +
        Regex(r'(?:[a-z0-9]+_)?({})'.format(args['--tables'] or r'[a-z0-9_]+'),
              flags=re.IGNORECASE) +
        BACKTICK
    ) + WordEnd()
    CREATE = CaselessKeyword('CREATE TABLE')
    CREATE_EXISTS = CaselessKeyword('IF NOT EXISTS')
    CREATE_BEGIN = CREATE + Optional(CREATE_EXISTS) + TABLE_NAME('table_name')
    CREATE_FULL = CREATE_BEGIN + CREATE_FIELDS

    # VALUES
    SQL_CONV = Regex(r'(CONV\(\'[0-9]\', [0-9]+, [0-9]+\) \+ [0-9]+)')
    VALUE = Combine((realNum | intNum
                     | quotedString.addParseAction(removeQuotes)
                     | SQL_CONV | Word(alphanums))) + Suppress(Optional(','))
    VALUES = Suppress('(') + Group(OneOrMore(VALUE)) + Suppress(
        ')') + Suppress(Optional(','))

    VALUES_ONLY = CaselessKeyword('VALUES') + Group(
        OneOrMore(VALUES))('values')

    # INSERT INTO
    INSERT = CaselessKeyword('INSERT INTO')
    INSERT_BEGIN = INSERT + TABLE_NAME('table_name')

    main(args)
