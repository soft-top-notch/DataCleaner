#!/usr/bin/env python2.7
"""Parse user data from SQL.

Called with filename argument for one or more sql files/dumps.  Will output
csv and move sql source file to completed or failed directories depending on
if there were any errors.

Filename argument can be a list of files or wildcard (*).

Usage:
    parse_sql.py [-hV] [--completed=DIR] [--failed=DIR] [--exit-on-error] \
[--user-table=NAME] SQLFILE...

Options:
    --completed=DIR               Directory to store completed sql files \
[default: completed]
    --exit-on-error               Exit on error, do not continue
    --failed=DIR                  Directory to store sql files with errors \
[default: failed]
    -h, --help                    This help output
    --user-table=NAME             User table name to match
    -V, --version                 Print version and exit

Examples:
    parse_sql.py --completed='~/success' --failed='~/error' test.sql
    parse_sql.py --exit-on-error ~/samples/*.sql
    parse_sql.py --user-table="myfriends" test.sql
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

from datacleaner import move, TqdmUpTo, c_success, c_action, c_action_info, c_action_system, c_sys_success,\
    c_warning, c_darkgray, c_darkgreen, c_lightgreen, c_lightgray, c_lightblue, c_blue


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

# pyparsing patterns for matching/parsing SQL
BACKTICK = Suppress(Optional('`'))

E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-", exact=1)
realNum = Combine(
    Optional(arithSign) + (Word(nums) + "." + Optional(Word(nums)) | (
        "." + Word(nums))) + Optional(E + Optional(arithSign) + Word(nums)))
intNum = Combine(
    Optional(arithSign) + Word(nums) + Optional(E + Optional("+") + Word(nums))
)

# Field Names
KEYS = (CaselessKeyword('PRIMARY') | CaselessKeyword('UNIQUE')
        | CaselessKeyword('KEY'))
FIELD_META = ZeroOrMore((Word('(' + alphanums + '_' + ',' + ')')
                         | quotedString) + NotAny(KEYS))
FIELD_NAME = BACKTICK + Word(alphanums + '_') + BACKTICK + Suppress(
    Optional(','))
FIELD_NAME_META = NotAny(KEYS) + FIELD_NAME + Suppress(Optional(FIELD_META))
CREATE_FIELDS = Suppress('(') + Group(
    OneOrMore(FIELD_NAME_META)).setResultsName('field_names') + Suppress(
        Optional(KEYS) + Optional(')'))
INSERT_FIELDS = Suppress('(') + Group(
    OneOrMore(FIELD_NAME)).setResultsName('field_names') + Suppress(')')

# CREATE TABLE
USER_NAME = Regex(r'((?:[a-z0-9]+_?)?users?)', flags=re.IGNORECASE)
MEMBER_NAME = Regex(r'((?:[a-z0-9]+_?)?members?)', flags=re.IGNORECASE)
TABLE_NAME = (USER_NAME | MEMBER_NAME)
USER_TABLE = Combine(BACKTICK + TABLE_NAME + BACKTICK) + WordEnd()
CREATE = CaselessKeyword('CREATE TABLE')
CREATE_EXISTS = CaselessKeyword('IF NOT EXISTS')
CREATE_BEGIN = CREATE + Optional(CREATE_EXISTS) + USER_TABLE('table_name')
CREATE_FULL = CREATE_BEGIN + CREATE_FIELDS

# VALUES
SQL_CONV = Regex(r'(CONV\(\'[0-9]\', [0-9]+, [0-9]+\) \+ [0-9]+)')
VALUE = Combine((realNum | intNum | quotedString.addParseAction(removeQuotes)
                 | SQL_CONV | Word(alphanums))) + Suppress(Optional(','))
VALUES = Suppress('(') + Group(OneOrMore(VALUE)) + Suppress(')') + Suppress(
    Optional(','))

VALUES_ONLY = CaselessKeyword('VALUES') + Group(OneOrMore(VALUES))('values')

# INSERT INTO
INSERT = CaselessKeyword('INSERT INTO')
INSERT_BEGIN = INSERT + USER_TABLE('table_name')


@attr.s()
class CreateTable(object):
    statement = attr.ib()
    ending = attr.ib(default=';')


@attr.s()
class InsertInto(object):
    statement = attr.ib()
    ending = attr.ib(default=');')


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

    bad_inserts = 0
    total_inserts = 0
    table_name = None
    last_read = 0

    # Extract data from statements and write to csv file
    with io.open(filepath, 'Ur', encoding=encoding) as sqlfile:
        user_table = read_file(sqlfile)
        create_table, table_name, insert, byte_num = user_table.next()
        if create_table:
            p_warning('Getting field names from create table')
            match = parse_sql(create_table.statement, CREATE_FULL)
            if match and isinstance(match, ParseResults):
                field_names = match.asDict()['field_names']
            elif isinstance(match, ParseException):
                raise_error(match, encoding)
            else:
                raise_error(Exception('Unknown error has occurred'))
        elif insert:
            c_warning('Getting field names from first insert')
            fields_only = re.search('(\(`.*`\))', insert).group(1)
            match = parse_sql(fields_only, INSERT_FIELDS)
            if match and isinstance(match, ParseResults):
                field_names = match.asDict().get('field_names')
            elif isinstance(match, ParseException):
                pass
            else:
                raise (Exception('Unknown error has occurred'))

        with io.open(filepath + '.csv', 'w', encoding=encoding) as cf:
            if field_names:
                c_success('Found field names')
                cf.write(','.join(field_names))
                cf.write(u'\n')
            else:
                c_error('No field names found')

        read_pbar = TqdmUpTo(desc='read', unit=' bytes',
                         total=os.path.getsize(filepath))
        read_pbar.update_to(byte_num)

        insert_pbar = TqdmUpTo(desc='processing', unit=' inserts')
        total_inserts += 1

        error = process_insert(filepath, encoding, insert)
        if not error:
            insert_pbar.update(1)

        for create_table, table_name, insert, byte_num in user_table:
            read_pbar.update_to(byte_num)
            error = process_insert(filepath, encoding, insert)
            if error:
                error_rate = bad_inserts / total_inserts
                # Consider the processing failed if over max failure rate
                if error_rate > MAX_FAILURE_RATE and total_inserts > 5:
                    print('{}: Error rate is too high'.format(filepath))
                    raise_error(error, encoding)
                else:
                    # write insert #, error msg, and insert
                    write_bad(filepath, total_inserts, error, insert)
            else:
                total_inserts += 1
                insert_pbar.update_to(total_inserts)


    read_pbar.close()
    insert_pbar.close()

    if not total_inserts:
        if not table_name:
            error = 'No matching user table found'
        else:
            error = 'No matching INSERT statements found'
        raise_error(ValueError(error))

    print('{}: Processed {} insert(s) and skipped {} errors'.format(
          filepath, total_inserts, bad_inserts))


def parse_sql(line, pattern):
    try:
        return pattern.parseString(line)
    except ParseException as pe:
        return pe


def process_insert(path, encoding, insert):
    match = re.search('^(?:.*)?(VALUES.*;)', insert)
    if match:
        value_only = match.group(1)
    else:
        return
    result = parse_sql(value_only, VALUES_ONLY)
    if result and isinstance(result, ParseResults):
        values_list = result.asDict()['values']
        with io.open(path + '.csv', 'a', encoding=encoding) as cf:
            for values in values_list:
                cf.write(','.join(
                    ['"%s"' % value for value in values]))
                cf.write(u'\n')
    else:
        return result


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
                    elif not create_table:
                        create = parse_sql(line, CREATE_BEGIN)
                        if create and isinstance(create, ParseResults):
                            if not table_name:
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


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
