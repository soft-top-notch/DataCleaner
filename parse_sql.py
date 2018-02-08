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
import attr
import io
import magic
import os
import re
import sys
from docopt import docopt
from pyparsing import alphanums, CaselessKeyword, CaselessLiteral, \
    Combine, Group, NotAny, nums, Optional, oneOf, OneOrMore, \
    ParseException, ParseResults, quotedString, Regex, removeQuotes, \
    Suppress, Word, WordEnd, ZeroOrMore

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
USER_NAME = Regex(r'((?:[a-zA-Z0-9]+_?)?[uU]sers?)')
MEMBER_NAME = Regex(r'((?:[a-zA-Z0-9]+_?)?[mM]embers?)')
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
                print_progress(filepath)('ERROR: ' + str(error), end=True)
        else:
            move(filepath, args['--completed'])


def move(src, dest):
    """Moves source file into success or failed directories.

    Creates directory if needed.
    """
    filename = os.path.basename(src)
    if '~' in dest:
        dest_dir = os.path.expanduser(dest)
    else:
        dest_dir = dest

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    new_path = '{}/{}'.format(dest_dir, filename)
    if src != new_path:
        os.rename(src, new_path)


def parse(filepath):
    """Opens sql file and parses statements, outputing data into CSV."""
    field_names = []
    progress = print_progress(filepath)

    # Not sure what the encoding is, will try iso-8859-1
    encoding = 'iso-8859-1'
    retry = False
    try:
        create_table, inserts = read_file(filepath, encoding)
    except UnicodeDecodeError:
        retry = True
    if retry:
        # Try detecting encoding
        m = magic.Magic(mime_encoding=True)
        encoding = m.from_file(filepath)
        try:
            create_table, inserts = read_file(filepath, encoding)
        except UnicodeDecodeError:
            print('Unable to determine encoding of file')
            raise

    progress('Finished reading sql file')
    if not inserts:
        error = 'No INSERT statements found!'
        raise_error(ValueError(error))

    # Extract data from statements and write to csv file
    with io.open(filepath + '.csv', 'w', encoding=encoding) as csvfile:
        if create_table:
            progress('Getting column names from create table')
            result = parse_sql(create_table.statement, CREATE_FULL)
            if result and isinstance(result, ParseResults):
                field_names = result.asDict()['field_names']
            elif isinstance(result, ParseException):
                raise_error(result, encoding)
            else:
                raise_error(Exception('Unknown error has occurred'))
        else:
            if inserts:
                progress('Getting column names from first insert')
                result = parse_sql(inserts[0], INSERT_FIELDS, True)
                if result and isinstance(result, ParseResults):
                    field_names = result.asDict().get('field_names')
                elif isinstance(result, ParseException):
                    pass
                else:
                    raise (Exception('Unknown error has occurred'))

        if field_names:
            csvfile.write(','.join(field_names))
            csvfile.write(u'\n')
            progress('Wrote csv field names')
        else:
            csvfile.write(u'###### NO HEADERS FOUND ######\n')
            progress('Warning! No field names found')

        total_data_lines = 0
        total_inserts = len(inserts)
        bad_inserts = []
        error_rate = 0.00
        for num, insert in enumerate(inserts):
            insert_status = 'Processing insert {} of {}: wrote {} data line(s)...'.format(
                num + 1, total_inserts, total_data_lines)
            progress(insert_status)
            match = re.search('^(?:.*)?(VALUES.*;)', insert)
            if match:
                value_only = match.group(1)
            else:
                continue
            result = parse_sql(value_only, VALUES_ONLY, True)
            if result and isinstance(result, ParseResults):
                values_list = result.asDict()['values']
                for values in values_list:
                    csvfile.write(','.join(
                        ['"%s"' % value for value in values]))
                    csvfile.write(u'\n')
                    total_data_lines += 1
            elif isinstance(result, ParseException):
                error_rate = len(bad_inserts) / total_inserts
                # Consider the processing as failed if over max failure rate
                if error_rate > MAX_FAILURE_RATE or total_inserts < 5:
                    progress('Error rate is too high', end=True)
                    raise_error(result, encoding)
                else:
                    # Append tuple: insert #, error msg, and insert
                    bad_inserts.append((num, result, insert))
            else:
                raise (Exception('Unknown error has occurred'))

        if bad_inserts:
            with open(filepath + '.bad_inserts.txt', 'w') as bi:
                error_percentage = error_rate * 100
                bi.write('##### Insert Errors #####\n')
                bi.write('Error rate: {0:.2f}%\n\n'.format(error_percentage))
                for bad_insert in bad_inserts:
                    num, error, insert = bad_insert
                    bi.write('******\n')
                    bi.write('Insert #{}\n'.format(num + 1))
                    bi.write('Error:{}\n'.format(error))
                    bi.write('\nLine:\n{}\n\n'.format(insert))
        progress(
            'Wrote {} total lines(s) and skipped {} errors'.format(
                total_data_lines + 1, len(bad_inserts)),
            end=True)


def parse_sql(line, pattern, search=False):
    try:
        return pattern.parseString(line)
    except ParseException as pe:
        return pe


def print_progress(filepath):
    def progress(data, end=False):
        filename = os.path.basename(filepath)
        msg = '{}: {}'.format(filename, data)
        if end:
            print(msg + ' ' * 50)
        else:
            msg += '\r\r'
            sys.stdout.write(msg)
            sys.stdout.flush()

    return progress


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


def read_file(filepath, encoding):
    # Current CreateTable object
    create_table = None
    # List of insert statments
    inserts = []
    # Current InsertInto object
    line_count = 0
    parsing = None
    progress = print_progress(filepath)
    # Extract the CREATE TABLE and INSERT INTO statements for the user table
    with io.open(filepath, 'Ur', encoding=encoding) as sqlfile:
        while True:
            buffer = 10485760
            lines = sqlfile.readlines(buffer)
            if not lines:
                break
            for line in lines:
                line_count += 1
                progress('Analyzing line {}...'.format(line_count))
                # If not parsing a CREATE or INSERT statement, look for one
                if parsing:
                    # Continue parsing the current statement
                    parsing.statement.append(line)
                    if parsing.ending in line:
                        no_newlines = rm_newlines(parsing.statement)
                        if isinstance(parsing, CreateTable):
                            create_table.statement = ''.join(no_newlines)
                        elif isinstance(parsing, InsertInto):
                            inserts.append(''.join(no_newlines))
                        parsing = None
                else:
                    insert = parse_sql(line, INSERT_BEGIN)
                    if insert and isinstance(insert, ParseResults):
                        insert_into = InsertInto([line])
                        if insert_into.ending in line:
                            no_newlines = rm_newlines([line])
                            inserts.append(''.join(no_newlines))
                        else:
                            parsing = insert_into
                            continue
                    elif create_table:
                        value = parse_sql(line, VALUES_ONLY)
                        if value and isinstance(value, ParseResults):
                            value_only = InsertInto([line])
                            if value_only.ending in line:
                                no_newlines = rm_newlines([line])
                                inserts.append(''.join(no_newlines))
                            else:
                                parsing = value_only
                                continue
                    else:
                        create = parse_sql(line, CREATE_BEGIN)
                        if create and isinstance(create, ParseResults):
                            create_table = CreateTable([line])
                            if create_table.ending not in line:
                                parsing = create_table
    return create_table, inserts


def rm_newlines(lines):
    return [x.replace('\n', '').replace('\r', '') for x in lines]


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
