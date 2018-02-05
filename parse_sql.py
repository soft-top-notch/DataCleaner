#!/usr/bin/env python2.7
"""Parse user data from SQL.

Called with filename argument for one or more sql files/dumps.  Will output
csv and move sql source file to completed or failed directories depending on
if there were any errors.

Filename argument can be a list of files or wildcard (*).

Usage:
    parse_sql.py [-hV] [--completed=DIR] [--failed=DIR] [--exit-on-error] SQLFILE...

Options:
    --completed=DIR               Directory to store completed sql files [default: completed]
    --exit-on-error               Exit on error, do not continue
    --failed=DIR                  Directory to store sql files with errors [default: failed]
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    parse_sql.py --completed='~/success' --failed='~/error' test.sql
    parse_sql.py --exit-on-error ~/samples/*.sql
"""
from __future__ import division, print_function
import attr
import codecs
import magic
import os
import sys
from docopt import docopt
from pyparsing import alphanums, CaselessKeyword, CaselessLiteral, \
    Combine, Group, NotAny, nums, Optional, oneOf, OneOrMore, \
    ParseException, ParseResults, quotedString, removeQuotes, \
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
USER_NAME = Combine(CaselessLiteral('user') + Optional(CaselessLiteral('s')))
MEMBER_NAME = Combine(
    CaselessLiteral('member') + Optional(CaselessLiteral('s')))
TABLE_NAME = (USER_NAME | MEMBER_NAME)
USER_TABLE = Combine(BACKTICK + Optional(Word(alphanums) + '_') + TABLE_NAME +
                     BACKTICK) + WordEnd()
CREATE = CaselessKeyword('CREATE TABLE')
CREATE_EXISTS = CaselessKeyword('IF NOT EXISTS')
CREATE_BEGIN = CREATE + Optional(CREATE_EXISTS) + USER_TABLE.setResultsName(
    'table_name')
CREATE_FULL = CREATE_BEGIN + CREATE_FIELDS

VALUE = Combine(
    Suppress(Optional(',')) + (realNum | intNum | alphanums
                               | quotedString.addParseAction(removeQuotes)
                               | 'NULL') + Suppress(Optional(',')))
VALUES = Suppress('(') + Group(OneOrMore(VALUE)) + Suppress(')') + Suppress(
    Optional(','))

# INSERT INTO
INSERT = CaselessKeyword('INSERT INTO')
INSERT_BEGIN = INSERT + USER_TABLE.setResultsName('table_name')
INSERT_FULL = INSERT_BEGIN + Optional(INSERT_FIELDS) + CaselessKeyword(
    'VALUES') + Group(OneOrMore(VALUES)).setResultsName('values')


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
                if isinstance(error, ParseException):
                    print(error)
                else:
                    print(error.message)
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
    # Current CreateTable object
    create_table = None
    # Current InsertInto object
    insert_into = None
    # List of insert statments
    inserts = []
    parsing = None
    line_count = 0
    progress = print_progress(filepath)
    m = magic.Magic(mime_encoding=True)
    encoding = m.from_file(filepath)

    # Extract the CREATE TABLE and INSERT INTO statements for the user table
    with codecs.open(filepath, encoding=encoding) as sqlfile:
        for line in sqlfile:
            line_count += 1
            progress('Analyzing line {}...'.format(line_count))
            # If not parsing a CREATE or INSERT statement, look for one
            if not parsing:
                insert = parse_sql(line, INSERT_BEGIN)
                if insert and isinstance(insert, ParseResults):
                    insert_into = InsertInto(line)
                    if insert_into.ending not in line:
                        parsing = insert_into
                    else:
                        inserts.append(insert_into.statement)
                elif not create_table:
                    create = parse_sql(line, CREATE_BEGIN)
                    if create and isinstance(create, ParseResults):
                        create_table = CreateTable(line)
                        if create_table.ending not in line:
                            parsing = create_table
            # Continue parsing the current statement
            else:
                parsing.statement += line
                if parsing.ending in line:
                    if isinstance(parsing, InsertInto):
                        inserts.append(parsing.statement)
                    parsing = None

    progress('Finished reading sql file')
    if not inserts:
        print()
        raise ValueError(
            'No INSERT statements found! Last statement parsed: %s' % parsing)

    # Extract data from statements and write to csv file
    with open(filepath + '.csv', 'w') as csvfile:
        if create_table:
            result = parse_sql(create_table.statement, CREATE_FULL)
            if result and isinstance(result, ParseResults):
                field_names = result.asDict()['field_names']
            elif isinstance(result, tuple):
                raise_error(result, create_table.statement)
            else:
                print()
                raise Exception('Unknown error has occurred')
        else:
            result = parse_sql(inserts[0], INSERT_FULL)
            if result and isinstance(result, ParseResults):
                field_names = result.asDict()['field_names']
            elif isinstance(result, tuple):
                raise_error(result, inserts[0])
            else:
                print()
                raise Exception('Unknown error has occurred')

        if not field_names:
            print()
            raise ValueError('Field Names not found!')

        csvfile.write(','.join(field_names))
        csvfile.write('\n')
        progress('Wrote csv field names')

        total_data_lines = 0
        total_inserts = len(inserts)
        bad_inserts = []
        error_rate = 0.00
        for num, insert in enumerate(inserts):
            insert_status = 'Processing insert {} of {}: '.format(
                num + 1, total_inserts)
            progress(insert_status)
            result = parse_sql(insert, INSERT_FULL)
            if result and isinstance(result, ParseResults):
                values_list = result.asDict()['values']
                for values in values_list:
                    csvfile.write(','.join(
                        '"{}"'.format(value) for value in values))
                    csvfile.write('\n')
                    total_data_lines += 1
                    progress(insert_status + ' wrote {} data line(s)...'.
                             format(total_data_lines))
            elif isinstance(result, tuple):
                error_rate = len(bad_inserts) / total_inserts
                # Consider the processing as failed if over max failure rate
                if error_rate > MAX_FAILURE_RATE or total_inserts < 5:
                    raise_error(result, insert)
                else:
                    # Append tuple: insert #, error msg, and insert
                    bad_inserts.append((num, result[1], insert))
            else:
                print()
                raise Exception('Unknown error has occurred')

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


def parse_sql(line, pattern):
    try:
        return pattern.parseString(line)
    except ParseException:
        return sys.exc_info()


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


def raise_error(exc_info, line):
    """Combine Exception error msg with last line processed."""
    if len(line) > 5000:
        line = line[0:5000] + '...OUTPUT CUT DUE TO LENGTH...'
    error_msg = '{}\n\nLine:\n{}\n'.format(exc_info[1], line)
    print()
    raise exc_info[0], error_msg, exc_info[2]


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
