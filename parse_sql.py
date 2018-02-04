#!/usr/bin/env python2.7
"""Parse user data from SQL.

Should be imported and used within datacleaner.py
"""
from __future__ import print_function
import attr
import os
import sys
from pyparsing import alphanums, CaselessKeyword, CaselessLiteral, \
    Combine, Group, NotAny, nums, Optional, oneOf, OneOrMore, \
    ParseException, ParseResults, quotedString, removeQuotes, \
    Suppress, Word, WordEnd, ZeroOrMore

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


def main():
    """Executes main code."""
    try:
        parse(sys.argv[1])
    except KeyboardInterrupt:
        print('Control-c pressed...')
        sys.exit(138)


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

    # Extract the CREATE TABLE and INSERT INTO statements for the user table
    with open(filepath) as sqlfile:
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
        insert_len = len(inserts)
        for num, insert in enumerate(inserts):
            insert_status = 'Processing insert {} of {}: '.format(
                num + 1, insert_len)
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
                raise_error(result, insert)
            else:
                print()
                raise Exception('Unknown error has occurred')

        progress(
            'Wrote {} total lines(s)'.format(total_data_lines + 1), end=True)


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
    main()
