#!/usr/bin/env python2.7
"""Parse user data from SQL.

Should be imported and used within datacleaner.py
"""
from __future__ import print_function
import attr
import sys
from pyparsing import alphanums, alphas, CaselessKeyword, CaselessLiteral, \
    Combine, delimitedList, Group, NotAny, nums, Optional, OneOrMore, \
    ParseException, ParseResults, quotedString, removeQuotes, Suppress, Word, \
    ZeroOrMore

# pyparsing patterns for matching/parsing SQL
BACKTICK = Suppress(Optional('`'))

# Field Names
KEYS = (CaselessKeyword('PRIMARY') | CaselessKeyword('UNIQUE')
        | CaselessKeyword('KEY'))
FIELD_TYPE = Combine(
    Word(alphas) + Optional(Word('(') + Word(nums) + Word(')')))

FIELD_SETTING = ZeroOrMore(Word(alphas + '_'))
FIELD_DEFAULT = quotedString
FIELD_NAME = BACKTICK + NotAny(KEYS) + Word(
    alphanums + '_'
) + BACKTICK + Suppress(
    Optional(FIELD_TYPE) + Optional(FIELD_SETTING) + Optional(FIELD_DEFAULT))
FIELDS = Suppress('(') + Group(delimitedList(FIELD_NAME)).setResultsName(
    'field_names') + Suppress(Optional(KEYS) + Optional(')'))

# CREATE TABLE
USER_TABLE = BACKTICK + Combine(
    Optional(Word(alphanums) + Word('_')) + CaselessLiteral('users')) + \
    BACKTICK
CREATE = CaselessKeyword('CREATE TABLE')
CREATE_EXISTS = CaselessKeyword('IF NOT EXISTS')
CREATE_BEGIN = CREATE + Optional(CREATE_EXISTS) + USER_TABLE.setResultsName(
    'table_name')
CREATE_FULL = CREATE_BEGIN + FIELDS

VALUE = Combine((nums | quotedString.addParseAction(removeQuotes)
                 | 'NULL') + Optional(','))
VALUES = Suppress('(') + OneOrMore(VALUE).setResultsName('values') + \
         Suppress(')')

# INSERT INTO
INSERT = CaselessKeyword('INSERT INTO')
INSERT_BEGIN = INSERT + USER_TABLE.setResultsName('table_name')
INSERT_FULL = INSERT_BEGIN + Optional(FIELDS) + \
              CaselessKeyword('VALUES') + VALUES


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


def parse(filename):
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

    # Extract the CREATE TABLE and INSERT INTO statements for the user table
    with open(filename) as sqlfile:
        for line in sqlfile:
            line_count += 1
            print_progress('Reading line {}...'.format(line_count))
            # If not parsing a CREATE or INSERT statement, look for one
            if not parsing:
                if not create_table:
                    create = parse_sql(line, CREATE_BEGIN)
                    if create and isinstance(create, ParseResults):
                        create_table = CreateTable(line)
                        if create_table.ending not in line:
                            parsing = create_table
                else:
                    insert = parse_sql(line, INSERT_BEGIN)
                    if insert and isinstance(insert, ParseResults):
                        insert_into = InsertInto(line)
                        if insert_into.ending not in line:
                            parsing = insert_into
                        else:
                            inserts.append(insert_into.statement)
            # Continue parsing the current statement
            else:
                parsing.statement += line
                if parsing.ending in line:
                    if isinstance(parsing, InsertInto):
                        inserts.append(parsing.statement)
                    parsing = None

    print_progress('Finished reading sql file')
    if not inserts:
        print()
        raise ValueError('No INSERT statements found!')

    # Extract data from statements and write to csv file
    with open(filename + '.csv', 'w') as csvfile:
        if create_table:
            result = parse_sql(create_table.statement, CREATE_FULL)
            if result and isinstance(result, ParseResults):
                print()
                print(create_table.statement)
                field_names = result.asDict()['field_names']
            elif isinstance(result, tuple):
                raise_error(result, create_table.statement)
            else:
                print()
                raise Exception('Unknown error has occurred')
        else:
            print('ERROR: No CREATE TABLE statement matched!')
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
        print_progress('Wrote csv field names')

        inserts_len = len(inserts)
        for entry, insert in enumerate(inserts):
            result = parse_sql(insert, INSERT_FULL)
            if result and isinstance(result, ParseResults):
                values = result.asDict()['values']
                csvfile.write(','.join(
                    '"{}"'.format(value) for value in values))
                csvfile.write('\n')
                print_progress('Wrote {} of {} inserts...'.format(
                    entry + 1, inserts_len))
            elif isinstance(result, tuple):
                raise_error(result, insert)
            else:
                print()
                raise Exception('Unknown error has occurred')

        print_progress('Wrote {} insert(s)'.format(inserts_len + 1), end=True)


def parse_sql(line, pattern):
    try:
        return pattern.parseString(line)
    except ParseException:
        return sys.exc_info()


def print_progress(data, end=False):
    msg = '{}: {}\r\r'.format(sys.argv[1], data)
    if end:
        print()
        print(msg)
    else:
        sys.stdout.write(msg)
        sys.stdout.flush()


def raise_error(exc_info, line):
    """Combine Exception error msg with last line processed."""
    if len(line) > 5000:
        line = line[0:5000] + '...LINE CUT...'
    error_msg = '{}\n\nLine:\n{}\n'.format(exc_info[1], line)
    print()
    raise exc_info[0], error_msg, exc_info[2]


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    main()
