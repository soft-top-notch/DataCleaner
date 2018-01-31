#!/usr/bin/env python2.7
"""Parse user data from SQL.

Should be imported and used within datacleaner.py
"""
import re
import sys

INSERT_RE = re.compile('^INSERT INTO `?\w+`? \(((?:`[\w\s]+`,?\s*)+)\) VALUES')
VALUES_RE = re.compile('\(((?:\'?[^\'`]*\'?,?)+)\)')


def clean_line(line):
    """Removes quotes while perserving included special characters.

    Returns sanitized list of elements.
    """
    data = []
    element = ''
    quotes = []
    for char in line:
        if char in "'":
            if quotes:
                if char == quotes[-1]:
                    quotes.pop()
                else:
                    quotes.append(char)
            else:
                quotes.append(char)
        elif char in ' ' and not element:
            pass
        elif char == ',' and not quotes:
            data.append(element)
            element = ''
        else:
            element += char
    return data


def parse_sql(filename):
    """Opens sql file and parses INSERT statements, outputing data into CSV."""
    fields = []
    end_pattern = []
    data_count = 0
    with open(filename + '.csv', 'w') as csvfile:
        with open(filename) as sqlfile:
            for line in sqlfile:
                values = []
                if not end_pattern:
                    insert_match = INSERT_RE.search(line)
                    if insert_match:
                        if not fields:
                            fields = re.sub('\`', '',
                                            insert_match.group(1)).split(',')
                            csvfile.write(','.join('"{0}"'.format(field)
                                                   for field in fields) + '\n')
                        end_pattern.append(');')
                if end_pattern:
                    values_match = VALUES_RE.search(line)
                    if values_match:
                        values = clean_line(values_match.group(1))
                    if end_pattern[-1] in line:
                        end_pattern.pop()
                if values:
                    csvfile.write(','.join('"{0}"'.format(value)
                                           for value in values) + '\n')
                    data_count += 1
    # Return the number of fields and number of lines of data
    return len(fields), data_count


def main():
    fields, data_count = parse_sql(sys.argv[1])
    print '%s - %d fields and %d data line(s)' % (sys.argv[1], fields,
                                                  data_count)


if __name__ == '__main__':
    main()
