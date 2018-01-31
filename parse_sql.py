#!/usr/bin/env python2.7
"""Parse user data from SQL.

Should be imported and used within datacleaner.py
"""
import csv
import re
import sys

INSERT_RE = re.compile('^INSERT INTO `?\w+`? \(((?:`[\w\s]+`,?\s*)+)\) VALUES')
VALUES_RE = re.compile('\(((?:\'?[^\'`]*\'?,?)+)\)')


def parse_sql(filename):
    """Opens sql file and parses INSERT statements, outputing data into CSV."""
    fields = []
    end_pattern = []
    data_count = 0
    with open(filename + '.csv', 'wb') as csvfile:
        csvwriter = csv.writer(
            csvfile, lineterminator='\n', quoting=csv.QUOTE_ALL, strict=True)
        with open(filename) as sqlfile:
            for line in sqlfile:
                values = []
                if not end_pattern:
                    insert_match = INSERT_RE.search(line)
                    if insert_match:
                        if not fields:
                            fields = re.sub('\`', '',
                                            insert_match.group(1)).split(',')
                            csvwriter.writerow(fields)
                        end_pattern.append(');')
                if end_pattern:
                    values_match = VALUES_RE.search(line)
                    if values_match:
                        values_no_quotes = re.sub('[\'\"]', '',
                                                  values_match.group(1))
                        values = re.sub(',\s+', ',',
                                        values_no_quotes).split(',')
                    if end_pattern[-1] in line:
                        end_pattern.pop()
                if values:
                    csvwriter.writerow(values)
                    data_count += 1
    # Return the number of fields and number of lines of data
    return len(fields), data_count


def main():
    fields, data_count = parse_sql(sys.argv[1])
    print '%s - %d fields and %d data line(s)' % (sys.argv[1], fields,
                                                  data_count)


if __name__ == '__main__':
    main()
