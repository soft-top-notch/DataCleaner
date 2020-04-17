#!/usr/bin/env python
import StringIO
import argparse
import cStringIO
import codecs
import csv
import io
import json
import os
import re
import sys
import glob

from shutil import copyfile
from collections import Counter
from validate_email import validate_email
from copy import deepcopy

import parse_sql
from dc import gather_files, move, c_failure, c_success, c_action, c_action_info, c_action_system, c_sys_success,\
    c_warning, c_darkgray, c_darkgreen, c_lightgreen, c_lightgray, c_lightblue, c_blue,\
    TqdmUpTo
from dc.sampling import create_sample

# Full path to directories used
DIRS = {
    'clean_fail':
    'fail',
    'clean_success':
    'done',
    'headers_skip':
    'headers_skip',
    'headers_success':
    'success',
    'json_done':
    'csv_complete',
    'json_success':
    'success',
    'sample':
    'samples',
    'skipped': ('completed', 'error', 'failed', 'fail', 'done', 'success',
                'headers_skip'),
    'sql_fail':
    'sql_fail',
    'sql_success':
    'success'
}

# Entries to be skipped when writing JSON.  Case insensitive, and will also
# match entries that are surrounded by a single charactor (#, <>, etc)
JSON_ENTRIES_SKIP = ('null', 'blank', 'xxx')

# Parts of filename to be removed when cleaned
UNWANTED = ('_cleaned', '_dump')

# Parts of filename not to include in release name
UNWANTED_RELEASE = set(UNWANTED)
UNWANTED_RELEASE.update({
    '_part1', '_part2', '_part3', '_2017', '_cleaned', '_error', '_vb-2017', '_parsed', '_bcrypt',
    '-vb-2016', '_vb___17', '_vb___16', '_p2', '_p3', '_p4', '-2016', '-2017', '.sql', '_md5crypt', 'bcrypt'
})

# Path to headers
BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)
HEADER_PATH = os.path.join(
    BASE_DIR,
    "headers/headers.json"
)


# Headers matched
with open(HEADER_PATH, "r") as file:
    HEADERS = json.loads(
        file.read()
    )

# Abbreviated headers that are enumerated
ENUMERATED = ('x', 'a', 'i')

# Abbreviations to headers
ABBR2HEADER = {
    abbr: header[0].lower() for abbr, header in HEADERS.items()
}

# Headers to abbreviations
HEADER2ABBR = {
    header.lower(): abbr for abbr, headers in HEADERS.items()
    for header in headers
}

csv.field_size_limit(sys.maxsize)

parser = argparse.ArgumentParser()
exclusive_args = parser.add_mutually_exclusive_group()
parser.add_argument(
    "-a", help="Don't ask if delimiter is guessed", action="store_true")
exclusive_args.add_argument(
    "-ah",
    help="Ask for headers to add to CSVs. No file cleaning.",
    action="store_true")
parser.add_argument("-c", type=int, help="Number of columns")

exclusive_args.add_argument(
    "-cl",
    help="Cleanse filename(s) of unwanted text. No file cleaning.",
    action="store_true")
parser.add_argument("-d", type=str, help="Delimiter")
exclusive_args.add_argument(
    "-j", help="Write JSON file. No file cleaning.", action="store_true")
parser.add_argument(
    "-m", help="Merge remaining columns into last", action="store_true")
parser.add_argument(
    "-o", help="Organize CSVs by column number", action="store_true")
parser.add_argument(
    "-p", help="Pass if delimiter can't guessed", action="store_true")
parser.add_argument(
    "-jm", help="Merge similar csv into list json", action="store_true")
parser.add_argument(
    "path",
    type=str,
    nargs='*',
    help="Path to one or more csv file(s) or folder(s)",
)
parser.add_argument("-r", type=str, help="Release Name")
exclusive_args.add_argument(
    "-s",
    help="Create sample of csv(s). No file cleaning.",
    action="store_true")
parser.add_argument(
    "-sci",
    type=float,
    default=3.0,
    help="Sampling Confidence interval (float) [default: 3.0]")
parser.add_argument(
    "-scl",
    type=int,
    default=95,
    help="Sampling Confidence level required (percent) [default: 95]")
exclusive_args.add_argument(
    "-sh",
    type=str,
    help="Specify headers to use for multiple files. No file cleaning.")
args = parser.parse_args()

if (args.c and (not args.d)) or (not args.c and args.d):
    print "Warning: Argument -c and -d should be used together"
    sys.exit(0)

guess = True
if args.c and args.d:
    guess = False

delims = ('\t', ' ', ';', ',', '|', ':')


def valid_ip(address):
    try:
        host_bytes = address.split('.')
        valid = [int(b) for b in host_bytes]
        valid = [b for b in valid if b >= 0 and b <= 255]
        return len(host_bytes) == 4 and len(valid) == 4
    except:
        return False


re_phone1 = re.compile('\d{3}-\d{3}-\d{3}')
re_phone2 = re.compile('\d{9}')
re_phone3 = re.compile('\+\d{4}-\d{3}-\d{4}')


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """

    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")


class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def tell(self):
        return self.stream.tell()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class myDialect(csv.Dialect):
    delimiter = ','
    quotechar = '"'
    lineterminator = '\n'
    skipinitialspace = False
    quoting = csv.QUOTE_ALL
    doublequote = False
    escapechar = '\\'


class excelDialect(csv.excel):
    quoting = csv.QUOTE_NONE
    quotechar = ''
    escapechar = "\\"


def process_dialect_row(row):
    pass


def find_column_count(f, dialect=csv.excel, show_freq=False):
    """Find the column count with the most occurences in 1000 lines."""
    row_lengths = []

    # Test 1000 first row
    index = 0
    while index < 1000:
        try:
            line = f.readline()
            line_buffer = StringIO.StringIO()
            line_buffer.write(line)
            line_buffer.seek(0)
            reader = UnicodeReader(line_buffer, dialect=dialect)
            row = reader.next()
            row_lengths.append(len(row))
            index += 1
        except Exception as err:
            break

    counts = Counter(row_lengths)
    column_count = counts.most_common()[0][0]
    column_freq = counts.most_common()[0][1]
    if column_count == 0:
        try:
            column_count = counts.most_common(2)[1][0]
            column_freq = counts.most_common(2)[1][1]
        except (ValueError, AttributeError, TypeError, IndexError):

            # If not found test the whole file
            while True:
                try:
                    line = f.readline()
                    line_buffer = StringIO.StringIO()
                    line_buffer.write(line)
                    line_buffer.seek(0)
                    reader = UnicodeReader(line_buffer, dialect=dialect)
                    row = reader.next()
                    row_lengths.append(len(row))
                except Exception as err:
                    break

            counts = Counter(row_lengths)
            column_count = counts.most_common()[0][0]
            column_freq = counts.most_common()[0][1]
            if column_count == 0:
                column_count = counts.most_common(2)[-1][0]
                column_freq = counts.most_common(2)[-1][1]

    if show_freq:
        return column_count, column_freq

    return column_count


def guess_delimeter_by_csv(F):
    # Init stuffs
    sniffer = csv.Sniffer()

    first_read = True
    sample_delims = deepcopy(delims)
    while True:
        F.seek(0)
        if first_read:
            sample_text = F.read(1024 * 100)
        else:
            sample_text = F.read()

        # Try sniff quote all
        all_dialect_variables = [None, 0, 0]
        try:
            all_dialect = sniffer.sniff(sample_text, delimiters=sample_delims)
            F.seek(0)
            all_column_count, all_column_freq = find_column_count(F, all_dialect, show_freq=True)
            all_dialect_variables = [all_dialect, all_column_count, all_column_freq]
        except Exception as err:
            pass

        # Try sniff quote all
        none_dialect_variables = [None, 0, 0]
        try:
            none_dialect = sniffer.sniff(sample_text, delimiters=sample_delims)
            none_dialect.quoting = csv.QUOTE_NONE
            F.seek(0)
            none_column_count, none_column_freq = find_column_count(F, none_dialect, show_freq=True)
            none_dialect_variables = [none_dialect, none_column_count, none_column_freq]
        except Exception as err:
            pass

        if none_dialect_variables[1] <=1 and all_dialect_variables[1] <=1:
            # Total ignore delims pool
            ignore_delims = []
            if all_dialect_variables[0]:
                ignore_delims.append(all_dialect_variables[0].delimiter)
            if none_dialect_variables[0]:
                ignore_delims.append(none_dialect_variables[0].delimiter)

            # Reload sample delims
            sample_delims = [
                delim for delim in sample_delims
                if delim not in ignore_delims
            ]

            if not sample_delims:
                return

            first_read = False

            continue

        break

    if all_dialect_variables[0] is None and none_dialect_variables[0] is None:
        return

    if float(all_column_freq + all_column_count)/2 >= float(none_column_count + none_column_freq)/2:
        return all_dialect_variables

    return none_dialect_variables


def ask_user_for_delimeter():

    csv_delimeter = raw_input(
        "Please idetify delimeter to be used for parsing csv file: ")
    csv_column_count = raw_input("Please identify column number: ")

    return csv_delimeter, int(csv_column_count)


def strip_delimeter(ls, csv_delimeter):

    while True:
        if ls:
            if ls[0] == csv_delimeter:
                ls = ls[1:]
            else:
                break
        break

    while True:
        if ls:
            if ls[-1] == csv_delimeter:
                ls = ls[:-1]
            else:
                break
        else:
            break

    return ls


def guess_delimeter(F):

    # Load sniff guess for compare
    sniff_guess = guess_delimeter_by_csv(F)

    # Load normal algorithm delimiter check for compare
    delim_counts_list = {}
    delim_freq = {}
    for d in delims:
        delim_counts_list[d] = []
        delim_freq[d] = {}

    x = 0

    F.seek(0)

    for l in F:
        if x >= 1000:
            break
        for d in delims:
            ls = l.strip()
            ls = strip_delimeter(ls, d)
            cnt = ls.count(d)
            delim_counts_list[d].append(cnt)
            x += 1

    most_frequent = (None, 0, 0)

    for d in delims:
        for c in delim_counts_list[d]:
            if c:
                if c in delim_freq[d]:
                    delim_freq[d][c] += 1
                else:
                    delim_freq[d][c] = 1

    for d in delim_freq:
        for c in delim_freq[d]:
            delim_average = (c + delim_freq[d][c]) / 2
            freq_average = (most_frequent[1] + most_frequent[2]) / 2
            if delim_average > freq_average:
                most_frequent = (d, delim_freq[d][c], c)

    csv_delimeter = most_frequent[0]
    rdialect = excelDialect()
    rdialect.delimiter = csv_delimeter

    if (sniff_guess
            and ((((sniff_guess[1] + sniff_guess[2])/2) > ((most_frequent[1] + most_frequent[2])/2))
                 or (most_frequent[1] == 0))):

        csv_delimeter = sniff_guess[0].delimiter
        rdialect = sniff_guess[0]

    F.seek(0)

    try:
        csv_column_count = find_column_count(F, rdialect)
    except IndexError:
        raise ValueError("Blank file, ignoring.")

    if csv_delimeter:

        print "\033[38;5;244mGuess method: Custom delimiter -> {}\n".format(
            csv_delimeter)
    elif not args.p:
        print "\033[38;5;203m Delimiter could not determined"
        csv_delimeter, csv_column_count = ask_user_for_delimeter()
        rdialect = excelDialect()
        rdialect.delimiter = csv_delimeter
        return rdialect, csv_column_count
    else:
        print "\033[38;5;203m Delimiter could not determined, passing"
        return False, False

    if args.a:
        return rdialect, csv_column_count

    print "\033[38;5;147m Guessed delimeter -> {}".format(repr(csv_delimeter))
    print "\033[38;5;147m Guessed column number", csv_column_count

    confirmed = confirm()
    if confirmed:
        return rdialect, csv_column_count
    else:
        csv_delimeter, csv_column_count = ask_user_for_delimeter()

        rdialect = excelDialect()
        rdialect.delimiter = csv_delimeter

        return rdialect, csv_column_count


def clean(e):
    while True:
        if e:
            if e[0] == '"' and e[-1] == '"':
                e = e[1:-1]
            else:
                break
        else:
            break

    return e.strip()


def clean_fields(l):
    return [clean(x) for x in l]


def clean_filename(source):
    """Remove unwanted parts of a filename."""
    source_filename = os.path.basename(source)
    print 'Checking filename "{}" for conformity...'.format(source_filename)
    new_name = source
    for unwanted in UNWANTED:
        new_name = new_name.replace(unwanted, '')
    if source != new_name:
        print 'Renaming {} to {}'.format(source_filename,
                                         os.path.basename(new_name))
        os.rename(source, new_name)


def data_prep(source):
    """Clean/refactor source dictionary."""
    # Consolidate address entries to 'a' field
    full_addy = ''
    for num in xrange(0, 9):
        addy_header = 'a' + str(num)
        addy = source.get(addy_header)
        if addy:
            full_addy += ' {}'.format(addy.strip())
            del source[addy_header]
    if full_addy:
        source['a'] = full_addy

    # Consolidate name fields
    if source.get('fn') or source.get('ln'):
        source['n'] = '{} {}'.format(
            source.pop('fn', ''), source.pop('ln', '')).strip()

    # Rename 'd' date of birth field to 'dob'
    if source.get('d'):
        source['dob'] = source.pop('d').strip()

    # Split out domain from email address
    if source.get('e'):
        email = source.get('e')
        if '@@' in email:
            email = '@'.join(email.split('@@'))
        if validate_email(email):
            source['e'] = email
            source['d'] = email.split('@')[-1]
        else:
            del source['e']

    # Remove unwanted fields/values
    for header, value in source.items():
        # Remove misc headers (x[0-9]) and entries with empty values
        if re.search('^x(?:\d+)?$', header) or not value:
            del source[header]
        # Remove entries that are in JSON_ENTRIES_SKIP
        elif found_in(value, JSON_ENTRIES_SKIP):
            del source[header]
        # Remove extra spaces at start or end
        else:
            source[header] = value.strip()
    return source


def found_in(value, array):
    for item in array:
        if re.match('[<#]?{}[#>]?$'.format(item), value, flags=re.IGNORECASE):
            return True
    return False


def wrap_fields(l, wrapper='"'):
    return ['{0}{1}{0}'.format(wrapper, x) for x in l]


def write_json(source):
    print "Writing json file for", source
    fbasename = os.path.basename(source)
    json_file = os.path.splitext(fbasename)[0] + '.json'

    if not os.path.exists(DIRS['json_success']):
        os.mkdir(DIRS['json_success'])

    out_reader = UnicodeReader(open(source), dialect=myDialect)

    # grab the first line as headers
    headers = out_reader.next()

    with open(os.path.join(DIRS['json_success'], json_file), 'w') as outfile:
        # Add first line of json
        line_count = 0
        pbar = TqdmUpTo(desc='Writing JSON', unit=' row')

        for row in out_reader:
            # If this is not the first row, add a new line
            if line_count > 0:
                outfile.write('\n')

            source = data_prep(dict(zip(headers, row)))

            # Set release name
            if args.r:
                source['r'] = args.r
            # Use filename without extension as release name
            else:
                filename = os.path.splitext(fbasename)[0]
                for undesirable in UNWANTED_RELEASE:
                    filename = filename.replace(undesirable, '')
                filename = filename.replace('_', ' ')
                source['r'] = filename

            data = {'_source': source}

            outfile.write(json.dumps(data))
            line_count += 1
            pbar.update(1)

        # Write final newline (no comma) and close json brackets
        outfile.write('\n')
        pbar.close()


def get_headers(csv_file, delimiter, column_count):
    """Reads file and tries to determine if headers are present.

    Returns a list of headers.
    """
    headers = []
    starting_location = csv_file.tell()

    while True:
        line = csv_file.readline()
        # Skip comment rows
        if line.startswith('#'):
            continue

        lowerrow = [
            cc.lower().replace('\n', '') for cc in line.split(delimiter)
        ]

        # Set them enumerated headers to zero
        tracked = {}
        for track in ENUMERATED:
            tracked[track] = 0

        for i in lowerrow:
            i = i.strip()
            # Match headers in double quotes on both sides or no double quotes
            matches = re.search('"(\w+)"|\'(\w+)\'|^(\w+)$', i)

            if matches:
                # Take whichever one matched
                field_name = matches.group(1) or matches.group(2) or matches.group(3)
                # match if this is an enumerated field
                enumerated = re.search('^([a-z])\d+', field_name)
                if enumerated:
                    header = enumerated.group(1)
                # match if it is a known abbreviation for a header
                elif ABBR2HEADER.get(field_name):
                    header = field_name
                else:
                    # Make it the header abbreviation or make it misc (x)
                    header = HEADER2ABBR.get(field_name, 'x')
                if header in ENUMERATED:
                    if tracked[header]:
                        header_format = '{}{}'.format(header, tracked[header])
                        tracked[header] += 1
                        header = header_format
                    else:
                        tracked[header] += 1
                headers.append(header)
            else:
                csv_file.seek(starting_location)
                break
        # Only check the first non-comment row
        break

    if len(headers) + 1 == column_count:
        headers.append("x")

    if len(headers) == column_count:
        if 's' in headers and 'p' in headers:
            idx = headers.index('p')
            headers[idx] = 'h'
        return headers
    else:
        return []


def ask_headers(column_count):
    """Ask user for headers.

    Returns a list of headers.
    """
    headers = []

    while True:
        print "Please provide the headers below:"
        seen = []
        for shortened, full_header in HEADERS.items():
            if shortened not in seen:
                print '{}:{}'.format(
                    shortened,
                    ", ".join(full_header)
                )
                seen.append(shortened)

        user_headers = raw_input(
            "Please enter {} headers as their abbreviation (i.e. u p x): ".
            format(column_count))

        if user_headers:
            user_headers = user_headers.strip().split(' ')
            header_len = len(user_headers)
            if 'zz' in user_headers:
                return headers
            blank_header = False
            for header in user_headers:
                if not header:
                    blank_header = True
                    c_failure(
                        '\nError: One of the provided headers was blank or'
                        ' more than one space was used between headers')
            if not blank_header:
                if header_len == column_count:
                    break
                else:
                    c_failure(
                        '\nERROR: {} headers entered for {} columns\n'.format(
                            header_len, column_count))
        else:
            print '\nERROR: No headers entered\n'

    # Set them enumerated headers to zero
    tracked = {}
    for track in ENUMERATED:
        tracked[track] = 0

    for hi in range(column_count):
        if hi < header_len:
            header = user_headers[hi]
            if header in ENUMERATED:
                if tracked[header]:
                    header_format = '{}{}'.format(header, tracked[header])
                    tracked[header] += 1
                    header = header_format
                else:
                    tracked[header] += 1
            headers.append(header)

    return headers


def remove_quote(values):
    # All possible quote chars
    possible_quote_chars = ["'", "\""]

    # Loop valid count
    valid_count = 0
    final_values = []
    for index, value in enumerate(values):
        # Not check value with less than 2 chars
        if len(value) < 2:
            final_values.append(value)
            continue

        # Check double quote
        if value[0] in possible_quote_chars and value[-1] == value[0]:
            value = value[1:-1]

        final_values.append(value)

    return final_values


def parse_file(tfile):

    org_tfile = tfile

    tfile = org_tfile.replace('(', '')
    tfile = tfile.replace(')', '')
    tfile = tfile.replace(' ', '')
    tfile = tfile.replace(',', '')

    os.rename(org_tfile, tfile)

    f_name, f_ext = os.path.splitext(tfile)

    c_action_system('   Escaping garbage characters')

    gc_file = "{0}_gc~".format(tfile)

    gc_cmd = "tr -cd '\11\12\15\40-\176' < {} > {}".format(tfile, gc_file)

    os.system(gc_cmd)

    #c_action_system('   Parsing file: {}'.format(gc_file))

    F = open(gc_file, 'rb')
    dialect = None

    if guess:
        #c_action_system('   Guessing delimiter \n')
        try:
            dialect, csv_column_count = guess_delimeter(F)
        except TypeError as e:
            print "Guessing delimiter failed, aborting."
        except ValueError as e:
            print "Blank file, aborting."

    if not dialect and args.p:
        return
    elif not dialect:
        dialect = csv.excel
        dialect.delimiter = args.d.decode('string_escape')
        csv_column_count = args.c

    c_sys_success('Using column number [{}] and delimiter [{}]\n'.format(
        csv_column_count, repr(dialect.delimiter)))

    F.seek(0)

    out_file_csv_name = f_name + '_cleaned.csv'
    out_file_csv_temp = out_file_csv_name + '~'
    out_file_err_name = f_name + '_error.csv'
    out_file_err_temp = out_file_err_name + '~'

    out_file_csv_file = open(out_file_csv_temp, 'wb')

    l_count = 0
    headers = set_headers(F, dialect, csv_column_count)
    if headers:
        write_headers(out_file_csv_file, headers)
        l_count += 1

    pbar = TqdmUpTo(
        desc='Writing ', unit=' bytes ', total=os.path.getsize(gc_file))

    # Load result files
    error_file = io.open(out_file_err_temp, 'w', encoding='utf-8')

    # Load clean dialect
    clean_dialect = myDialect()

    # Init clean write
    clean_writer = UnicodeWriter(out_file_csv_file, dialect=clean_dialect)

    # Loop line
    for line in F:
        l_count += 1

        cleaned_row, failed_row = parse_row(line, csv_column_count, dialect)

        if failed_row:
            error_file.write(unicode(failed_row))
        else:
            clean_writer.writerow(
                remove_quote(cleaned_row)
            )

        pbar.update_to(clean_writer.tell() + error_file.tell())

    pbar.close()

    F.close()
    out_file_csv_file.close()
    error_file.close()

    output_stats = os.stat(out_file_csv_temp)
    errors_stats = os.stat(out_file_err_temp)

    c_action_info('Output file {} had {} bytes written/'.format(
        out_file_csv_temp, output_stats.st_size))
    c_action_info('Error file {} had {} bytes written/'.format(
        out_file_err_temp, errors_stats.st_size))

    if headers:
        move(tfile, DIRS['headers_success'])
    else:
        move(tfile, DIRS['clean_success'])

    if errors_stats.st_size > 0:
        move(out_file_err_temp, DIRS['clean_fail'])
    else:
        os.remove(out_file_err_temp)

    if os.path.exists(out_file_csv_temp):
        if os.path.exists(out_file_csv_name):
            os.remove(out_file_csv_temp)
        else:
            os.rename(out_file_csv_temp, out_file_csv_name)
    if args.j:
        write_json(out_file_csv_name)

    #print "Removing", gc_file
    os.remove(gc_file)


def parse_row(line, csv_column_count, dialect):
    """Parses row and returns a cleaned or failed row."""
    line_buffer = StringIO.StringIO()
    line_buffer.write(line)
    line_buffer.seek(0)

    orig_reader = UnicodeReader(line_buffer, dialect=dialect)

    row_escaped = orig_reader.next()

    if len(row_escaped) == csv_column_count:
        return row_escaped, None
    if args.m and csv_column_count > 1:
        lx = row_escaped[:csv_column_count - 1]
        lt = dialect.delimiter.join(row_escaped[csv_column_count - 1:])
        lx.append(lt)
        return lx, None
    return None, line


def write_headers(f, headers):
    """Write headers to file."""
    header_line = ','.join(headers)
    return f.write(header_line + '\n')


def print_lines(f, num_of_lines):
    last_location = f.tell()
    f.seek(0)
    print 'The first {} lines:'.format(num_of_lines)
    print '-' * 20
    for x in range(num_of_lines):
        print f.readline(),
    print
    print '-' * 20
    print
    f.seek(last_location)


def set_headers(f, dialect, csv_column_count=0):
    headers = []
    if args.sh:
        headers = args.sh.split(',')
    elif args.ah:
        if not csv_column_count:
            csv_column_count = find_column_count(f)
        f.seek(0)
        headers = get_headers(f, dialect.delimiter, csv_column_count)
        print_lines(f, 30)
        while True:
            # Add a new line
            if headers:
                c_success('Headers found for {}\n'.format(f.name))
                c_warning('Headers to be used: {}'.format(' '.join(headers)))
                correct = confirm()
                if correct:
                    break
                else:
                    headers = []
            else:
                c_warning('Setting the headers for file {}\n'.format(f.name))
                headers = ask_headers(csv_column_count)
                break
    return headers


def confirm():
    """Continually prompt until user answers y or n.

    Default is y (just pressing enter).
    """
    while True:
        resp = raw_input('Is this correct? [Y/n] ')
        if not resp or resp.lower() == 'y':
            return True
        elif resp.lower() == 'n':
            return False
        else:
            c_failure('Please answer y or n')


def is_sqldump(file_path):
    sql_pattern = re.compile(
        "insert into \`?.*?\`?",
        re.IGNORECASE
    )

    try:
        with open(file_path, 'rb') as file:
            text = file.read(100000)
    except IOError:
        return

    return sql_pattern.search(text)


def scan_json_merge(source_folder, dest_folder):

    # Check if correct folder
    if not os.path.isdir(source_folder):
        raise ValueError(
            "Provided source folder %s is incorrect" % (
                source_folder,
                dest_folder
            )
        )

    # Check exist dest folder
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
        print("%s not existing, created." % dest_folder)

    # Load and cluster all files
    all_files = glob.glob(
        os.path.join(
            source_folder,
            "*.json"
        )
    )

    # Init release pool
    release_pool = {}
    for file_path in all_files:
        with open(file_path, "rb") as file:
            item = json.loads(file.readline())

            # Load release name
            release_name = item.get("_source").get("r").replace(" ", "_")

            # Add to release pool
            if release_pool.get(release_name):
                release_pool[release_name].append(file_path)
            else:
                release_pool[release_name] = [file_path]

    # Start merging or copy
    for release_name, list_file in release_pool.items():
        if len(list_file) == 1:
            # Load copy path
            copy_path = os.path.join(
                dest_folder,
                os.path.basename(list_file[0])
            )
            # Do copy
            copyfile(list_file[0],copy_path)
            print("Finish copy %s into %s" % (list_file[0], copy_path))
        else:
            # Load release path
            release_path = os.path.join(
                dest_folder,
                "%s.json" % release_name
            )

            # If release path exist than remove
            if os.path.exists(release_path):
                os.remove(release_path)

            # Start merging
            for file_path in list_file:
                with open(file_path, "rb") as file:
                    with open(release_path, "a+") as release_file:
                        file.seek(0)
                        while True:
                            line = file.readline()
                            if not line:
                                break
                            release_file.write(line)

                print("Finish merge %s into %s" % (file_path, release_name))


def main():
    dialect = myDialect()

    if len(args.path) == 0 and not args.jm:
        raise ValueError("Please input path.")

    files = gather_files(args.path, DIRS['skipped'])
    nonsql_files = [x for x in files if not is_sqldump(x)]

    if args.cl:
        print 'Cleaning filenames...'
        for file in files:
            clean_filename(file)
    elif args.jm:
        if len(args.path) > 2:
            raise ValueError(
                "Only accept source and destination folder."
            )
        else:
            try:
                source_folder, dest_folder = args.path
            except ValueError:
                # Init source folder in case not enough path arguments
                try:
                    source_folder = args.path[0]
                except (ValueError, IndexError):
                    source_folder = os.getcwd()

                # Init dest folder
                dest_folder = os.path.join(
                    source_folder,
                    "processed"
                )

        scan_json_merge(source_folder, dest_folder)
    elif args.sh or args.ah:
        for filepath in nonsql_files:
            headers = []
            c_action_info('{}: Checking for headers'.format(filepath))
            with open(filepath, 'rb') as cf:
                headers = set_headers(cf, dialect)
                if headers:
                    c_sys_success(
                        '{}: Headers found, writing new file'.format(filepath))
                    pbar = TqdmUpTo(
                        total=os.path.getsize(filepath), unit=' bytes')
                    with open(filepath + '~', 'wb') as new_csv:
                        write_headers(new_csv, headers)
                        pbar.update_to(new_csv.tell())
                        for line in cf:
                            new_csv.write(line)
                            pbar.update_to(new_csv.tell())
                    pbar.close()
                    c_action_info('{}: New file written'.format(filepath))
            if headers:
                c_action_info('{}: Moving to {}/'.format(
                    filepath, DIRS['headers_success']))
                os.rename(filepath + '~', filepath)
                move(filepath, DIRS['headers_success'])
            else:
                c_warning('{}: Skipping setting headers, moving to {}/'.format(
                    filepath, DIRS['headers_skip']))
                move(filepath, DIRS['headers_skip'])
    elif args.j:
        if not nonsql_files:
            c_failure('No non-sql files found to write json for')
        for cf in nonsql_files:
            write_json(cf)
            move(cf, DIRS['json_done'])
    elif args.o:
        for path in nonsql_files:
            with open(path, 'rb') as f:
                column_count = find_column_count(f)
            if column_count <= 10:
                existing_dir = os.path.dirname(path)
                new_dir = '{}col'.format(column_count)
                new_path = os.path.join(existing_dir, new_dir)
                c_action_system('Moving {} to {}'.format(path, new_path))
                move(path, new_path)
            else:
                c_warning('{} has {} columns, skipping'.format(
                    path, column_count))
    elif args.s:
        c_action_system('Creating samples of CSV(s)\n')
        for path in nonsql_files:
            c_action_info('\nSampling {}'.format(path))
            create_sample(path, args.scl, args.sci, DIRS['sample'])
    elif files:
        if nonsql_files:
            print
            c_lightgray('PARSING TXT and CSV FILES')
            c_darkgray('-------------------------')

            fc = 0
            nf = len(nonsql_files)
        for filename in nonsql_files:
            # Skip files with _cleaned in filename
            if '_cleaned' in filename:
                continue
            # print "\n \033[1;34mProcessing", f
            # print "\033[0m"
            fdirname = os.path.dirname(filename)
            fbasename = os.path.basename(filename)
            clean_name = []

            for char in fbasename:
                if char in "&+@'":
                    clean_name.append('_')
                else:
                    clean_name.append(char)
            new_basename = ''.join(clean_name)

            if new_basename != fbasename:
                new_filename = os.path.join(fdirname, new_basename)
                os.rename(filename, new_filename)
                filename = new_filename

            fc += 1
            c_darkgray('------------------------------------------')

            c_action_info('File {}/{}'.format(fc, nf))
            c_action('Processing {}'.format(filename))

            if os.path.exists(filename):
                if os.stat(filename).st_size > 0:
                    parse_file(filename)
                else:
                    print "File {} is empty, passing".format(filename)
            else:
                c_warning('Unable to find file {}'.format(filename))

        sql_files = [x for x in files if is_sqldump(x)]
        if sql_files:
            print
            c_action('PARSING SQL FILES')
            c_darkgray('------------------------------------------\n')

        for sf in sql_files:
            try:
                parse_sql.parse(sf)
            except KeyboardInterrupt:
                c_warning('Control-c pressed...')
                sys.exit(138)
            except Exception as error:
                move(sf, DIRS['sql_fail'])
                c_failure('ERROR: {}'.format(str(error)))
            else:
                move(sf, DIRS['sql_success'])


if __name__ == "__main__":
    main()
    print "\nFINISHED\n"
