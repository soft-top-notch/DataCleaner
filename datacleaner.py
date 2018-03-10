#!/usr/bin/env python
import argparse
import codecs
import cStringIO
import csv
import json
import os
import re
import StringIO
import sys
from crayons import red, blue, green

import parse_sql
from datacleaner import move

# Full path to directories used
CLEAN_FAIL_DIR = '../0_errors/clean_fail'
CLEAN_SUCCESS_DIR = '../2_needs_headers'
HEADERS_SKIP_DIR = '../headers_skip'
HEADERS_SUCCESS_DIR = '../headers_success'
JSON_SUCCESS_DIR = '../4_complete'
SQL_FAIL_DIR = '../0_errors/sql_fail'
SQL_SUCCESS_DIR = '../2_needs_headers'

# Directories to skip when gathering lists of files
SKIPPED_DIRS = ('completed', 'error', 'failed')
# Parts of filename to be removed when cleaned
UNWANTED = ('_cleaned', '_dump')

# Headers matched
HEADERS = [
    ('misc', 'x'),
    ('address', 'a'),
    ('dob', 'd'),
    ('email', 'e'),
    ('first_name', 'fn'),
    ('first', 'fn'),
    ('firstname', 'fn'),
    ('hash', 'h'),
    ('ip', 'i'),
    ('ipaddress', 'i'),
    ('last_name', 'ln'),
    ('last', 'ln'),
    ('lastname', 'ln'),
    ('name', 'n'),
    ('password', 'p'),
    ('phone', 't'),
    ('salt', 's'),
    ('username', 'u')
    ('city', 'a1')
    ('state', 'a2')
    ('zip', 'a3')
    ('zip_code', 'a3')
    ('zipcode', 'a3')
    ('postalcode', 'a3')
    ('postal_code', 'a3')
    ('country', 'a4')

]  # yapf: disable

# Abbreviated headers that are enumerated
ENUMERATED = ('x', 'a')
# Abbreviations to headers
ABBR2HEADER = {abbr: header for header, abbr in HEADERS}
# Headers to abbreviations
HEADER2ABBR = {header: abbr for header, abbr in HEADERS}

csv.field_size_limit(sys.maxsize)

parser = argparse.ArgumentParser()
exclusive_args = parser.add_mutually_exclusive_group()
parser.add_argument(
    "-a", help="Don't ask if delimiter is guessed", action="store_true")
exclusive_args.add_argument(
    "-ah",
    help="Ask for field names (headers) to add to CSVs. No file cleaning.",
    action="store_true")
parser.add_argument(
    "-p", help="Pass if delimiter can't guessed", action="store_true")
parser.add_argument(
    "-m", help="Merge remaining columns into last", action="store_true")
exclusive_args.add_argument(
    "-j", help="Write JSON file. No file cleaning.", action="store_true")
parser.add_argument("-c", type=int, help="Number of columns")
exclusive_args.add_argument(
    "-cl",
    help="Cleanse filename(s) of unwanted text. No file cleaning.",
    action="store_true")
parser.add_argument("-d", type=str, help="Delimiter")
parser.add_argument("-r", type=str, help="Release Name")
exclusive_args.add_argument(
    "-sh",
    type=str,
    help="Specify headers to use for multiple files. No file cleaning.")
parser.add_argument(
    "path",
    type=str,
    nargs='+',
    help="Path to one or more csv file(s) or folder(s)")

args = parser.parse_args()

if (args.c and (not args.d)) or (not args.c and args.d):
    print "Warning: Argument -c and -d should be used together"
    sys.exit(0)

guess = True
if args.c and args.d:
    guess = False

delims = ('\t', ' ', ';', ':', ',', '|')


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
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_ALL
    escapechar = '\\'


def find_mode(L):
    mDict = {}
    for i in L:
        if i in mDict:
            mDict[i] += 1
        else:
            mDict[i] = 1

    mList = [(mDict[i], i) for i in mDict]

    if mList:
        return max(mList)


def find_column_count(f, dialect=csv.excel):
    column_count = 0
    reader = UnicodeReader(f, dialect=dialect)
    for x in range(10):
        try:
            row = reader.next()
        except StopIteration:
            break
        length = len(row)
        if length > column_count:
            column_count = length
    return column_count


def guess_delimeter_by_csv(F):
    F.seek(0)

    sniffer = csv.Sniffer()

    try:
        dialect = sniffer.sniff(F.read(1024 * 5), delimiters=delims)

        if not dialect.escapechar:
            dialect.escapechar = '\\'

        F.seek(0)

        column_count = find_column_count(F, dialect)

        return dialect, column_count
    except:
        return None


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

    csv_guess = guess_delimeter_by_csv(F)

    if csv_guess:
        rdialect, csv_column_count = csv_guess
        csv_delimeter = rdialect.delimiter
        print "\033[38;5;117mGuessed CSV delimeter -> {}".format(csv_delimeter)
    else:
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

                if delim_freq[d][c] > most_frequent[1]:
                    most_frequent = (d, delim_freq[d][c], c)

        csv_delimeter = most_frequent[0]
        csv_column_count = most_frequent[2] + 1

        rdialect = csv.excel
        rdialect.delimiter = csv_delimeter

        if csv_delimeter:

            print "\033[38;5;244mGuess method: Custom delimiter -> {}\n".format(
                csv_delimeter)

        else:

            if not args.p:

                print "\033[38;5;203m Delimiter could not determined"
                csv_delimeter, csv_column_count = ask_user_for_delimeter()
                rdialect = csv.excel
                rdialect.delimiter = csv_delimeter

                return rdialect, csv_column_count
            else:
                print "\033[38;5;203m Delimiter could not determined, passing"
                return False, False

    if args.a:
        return rdialect, csv_column_count

    # print first 10 lines
    print_lines(F, 10)

    print "\033[38;5;147m Gusessed delimeter -> {}".format(
        '{tab}' if csv_delimeter == '\t' else csv_delimeter)
    print "\033[38;5;147m Guessed column number", csv_column_count

    r = raw_input("Do you want to proceed with these guessed values? [Y|n]: ")
    if (not r) or (r in ('Y', 'y')):
        return rdialect, csv_column_count
    else:
        csv_delimeter, csv_column_count = ask_user_for_delimeter()

        rdialect = csv.excel
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


def wrap_fields(l, wrapper='"'):
    return ['{0}{1}{0}'.format(wrapper, x) for x in l]


def write_json(source):
    print "Writing json file for", source
    fbasename = os.path.basename(source)
    json_file = os.path.splitext(fbasename)[0] + '.json'

    if not os.path.exists(JSON_SUCCESS_DIR):
        os.mkdir(JSON_SUCCESS_DIR)

    out_reader = UnicodeReader(open(source), dialect=myDialect)

    # grab the first line as headers
    headers = out_reader.next()

    with open(os.path.join(JSON_SUCCESS_DIR, json_file), 'w') as outfile:
        # Add first line of json
        line_count = 0
        for row in out_reader:
            # If this is not the first row, add a new line
            if line_count > 0:
                outfile.write('\n')

            source = dict(zip(headers, row))

            # Clean up source of unwanted values before writing json
            for header, value in source.items():
                # Remove misc headers (x[0-9])
                if re.search('^x\d+$', header):
                    del source[header]
                # Remove entries that are empty
                elif not value or value in ('NULL', 'null', 'xxx'):
                    del source[header]
                else:
                    # Consolidate 'a' entries
                    if re.search('^a\d', header):
                        existing_data = source.get('a', [])
                        existing_data.append(value.rstrip())
                        source['a'] = existing_data
                        del source[header]
                    else:
                        source[header] = value.rstrip()

            # Set release name
            if args.r:
                source['r'] = args.r
            # Use filename without extension as release name
            else:
                source['r'] = os.path.splitext(fbasename)[0]

            data = {'_type': 'breach', '_source': source}

            outfile.write(json.dumps(data))
            line_count += 1
            if line_count % 100:
                print "\r \033[38;5;245mWriting json row: {0}".format(
                    line_count),
                sys.stdout.flush()
        # Write final newline (no comma) and close json brackets
        outfile.write('\n')


def get_headers(csv_file, delimiter, column_count):
    """Reads file and tries to determine if headers are present.

    Returns a list of headers.
    """
    headers = []
    starting_location = csv_file.tell()

    for line in csv_file:
        # Skip comment rows
        if line.startswith('#'):
            continue

        lowerrow = [
            cc.lower().replace('\n', '') for cc in line.split(delimiter)
        ]
        tracked = {'x': 0, 'a': 0}
        for i in lowerrow:
            # Match headers in double quotes on both sides or no double quotes
            matches = re.search('(?="\w+)"(\w+)"|^(\w+)$', i)
            if matches:
                # Take whichever one matched
                field_name = matches.group(1) or matches.group(2)
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
                    header_format = '{}{}'.format(header, tracked[header])
                    tracked[header] += 1
                    header = header_format
                headers.append(header)
            else:
                csv_file.seek(starting_location)
                break
        # Only check the first non-comment row
        break
    if len(headers) == column_count:
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
        for full_header, shortened in HEADERS:
            if shortened not in seen:
                print '{}:{}'.format(shortened, full_header)
                seen.append(shortened)

        user_headers = raw_input(
            "Please enter {} headers as their abbreviation (i.e. u p x): ".
            format(column_count))

        if user_headers:
            user_headers = user_headers.split(' ')
            header_len = len(user_headers)
            if header_len == column_count:
                break
            else:
                if 'zz' in user_headers:
                    return headers
                print '\nERROR: {} headers entered for {} columns\n'.format(
                    header_len, column_count)
        else:
            print '\nERROR: No headers entered\n'

    tracked = {'x': 0, 'a': 0}

    for hi in range(column_count):
        if hi < header_len:
            header = user_headers[hi]
            if header in ENUMERATED:
                header_format = '{}{}'.format(header, tracked[header])
                tracked[header] += 1
                header = header_format
            headers.append(header)

    return headers


def parse_file(tfile):

    org_tfile = tfile

    tfile = org_tfile.replace('(', '')
    tfile = tfile.replace(')', '')
    tfile = tfile.replace(' ', '')
    tfile = tfile.replace(',', '')

    os.rename(org_tfile, tfile)

    f_name, f_ext = os.path.splitext(tfile)

    fbasename = os.path.basename(tfile)

    if not os.path.exists(CLEAN_SUCCESS_DIR):
        os.mkdir(CLEAN_SUCCESS_DIR)

    print "\n\033[38;5;244mEscaping grabage characters"

    gc_file = "{0}_gc~".format(tfile)

    gc_cmd = "tr -cd '\11\12\15\40-\176' < {} > {}".format(tfile, gc_file)

    os.system(gc_cmd)

    print "\033[0mParsing file: ", gc_file

    F = open(gc_file, 'rb')
    dialect = None

    if guess:
        print "\n\033[38;5;244mGuessing delimiter"
        dialect, csv_column_count = guess_delimeter(F)

    if not dialect and args.p:
        return
    elif not dialect:
        dialect = csv.excel
        dialect.delimiter = args.d.decode('string_escape')
        csv_column_count = args.c

    print "\033[38;5;156mUsing column number [{}] and delimiter [{}]".format(
        csv_column_count, dialect.delimiter)

    F.seek(0)

    out_file_csv_name = f_name + '_cleaned.csv'
    out_file_csv_temp = out_file_csv_name + '~'
    out_file_err_name = f_name + '_error.csv'
    out_file_err_temp = out_file_err_name + '~'

    out_file_csv_file = open(out_file_csv_temp, 'wb')

    out_file_err_file = open(out_file_err_temp, 'wb')

    print "\033[38;5;244mCleaning ... \n"

    clean_writer = UnicodeWriter(out_file_csv_file, dialect=myDialect)
    error_writer = UnicodeWriter(out_file_err_file, dialect=dialect)

    l_count = 0
    headers = set_headers(F, dialect, csv_column_count)
    if headers:
        write_headers(out_file_csv_file, headers)
        l_count += 1

    for lk in F:
        a = StringIO.StringIO()
        a.write(lk)
        a.seek(0)
        orig_reader = UnicodeReader(a, dialect=dialect)

        for row in orig_reader:
            l_count += 1
            if l_count % 100:
                print "\r \033[38;5;245mParsing line: {0}".format(l_count),
                sys.stdout.flush()

            row = [x.replace('\n', '').replace('\r', '') for x in row]

            if len(row) == csv_column_count:
                clean_writer.writerow(row)

            elif len(row) == csv_column_count - 1:
                row.append("")
                clean_writer.writerow(row)
            else:
                if args.m and csv_column_count > 1:
                    lx = row[:csv_column_count - 1]
                    lt = dialect.delimiter.join(row[csv_column_count - 1:])
                    lx.append(lt)
                    clean_writer.writerow(lx)
                else:
                    error_writer.writerow(row)

    F.close()
    out_file_csv_file.close()
    out_file_err_file.close()

    output_stats = os.stat(out_file_csv_temp)
    errors_stats = os.stat(out_file_err_temp)

    print
    print "\033[38;5;241m Output file {} had {} bytes written".format(
        out_file_csv_temp, output_stats.st_size)
    print "\033[38;5;241m Error file {} had {} bytes written".format(
        out_file_err_temp, errors_stats.st_size)
    print "\033[38;5;241m Moving {} to completed folder".format(tfile)
    if headers:
        move(tfile, HEADERS_SUCCESS_DIR)
    else:
        move(tfile, CLEAN_SUCCESS_DIR)

    if errors_stats.st_size > 0:
        print "\033[38;5;241m Moving {} to error folder".format(
            out_file_err_temp)
        move(out_file_err_temp, CLEAN_FAIL_DIR)
    else:
        print "Removing", out_file_err_temp
        os.remove(out_file_err_temp)

    if os.path.exists(out_file_csv_temp):
        if os.path.exists(out_file_csv_name):
            os.remove(out_file_csv_temp)
        else:
            os.rename(out_file_csv_temp, out_file_csv_name)
    if args.j:
        write_json(out_file_csv_name)

    print "Removing", gc_file
    os.remove(gc_file)


def gather_files(path, file_list=[]):
    """Gather list of files recursively."""
    if isinstance(path, list):
        for p in path:
            gather_files(p, file_list)
    else:
        if os.path.isdir(path):
            if os.path.basename(path) not in SKIPPED_DIRS:
                for subpath in os.listdir(path):
                    gather_files(os.path.join(path, subpath), file_list)
        else:
            basename = os.path.basename(path)
            if not basename.startswith('.') and not basename.endswith('~'):
                file_list.append(path)
    return file_list


def write_headers(f, headers):
    """Write headers to file."""
    header_line = ','.join(headers)
    print "Header Line:", header_line
    f.write(header_line + '\n')


def print_lines(f, num_of_lines):
    last_location = f.tell()
    f.seek(0)
    print 'The first {} lines:'.format(num_of_lines)
    print '-' * 20
    for x in range(num_of_lines):
        print f.readline(),
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
        while True:
            # Add a new line
            print
            if headers:
                print green('Headers found for {}\n'.format(f.name))
            else:
                print blue('Setting the headers for file {}\n'.format(f.name))
                print_lines(f, 10)
                headers = ask_headers(csv_column_count)
            if headers:
                print blue('Headers to be used: {}'.format(' '.join(headers)))
                correct = confirm()
                if correct:
                    break
                else:
                    headers = []
            else:
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
            print red('Please answer y or n')


def check_unwanted(filename):
    for unwanted in UNWANTED:
        if unwanted in filename:
            print "Skipping {} because {} in filename".format(
                filename, unwanted)
            return True
    return False


def main():
    dialect = myDialect()
    files = gather_files(args.path)
    nonsql_files = [x for x in files if not x.endswith('.sql')]
    if args.cl:
        print 'Cleaning filenames...'
        for file in files:
            clean_filename(file)
    elif args.sh or args.ah:
        for filepath in nonsql_files:
            headers = []
            with open(filepath, 'rb') as cf:
                headers = set_headers(cf, dialect)
                if headers:
                    with open(filepath + '~', 'wb') as new_csv:
                        write_headers(new_csv, headers)
                        for line in cf:
                            new_csv.write(line)
            if headers:
                os.rename(filepath + '~', filepath)
                move(filepath, HEADERS_SUCCESS_DIR)
            else:
                print red('Skipping setting headers for {}'.format(filepath))
                move(filepath, HEADERS_SKIP_DIR)
    elif args.j:
        if not nonsql_files:
            print red('No non-sql files found to write json')
        for cf in nonsql_files:
            write_json(cf)

    elif files:
        if nonsql_files:
            print
            print "\033[38;5;248m PARSING TXT and CSV FILES"
            print "\033[38;5;240m  -------------------------"

            fc = 0
            nf = len(nonsql_files)
        for filename in nonsql_files:
            # Skip files with unwanted filenames (cleaned, errored, etc)
            if check_unwanted(filename):
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
            print
            print "\033[38;5;240m ------------------------------------------\n"
            print "\033[1;34mProcessing", filename
            print "\033[0mFile {}/{}".format(fc, nf)
            if os.stat(filename).st_size > 0:
                parse_file(filename)
            else:
                print "File {} is empty, passing".format(filename)

        sql_files = [x for x in files if x.endswith('.sql')]
        if sql_files:
            print
            print "\033[1;31m PARSING SQL FILES"
            print "\033[38;5;240m -------------------------\n\033[38;5;255m"

        for sf in sql_files:
            try:
                parse_sql.parse(sf)
            except KeyboardInterrupt:
                print('Control-c pressed...')
                sys.exit(138)
            except Exception as error:
                move(sf, SQL_FAIL_DIR)
                print 'ERROR:', str(error)
            else:
                move(sf, SQL_SUCCESS_DIR)


if __name__ == "__main__":
    main()
    print "\nFINISHED\n"
