#!/usr/bin/python

import os, sys, shutil
import csv, codecs, cStringIO
import argparse
import StringIO
import re
import json

import dateutil.parser

from validate_email import validate_email

import parse_sql

csv.field_size_limit(sys.maxsize)

parser = argparse.ArgumentParser()
parser.add_argument("-a", help="Don't ask if delimiter is guessed", action="store_true")
parser.add_argument("-p", help="Pass if delimiter can't guessed", action="store_true")
parser.add_argument("-m", help="Merge remaining columns into last", action="store_true")
parser.add_argument("-j", help="Write JSON file", action="store_true")
parser.add_argument("-gh", help="Guess headers", action="store_true")
parser.add_argument("-gho", help="Guess headers only", action="store_true")
parser.add_argument("-jo", help="Write json only", action="store_true")
parser.add_argument("-c", type=int, help="Number of columns")
parser.add_argument("-d", type=str, help="Delimiter")
parser.add_argument("path", help="Path to csv file or folder")

args = parser.parse_args()

if (args.c and (not args.d)) or (not args.c and args.d):
    print "Warning: Argument -c and -d should be used together"
    sys.exit(0)

guess = True
if args.c and args.d:
    guess = False


header_conversions = {
    'username': 'u',
    'name': 'n',
    'email': 'e',
    'mail': 'e',
    'ip': 'I',
    'date': 'd',
    'password': 'p',
    'passwd': 'p',
    'salt': 's',
    'address':'a',
    'telephone': 't',
    'phone': 't',
}



delims = ('\t', ' ', ';', ':', ',', '|')

def valid_ip(address):
    try:
        host_bytes = address.split('.')
        valid = [int(b) for b in host_bytes]
        valid = [b for b in valid if b >= 0 and b<=255]
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
    escapechar='\\'


def find_mode(L):
    mDict = {}
    for i in L:
        if i in mDict: 
            mDict[i] +=1
        else:
            mDict[i] =1

    mList = [ (mDict[i],i) for i in mDict ]

    if mList:
        return max(mList)


def find_column_count(f, dialect, iter_n = 500):
    i=1
    L=[]
    reader = UnicodeReader(f, dialect=dialect)
    for l in reader:
        if l:
            while True:
                if not l:
                    break
                if not l[-1]:
                    l.pop()
                else:
                    break
            if l:
                L.append(len(l))
        if i >= iter_n:
            break
        i +=1

    mode = find_mode(L)
    return mode[1]


def guess_delimeter_by_csv(F):
    F.seek(0)

    sniffer = csv.Sniffer()

    try:
        dialect = sniffer.sniff(F.read(1024*5), delimiters = delims)

        if not dialect.escapechar:
            dialect.escapechar='\\'
    
        F.seek(0)

        column_count = find_column_count(F, dialect)
        
        return dialect, column_count
    except:
        return None



def ask_user_for_delimeter():

    csv_delimeter = raw_input("Please idetify delimeter to be used for parsing csv file: ")
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
        print ">>> Guess method: CSV Sniffer delimiter -> {}\n".format(csv_delimeter)
    else:
        delim_counts_list={}
        delim_freq = {}
        for d in delims:
            delim_counts_list[d]=[]
            delim_freq[d] = {}

        x=0

        F.seek(0)

        for l in F:
            if x>=1000:
                break
            for d in delims:
                ls = l.strip()
                ls = strip_delimeter(ls, d)
                cnt = ls.count(d)
                delim_counts_list[d].append(cnt)
                x+=1

        most_frequent = (None, 0, 0)

        for d in delims:
            for c in delim_counts_list[d]:
                if c:
                    if c in delim_freq[d]:
                        delim_freq[d][c] +=1
                    else:
                        delim_freq[d][c] = 1
                    
        for d in delim_freq:
            for c in delim_freq[d]:
                
                if delim_freq[d][c] > most_frequent[1]:
                    most_frequent = (d,  delim_freq[d][c], c)

        csv_delimeter = most_frequent[0]
        csv_column_count = most_frequent[2] + 1

        rdialect = csv.excel
        rdialect.delimiter = csv_delimeter

        if csv_delimeter:

            print ">>> Guess method: Custom delimiter -> {}\n".format(csv_delimeter)
        
        else:
        
            if not args.p:
        
                print "Delimiter could not determined"
                csv_delimeter, csv_column_count = ask_user_for_delimeter()
                rdialect = csv.excel
                rdialect.delimiter = csv_delimeter

                return rdialect, csv_column_count
            else:
                print "Delimiter could not determined, passing"
                return False, False

    if args.a:
        return rdialect, csv_column_count

    print "Here is the first 10 lines\n"
    print "-"*30
    F.seek(0)
    
    c=0
    
    for l in F:
        print l.strip()
        if c>=10:
            break
        c +=1

    print "-"*30
    print

    print "Gusessed delimeter -> {}".format('{tab}' if  csv_delimeter =='\t' else csv_delimeter)
    print "Guessed column number", csv_column_count

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
            if e[0]=='"' and e[-1]=='"':
                e = e[1:-1]
            else:
                break
        else:
            break
        
    return e.strip()


def clean_fields(l):
    return [clean(x) for x in l]


def wrap_fields(l, wrapper='"'):
    return ['{0}{1}{0}'.format(wrapper, x) for x in l]



def get_or_guess_headers(file_name):

    print "Guessing headers ..."

    csv_reader = UnicodeReader(open(file_name), dialect=myDialect)
    date_column=[]
    ip_column = []
    email_colum = []
    phone_number = []
    name_field = []
    username_field = []
    usernames = []
    passwords = []
    password_field = []
    n=0
    
    for row in csv_reader:
        hc = 0
        
        if n==0:
            ncolumns = len(row)
            lowerrow = [cc.lower() for cc in row]
            hcx = 0
            for j in row:
                if j in header_conversions.values():
                    hcx +=1
            if hcx > 1:
                return False, row, True
            
            for x in ('email', 'name', 'date', 'id', 'mail', 'user', 'username', 'pass', 'passwd', 'password', 'phone', 'phone number', 'ip'):
                if x in lowerrow:
                    hc +=1
            if hc > 1:
                print "First row is headers, abbreviate"
                headers = []
                for i in lowerrow:
                    headers.append( header_conversions.get(i, i))
                return 1, headers, True
                
        for i,c in enumerate(row):
            phonef = False
            try:
                dob = dateutil.parser.parse(c)
                date_column.append(i)
                
            except:
                cr = c.replace(' ', '')
                cr = cr.replace('+','')
                cr = cr.replace('-','')
                
                if cr.isdigit():
                    phone_number.append(i)
                    phonef = True
            
            if valid_ip(c):
                ip_column.append(i)
            
                
            #email, username and should be unique, so we muste use elif
            if validate_email(c):
                email_colum.append(i)
            elif not phonef:
                if not c in usernames:
                    username_field.append(i)
                    usernames.append(c)
                else:
                    passwords.append(c)
            
            #if re_phone1.match(c) or re_phone3.match(c) or re_phone1.match(c):
            #    phone_number.append(i)
            
            if c.count(' ') > 0:
                name_field.append(i)

        n += 1
        if n > 100:
            break


    headers = ['U{}'.format(i) for i in range(ncolumns)]

    #at least 50% of the rows should contain similar value
    
    mode_email_colum = find_mode(email_colum)
    if mode_email_colum:
        if mode_email_colum[0] > 50:
            headers[mode_email_colum[1]] = 'e'
    
    mode_date_column = find_mode(date_column)
    if mode_date_column:
        if mode_date_column[0]> 50:
            headers[mode_date_column[1]] = 'd'
            
    mode_ip_column = find_mode(ip_column)
    if mode_ip_column:
        if mode_ip_column[0] > 50:
            headers[mode_ip_column[1]] = 'I'
            
    mode_phone_number = find_mode(phone_number)
    if mode_phone_number:
        if mode_phone_number[0] > 50:
            headers[mode_phone_number[1]] = 't'
    mode_name_field = find_mode(name_field)

    #for name field 1/3 should be enough
    if mode_name_field:
        if mode_name_field[0] > 33:
            headers[mode_name_field[1]] = 'n'


    mode_username_field = find_mode(username_field)
    if mode_username_field:
        if mode_username_field[0] > 50:
            headers[mode_username_field[1]] = 'u'

    return 1, headers, False


def write_json(source):
    print "Writing json file for ", source
    fdirname = os.path.dirname(source)
    fbasename = os.path.basename(source)
    json_file = os.path.splitext(fbasename)[0]+'.json'
    json_dir = os.path.join(fdirname, 'json')

    if not os.path.exists(json_dir):
        os.mkdir(json_dir)

    json_list = []
    out_reader = UnicodeReader(open(source))
    fl = True
    for l in out_reader:
        if fl:
            headers = l
            fl = False

        else:
            jdict = dict()
            for i in range(len(headers)):
                jdict[ headers[i] ] = l[i]
            json_list.append(jdict)
    
    with open(os.path.join(json_dir, json_file), 'w') as outfile:
        json.dump(json_list, outfile)
    
def write_headers(source, cleaned=None):
    
    if cleaned:
        os.rename(source, source+'~')

    gh = get_or_guess_headers(source+'~')

    if gh[0]:
        ghw = wrap_fields(gh[1])
        header_line = ",".join(ghw)
        print "Header Line:", header_line
        write_first_line = True
        if gh[2]:
            write_first_line = False
        c = 0
        with open(source, 'w') as W:
            W.write(header_line+'\n')
            for l in open(source+'~'):
                if write_first_line:
                    W.write(l)
                else:
                    if c > 0:
                        W.write(l)
                c += 1

    else:
        shutil.copy(source+'~', source)

    if cleaned:
        print "Removing", source+'~'
        os.remove(source+'~')

def parse_file(tfile):

    org_tfile = tfile
    
    tfile = org_tfile.replace('(','')
    tfile = tfile.replace(')','')
    tfile = tfile.replace(' ','')
    tfile = tfile.replace(',','')

    os.rename(org_tfile, tfile)

    f_name, f_ext = os.path.splitext(tfile)

    fdirname = os.path.dirname(tfile)
    fbasename = os.path.basename(tfile)
    
    json_file = os.path.splitext(fbasename)[0]+'.json'
    
    completed_dir = os.path.join(fdirname, 'completed')
    error_dir = os.path.join(fdirname, 'error')
    json_dir = os.path.join(fdirname, 'json')

    if not os.path.exists(json_dir):
        os.mkdir(json_dir)

    
    if not os.path.exists(completed_dir):
        os.mkdir(completed_dir)

    if not os.path.exists(error_dir):
        os.mkdir(error_dir)

    

    print "\n---------------------------------------------\n"
    
    print "Escaping grabage characters"
    
    gc_file = "{0}_gc~".format(tfile)
    
    gc_cmd = "tr -cd '\11\12\15\40-\176' < {} > {}".format(tfile, gc_file)

    os.system(gc_cmd)

    print "Parsing file", gc_file

    F = open(gc_file,'rb')

    if guess:
        print "Guessing delimiter"
        dialect, csv_column_count = guess_delimeter(F)
        if dialect:
            print "Guessed column number [{}] and delimiter [{}]".format(
                dialect.delimiter, csv_column_count)
    else:
        print "Using provided column number [{}] and delimiter [{}]".format(
                                    args.c, args.d)
        dialect = csv.excel
        dialect.delimiter = args.d
        csv_column_count = args.c

    if dialect:
        
        F.seek(0)

        out_file_csv_name = f_name+'_cleaned.csv'
        out_file_err_name = f_name+'_error.csv'
        
        
        out_file_csv_file = open(out_file_csv_name+'~','wb')
        
        out_file_err_file = open(out_file_err_name+'~','wb')

        print "Cleaning ... \n"

        clean_writer = UnicodeWriter(out_file_csv_file, dialect=myDialect)
        error_writer = UnicodeWriter(out_file_err_file, dialect=dialect)

        json_list = []

        l_count = 0

        for lk in F:
            a = StringIO.StringIO()
            a.write(lk)
            a.seek(0)
            orig_reader = UnicodeReader(a, dialect=dialect)

            for l in orig_reader:
                l_count +=1
                if l_count % 100:
                    print"\r Parsing line: {0}".format(l_count),
                    sys.stdout.flush()
                
                while True:
                    if not l:
                        break
                    if not l[-1]:
                        l.pop()
                    else:
                        break
                ll = l[:]
                l=[]
                for li in ll:
                    if li:
                        if not li[-1] == "":
                            l.append(li.strip())
                        else:
                            l.append(li)

                if len(l) == csv_column_count:
                    clean_writer.writerow(l)
                    
                elif len(l) == csv_column_count-1:
                    l.append("")
                    clean_writer.writerow(l)
                else:
                    if args.m and csv_column_count > 1:
                        lx=l[:csv_column_count-1]
                        lt = dialect.delimiter.join(l[csv_column_count-1:])
                        lx.append(lt)
                        clean_writer.writerow(lx)
                    else:
                        error_writer.writerow(l)

        F.close()
        out_file_csv_file.close()
        out_file_err_file.close()

        print
        print "Output file", out_file_csv_name+'~', "were written"
        print "Error file", out_file_err_name+'~', "were written"


        print "Moving {} to completed folder".format(tfile)
        os.rename(tfile, os.path.join(completed_dir, fbasename))
        
        err_basename = os.path.basename(out_file_err_name+'~')
        
        print "Moving {} to error folder".format(out_file_err_name+'~')
        
        err_basename = os.path.basename(out_file_err_name+'~')
        
        target_out_err_file = os.path.join(error_dir, err_basename[:-1])
        
        os.rename(out_file_err_name+'~', target_out_err_file)
        
        
        if args.gh:
            write_headers(out_file_csv_name)
            os.remove(out_file_csv_name+'~')
        else:
            os.rename(out_file_csv_name+'~', out_file_csv_name)
        if args.j:
            write_json(out_file_csv_name)
        
        
        
    else:
        os.rename(tfile, os.path.join(error_dir, fbasename))

    print "Removing", gc_file
    os.remove(gc_file)
    
if __name__ == '__main__':

    mpath = args.path

    parse_path_list = []

    sql_path_list = []

    cleaned_file_list = []

    if os.path.isdir(mpath):
        for ppath in os.listdir(mpath):
            if not ppath in ('completed','error'):
                ppath = os.path.join(mpath, ppath)
                if os.path.isdir(ppath):
                    for tfile in os.listdir(ppath):
                        tf = os.path.join(ppath,tfile)
                        if not tf.endswith('~') and not tf.startswith('.'):
                            
                            if args.gh:
                                if os.path.isfile(tf):
                                    if tf.lower().endswith('.sql'):
                                        sql_path_list.append(tf)
                                    else:
                                        parse_path_list.append(tf)
                            else:
                                if not '_cleaned.' in tf:
                                
                                    if os.path.isfile(tf):
                                        if tf.lower().endswith('.sql'):
                                            sql_path_list.append(tf)
                                        else:
                                            parse_path_list.append(tf)
                                else:
                                    cleaned_file_list.append(tf)

                elif os.path.isfile(ppath):
                    if not ppath.endswith('~') and not ppath.startswith('.'):
                        
                        if args.gh:
                            if ppath.lower().endswith('.sql'):
                                sql_path_list.append(ppath)
                            else:
                                parse_path_list.append(ppath)
                        else:
                            if not '_cleaned.' in ppath:
                                if ppath.lower().endswith('.sql'):
                                    sql_path_list.append(ppath)
                                else:
                                    parse_path_list.append(ppath)
                            else:
                                    cleaned_file_list.append(ppath)

    elif os.path.isfile(mpath):
        if not mpath.endswith('~'):
            if args:
                if mpath.lower().endswith('.sql') and not mpath.startswith('.'):
                    sql_path_list.append(mpath)

                else:
                    parse_path_list.append(mpath)
            else:
                if not '_cleaned.' in mpath:
                    if mpath.lower().endswith('.sql') and not mpath.startswith('.'):
                        sql_path_list.append(mpath)
                    else:
                        parse_path_list.append(mpath)
                else:
                    cleaned_file_list.append(mpath)
    
    if args.gho:
        print "Guessing headers"
        for f in cleaned_file_list:
            if not f.endswith('.json'):
                print "Processing", f
                write_headers(f, cleaned=True)
    
    if args.jo:
        for f in cleaned_file_list:
            if not f.endswith('.json'):
                write_json(f)
    
    if not (args.gho or args.jo):
    
        print
        print "PARSING TXT and CSV FILES"
        print "-------------------------\n"
        
        fc = 0
        nf = len(parse_path_list)
        for f in parse_path_list:
            fdirname = os.path.dirname(f)
            fbasename = os.path.basename(f)
        
            if ('&' in fbasename) or ('+' in fbasename) or ('@' in fbasename) or ("'" in fbasename):
                
                nfbasename = fbasename
                for ch in "&+@'":
                    nfbasename = nfbasename.replace(ch,'_')
                    nf = os.path.join(fdirname, nfbasename)
                    os.rename(f, nf)
                    f = nf


            fc += 1
            print 
            print "File {}/{}".format(fc, nf)
            if os.stat(f).st_size > 0:
                parse_file(f)
            else:
                print "File {} is empty, passing".format(f)
        print
        print "PARSING SQL FILES"
        print "-------------------------\n"
        
        for sf in sql_path_list:
            dir_name = os.path.dirname(sf)
            sARGS={ 
                    'SQLFILE': [sf],
                    '--failed': os.path.join(dir_name,'failed'),
                    '--completed': os.path.join(dir_name,'completed'),
                    '--exit-on-error': False,
                    }
            parse_sql.main(sARGS)


    print "\nFINISHED\n"

