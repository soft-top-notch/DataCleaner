import os, sys, shutil
import csv, codecs, cStringIO
import argparse

csv.field_size_limit(sys.maxsize)

parser = argparse.ArgumentParser()
parser.add_argument("-a", help="Don't ask if delimiter is guessed", action="store_true")
parser.add_argument("-p", help="Pass if delimiter can't guessed", action="store_true")
parser.add_argument("path", help="Path to csv file or folder")

args = parser.parse_args()



delims = ('\t', ' ', ';', ':', ',', '|')

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
    
    completed_dir = os.path.join(fdirname, 'completed')
    error_dir = os.path.join(fdirname, 'error')

    if not os.path.exists(completed_dir):
        os.mkdir(completed_dir)

    if not os.path.exists(error_dir):
        os.mkdir(error_dir)

    if f_ext in ('.csv' '.txt'):

        print "\n---------------------------------------------\n"
        
        print "Escaping grabage characters"
        
        gc_file = "{0}_gc~".format(tfile)
        
        gc_cmd = "tr -cd '\11\12\15\40-\176' < {} > {}".format(tfile, gc_file)

        os.system(gc_cmd)

        print "Parsing file", gc_file

        F = open(gc_file,'rb')

        print "Guessing delimiter"
        dialect, csv_column_count = guess_delimeter(F)

        if dialect:

            F.seek(0)

            out_file_csv_name = f_name+'_cleaned.csv'
            out_file_err_name = f_name+'_error.csv'
            
            
            out_file_csv_file = open(out_file_csv_name+'~','wb')
            
            out_file_err_file = open(out_file_err_name+'~','wb')


            print "Cleaning ... \n"

            orig_reader = UnicodeReader(F, dialect=dialect)
            clean_writer = UnicodeWriter(out_file_csv_file, dialect=myDialect)
            error_writer = UnicodeWriter(out_file_err_file, dialect=dialect)

            for l in orig_reader:
                
                while True:
                    if not l:
                        break
                    if not l[-1]:
                        l.pop()
                    else:
                        break
                
                if len(l) == csv_column_count:
                    clean_writer.writerow(l)
                elif len(l) == csv_column_count-1:
                    l.append("")
                    clean_writer.writerow(l)
                else:
                    error_writer.writerow(l)

            F.close()
            out_file_csv_file.close()
            out_file_err_file.close()


            print "Output file", out_file_csv_name+'~', "were written"
            print "Error file", out_file_err_name+'~', "were written"



            print "Moving {} to completed folder".format(tfile)
            os.rename(tfile, os.path.join(completed_dir, fbasename))
            
            err_basename = os.path.basename(out_file_err_name+'~')
            
            print "Moving {} to error folder".format(out_file_err_name+'~')
            
            err_basename = os.path.basename(out_file_err_name+'~')
            
            os.rename(out_file_err_name+'~', os.path.join(error_dir, err_basename[:-1]))

            os.rename(out_file_csv_name+'~', out_file_csv_name)

        print "Removing", gc_file
        os.remove(gc_file)
        
if __name__ == '__main__':

    mpath = args.path

    print mpath

    if os.path.isdir(mpath):
        for ppath in os.listdir(mpath):
            ppath = os.path.join(mpath, ppath)
            if os.path.isdir(ppath):
                print "PATH"
                for tfile in os.listdir(ppath):
                    tf = os.path.join(ppath,tfile)
                    if not tf.endswith('~'):
                        if os.path.isfile(tf):
                            parse_file(tf)

            elif os.path.isfile(ppath):
                parse_file(ppath)
    elif os.path.isfile(mpath):
        if not mpath.endswith('~'):
            parse_file(mpath)
        

