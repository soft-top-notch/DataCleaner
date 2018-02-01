import sys, shutil
import os

if not len(sys.argv) > 1:
    print "Usage: {} path_to_file.csv".format(sys.argv[0]) 
    sys.exit(0)


def ask_user_for_delimeter():

    csv_delimeter = raw_input("Please idetify delimeter to be used for parsing csv file: ")
    csv_column_count = raw_input("Please identify column number: ")

    return csv_delimeter, csv_column_count

def x_guess_delimeter(F):

    print "Guessing delimeter by looking first 100 lines"

    delim_counts = {'\t':[],
                ' ': [],
                ';': [],
                ':': [],
                ',': [],
                '|': [],
                }

    csv_delimeter = None
    csv_column_count = 0

    c=0
    for l in F:
        if c>=100:
            break
        ls=l.strip()
        if ls:
            for d in delim_counts:
                dc = ls.count(d)
                if not dc in delim_counts[d]:
                    delim_counts[d].append(ls.count(d))
            c +=1


    delimeter_list = []

    for d in delim_counts:
        if len(delim_counts[d]) == 1:
            if not delim_counts[d][0] == 0:
                
                delim = (d, delim_counts[d][0])
                if not delim in delimeter_list:
                    delimeter_list.append(delim)

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

    if not delimeter_list:
        print "Can't guess delimeter"

        csv_delimeter, csv_column_count = ask_user_for_delimeter()

    elif  len (delimeter_list)> 1:
        print "Can't guess delimeter, found multiple delimeters"
        for d in delimeter_list:
            if d[0] == '\t':
                print "Guessed delimeter: {tab}"
            else:
                print "Guessed delimeter:", d[0]
            print "Guessed column number", d[0]+1
        
        csv_delimeter, csv_column_count = ask_user_for_delimeter()

    else:
        print "Gusessed delimeter -> {}".format('{tab}' if  delimeter_list[0][0] =='\t' else delimeter_list[0][0])
        print "Guessed column number", delimeter_list[0][1]+1

        r = raw_input("Do you want to proceed with these guessed values? [Y|n]: ")
        if (not r) or (r in ('Y', 'y')):
            csv_delimeter = delimeter_list[0][0]
            csv_column_count = delimeter_list[0][1]+1
        else:
            csv_delimeter, csv_column_count = ask_user_for_delimeter()
            
    return csv_delimeter, csv_column_count


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

    delims = ('\t', ' ', ';', ':', ',', '|')

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
        return csv_delimeter, csv_column_count

    else:
        csv_delimeter, csv_column_count = ask_user_for_delimeter()
        return csv_delimeter, csv_column_count


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

        print "Escaping grabage characters"
        
        gc_file = "{0}_gc~".format(tfile)
        
        gc_cmd = "tr -cd '\11\12\15\40-\176' < {} > {}".format(tfile, gc_file)

        os.system(gc_cmd)

        print "Parsing file", gc_file

        F = open(gc_file,'rb')

        csv_delimeter, csv_column_count = guess_delimeter(F)

        c=0

        F.seek(0)

        out_file_csv_name = f_name+'_parsed.csv'
        out_file_err_name = f_name+'_error.csv'
        
        write_delimeter = ','
        
        out_file_csv_file = open(out_file_csv_name+'~','wb')
        
        out_file_err_file = open(out_file_err_name+'~','wb')
        
        for li in F:
            ls = li.strip()
            if ls:
                ls = strip_delimeter(ls, csv_delimeter)
                l = ls.split(csv_delimeter)
                if l:
                    if not l[-1]:
                        l.pop()

                    lc = clean_fields(l)
                    lc = wrap_fields(lc)

                    if len(l)== csv_column_count:
                        out_file_csv_file.write(write_delimeter.join(lc)+'\n')
                    else:
                        out_file_err_file.write(write_delimeter.join(l)+'\n')
            
        print "Output file", out_file_csv_name, "were written"
        print "Error file", out_file_err_name, "were written"

        print "Removing", gc_file
        os.remove(gc_file)

        print "Moving {} to completed folder".format(tfile)
        os.rename(tfile, os.path.join(completed_dir, fbasename))
        
        err_basename = os.path.basename(out_file_err_name+'~')
        
        print "Moving {} to error folder".format(out_file_err_name+'~')
        
        err_basename = os.path.basename(out_file_err_name+'~')
        
        os.rename(out_file_err_name+'~', os.path.join(error_dir, err_basename[:-1]))

        os.rename(out_file_csv_name+'~', out_file_csv_name)


if __name__ == '__main__':

    ppath = sys.argv[1]

    if os.path.isdir(ppath):
        for tfile in os.listdir(ppath):
            tf = os.path.join(ppath,tfile)
            if not tf.endswith('~'):
                if os.path.isfile(tf):
                    parse_file(tf)

    elif os.path.isfile(ppath):
        print "parsing"
        parse_file(ppath)

