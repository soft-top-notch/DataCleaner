import sys
import os

if not len(sys.argv) > 1:
    print "Usage: {} path_to_file.csv".format(sys.argv[0]) 
    sys.exit(0)

tfile = sys.argv[1]

f_name, f_ext = os.path.splitext(tfile)


def ask_user_for_delimeter():

    csv_delimeter = raw_input("Please idetify delimeter to be used for parsing csv file: ")
    csv_column_count = raw_input("Please identify column number: ")

    return csv_delimeter, csv_column_count

def guess_delimeter(F):

    print "Guessing delimeter by looking first 100 lines"

    delim_counts = {'\t':[],
                ' ': [],
                ';': [],
                ':': [],
                ',': [],
                }

    csv_delimeter = None
    csv_column_count = 0

    c=0
    for l in F:
        if c>=100:
            break
        ls=l.strip()
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


F = open(tfile,'rb')

def clean(e):
    while True:
        if e:
            if e[0]=='"' and e[-1]=='"':
                e = e[1:-1]
            else:
                break
        else:
            break
        
    return e


def clean_fields(l):
    return [clean(x) for x in l]


if f_ext in ('.csv' '.txt'):

    print "Escaping NULL bytes"
    
    os.system("sed -i -e 's|\\x0||g' "+tfile)
    

    print "Parsing file", tfile

    csv_delimeter, csv_column_count = guess_delimeter(F)

    c=0

    F.seek(0)

    #df = csv.reader(open(tfile), delimiter=csv_delimeter, dialect=csv.excel_tab)

    #df = open(tfile)
    out_file_csv_name = f_name+'_parsed.csv'
    out_file_err_name = f_name+'_error.csv'
    
    
    out_file_csv_file = open(out_file_csv_name,'wb')
    #csvwriter = csv.writer(out_file_csv_file, delimiter=csv_delimeter, quoting=csv.QUOTE_NONE)
    
                            #, doublequote=True,
                            #quotechar='"', quoting=csv.QUOTE_ALL)
    
    out_file_err_file = open(out_file_err_name,'wb')
    
    for li in F:
        l = li.strip().split(csv_delimeter)
        if l:
            if not l[-1]:
                l.pop()
            if len(l)== csv_column_count:
                lc = clean_fields(l)
                
                out_file_csv_file.write(csv_delimeter.join(lc)+'\n')
            else:
                
                out_file_err_file.write(csv_delimeter.join(l)+'\n')
        
    print "Output file", out_file_csv_name, "were written"
    print "Error file", out_file_err_name, "were written"

