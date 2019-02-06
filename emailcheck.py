'''
simple script to check batches of files and determine if 
there is an email address in the first column. output will be true/false. 
'''
import os
import re
import sys
from glob import glob


def check_email(input_path, output_file):
    email_pattern = re.compile(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,6}")

    if not os.path.exists(input_path):
        print('Input path does not exist')
        return

    files = [f for f in list(glob(input_path+'/*')) if os.path.isfile(f)]
    with open(output_file, 'w') as out_file:
        for f in files:
            with open(f, 'r') as in_file:
                first_line = in_file.readline()
                if email_pattern.findall(first_line.split(',')[0]):
                    out_file.write('{}: True\n'.format(f))
                else:
                    out_file.write('{}: False\n'.format(f))

if __name__ == '__main__':
    args = sys.argv
    if not len(args) == 3:
        print('Invalid parameters')
        sys.exit()
    input_path, output_file = sys.argv[1:]
    check_email(input_path, output_file)

