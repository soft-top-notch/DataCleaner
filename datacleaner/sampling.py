#!/usr/bin/env python
"""Create sampling of CSV(s).

Called with path argument for one or more files. Filename argument can be a \
list of files or wildcard (*).

Usage:
    sampling.py [-h] [--ci <interval>] [--cl <percent>] PATH...

Options:
    --ci <interval>                 Confidence interval [default: 2.0]
    --cl <percent>                  Confidence level (percent) [default: 95]
    -h, --help                      This help output

Examples:
    sampling.py test.csv
    sampling.py ~/samples/*.csv
"""
from __future__ import print_function, division

import math
import os
import random

from docopt import docopt

from datacleaner import gather_files, move, print_progress


# SUPPORTED CONFIDENCE LEVELS: 50%, 68%, 90%, 95%, and 99%
CONFIDENCE_LEVELS = {50: .67, 68: .99, 90: 1.64, 95: 1.96, 99: 2.57}


def create_sample(path, con_level, con_interval, dest_dir=None):
    """Create sample of file.

    Parameters:
        path: file path (str)
        con_level: confidence level (percent int)
        con_interval: confidence interval (float)
        dest_dir: destination directory to write sample (default is same as
                    source file)

    Returns path for sample (str).
    """
    progress = print_progress(path)
    last = 0
    name = path.rstrip('.csv')
    if not dest_dir:
        dest_dir = os.path.dirname(path)
    sample_path = os.path.join(dest_dir, name + '-sample.csv')
    num_of_lines = sum(1 for _ in open(path)) - 1
    sample_size = calc_sample_size(num_of_lines, con_level, con_interval)
    progress('Will use sample size of {} from {} total lines for {}% '
             'confidence'.format(sample_size, num_of_lines, con_level),
             newline=True)
    sample_lines = sorted(random.sample(xrange(1, num_of_lines + 1),
                                        sample_size))

    with open(sample_path, 'wb') as sc:
        with open(path, 'rb') as oc:
            # Write headers
            sc.write(oc.readline())
            progress('Wrote headers to {}'.format(sample_path), newline=True)
            line_number = 1
            lines_written = 0
            for line in oc:
                line_number += 1
                if line_number in sample_lines:
                    sc.write(line)
                    lines_written += 1

                last = progress('Read {} lines, wrote {} lines to {}'
                         .format(line_number, lines_written, sample_path),
                                last_len=last)
    progress('{} of {} lines written to sample'.format(lines_written + 1,
                                                       num_of_lines),
             newline=True, last_len=last)
    return sample_path


def calc_sample_size(num_of_lines, confidence_level, confidence_interval):
    p = 0.5
    e = float(confidence_interval) / 100
    N = num_of_lines

    # Loop through supported confidence levels and find the num std
    # deviations for that confidence level
    Z = CONFIDENCE_LEVELS.get(int(confidence_level))
    if not Z:
        raise ValueError('Confidence level must be one of these: {}'\
                         .format(CONFIDENCE_LEVELS.keys()))

    # Calc sample size
    n_0 = ((Z ** 2) * p * (1 - p)) / (e ** 2)

    # Adjust sample size for finite number of lines
    n = n_0 / (1 + ((n_0 - 1) / float(N)))

    # Return sample size
    return int(math.ceil(n))


def main(args):
    file_list = gather_files(args['PATH'])
    for path in file_list:
        create_sample(path, args['--cl'], args['--ci'])


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__)
    main(args)
