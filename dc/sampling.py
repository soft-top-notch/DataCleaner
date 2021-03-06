#!/usr/bin/env python
"""Create sampling of CSV(s).

Called with path argument for one or more files. Filename argument can be a \
list of files or wildcard (*).

Usage:
    sampling.py [-h] [--ci <interval>] [--cl <percent>] PATH...

Options:
    --sci <interval>                 Confidence interval [default: 2.0]
    --scl <percent>                  Confidence level (percent) [default: 95]
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
from tqdm import tqdm

from dc import gather_files, move, c_failure, c_success, c_action, c_action_info, c_action_system, c_sys_success,\
    c_warning, c_darkgray, c_darkgreen, c_lightgreen, c_lightgray, c_lightblue, c_blue

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
    name = path.rstrip('.csv')
    base_dir = os.path.dirname(path)
    sample_path = os.path.join(base_dir, name + '-sample.csv')
    num_of_lines = sum(1 for _ in open(path)) - 1
    sample_size = calc_sample_size(num_of_lines, con_level, con_interval)
    c_action('{}: Will use sample size of {} from {} total lines for {}% '
             'confidence'.format(path, sample_size, num_of_lines, con_level))
    sample_lines = sorted(
        random.sample(xrange(1, num_of_lines + 1), sample_size))

    with open(sample_path, 'wb') as sc:
        with open(path, 'rb') as oc:
            # Write headers
            sc.write(oc.readline())
            c_action_system('{}: Wrote headers to {}'.format(
                path, sample_path))
            pbar = tqdm(total=sample_size)
            line_number = 1
            lines_written = 0
            for line in oc:
                line_number += 1
                if line_number in sample_lines:
                    sc.write(line)
                    lines_written += 1
                    pbar.update(1)
    pbar.close()
    c_action_system('{}: {} of {} lines written to sample'.format(
        path, lines_written + 1, num_of_lines))
    if dest_dir:
        c_action_system('{}: Moving {} to {}'.format(path, sample_path,
                                                     dest_dir))
        move(sample_path, dest_dir)
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
    n_0 = ((Z**2) * p * (1 - p)) / (e**2)

    # Adjust sample size for finite number of lines
    n = n_0 / (1 + ((n_0 - 1) / float(N)))

    # Return sample size
    return int(math.ceil(n))


def main(args):
    file_list = gather_files(args['PATH'])
    for path in file_list:
        create_sample(path, args['--scl'], args['--sci'])


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__)
    main(args)
