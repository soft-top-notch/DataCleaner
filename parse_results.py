#!/usr/bin/env python2.7
"""Parse matches from search results file.

Called with filename argument for one or more search result files/dumps.  Will
output matches into named file.

Filename argument can be a list of files or wildcard (*).

Usage:
    parse_results.py [-hV] RESULT_FILES...

Options:
    -h, --help                    This help output
    -V, --version                 Print version and exit

Examples:
    parse_results.py test.txt
"""
from __future__ import division, print_function
import os
import re
from docopt import docopt
from pprint import pprint as pp

__version__ = '0.0.1'
__license__ = """
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

BLANK = re.compile('^(?:\s+)?\r?$')
FOUND = re.compile('^Found matches in (.*)\:')


def main(args):
    for results_file in args['RESULT_FILES']:
        with open(results_file) as f:
            print('Reading file ' + results_file)
            parsing = False
            matches = dict()
            db_file = None
            for line in f:
                if parsing:
                    if BLANK.search(line):
                        parsing = False
                    else:
                        matches[db_file].append(line)
                else:
                    match = FOUND.search(line)
                    if match:
                        db_file = match.group(1)
                        matches[db_file] = list()
                        parsing = True

        if not matches:
            print('No matches found in {}'.format(results_file))

        # Write out matches inside named file
        for db_file, lines in matches.items():
            dirname = os.path.dirname(db_file)
            if not os.path.exists(dirname):
                print('Creating {} directory'.format(dirname))
                os.mkdir(dirname)
            with open(db_file, 'w') as dbf:
                print('Writing to file ' + db_file)
                for line in lines:
                    dbf.write(line)


if __name__ == '__main__':
    """Executed if called from CLI directly."""
    args = docopt(__doc__, version=__version__)
    main(args)
