"""Common functions."""
import os

import colored
from colored import stylize
from tqdm import tqdm


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize


def gather_files(path, skip=[], file_list=[]):
    """Gather list of files recursively."""
    if isinstance(path, list):
        for p in path:
            gather_files(p, skip, file_list)
    else:
        if os.path.isdir(path):
            if os.path.basename(path) not in skip:
                for subpath in os.listdir(path):
                    gather_files(os.path.join(path, subpath), skip, file_list)
        else:
            basename = os.path.basename(path)
            if not basename.startswith('.') and not basename.endswith('~'):
                if os.path.exists(path):
                    file_list.append(path)
                else:
                    p_failure('File {} does not exist'.format(path))
    return file_list


def move(src_path, dest_dir):
    """Moves source file into new directory.

    Creates directory if needed.
    """
    filename = os.path.basename(src_path).rstrip('~')

    if dest_dir.startswith('~'):
        dest_dir = os.path.expanduser(dest_dir)
    else:
        dest_dir = dest_dir

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    new_path = os.path.join(dest_dir, filename)
    if src_path != new_path:
        os.rename(src_path, new_path)


def print_color(fg_color, attr=None):
    def color_me(txt):
        if attr:
            style = colored.fg(fg_color) + colored.attr(attr)
        else:
            style = colored.fg(fg_color)
        print stylize(txt, style)

    return color_me


c_success = print_color('spring_green_1')
c_failure = print_color('red_1', 'bold')
c_warning = print_color('yellow_1', 'bold')
c_error = print_color('dark_orange_3b')
c_info = print_color('grey')
c_action = print_color('steel_blue_1b')
c_action_system = print_color('grey_40')
c_action_info = print_color('grey_70')
c_darkgreen = print_color('dark_turquoise')
c_lightgreen = print_color('spring_green_2a')
c_darkgray = print_color('grey_27')
c_lightgray = print_color('grey_54')
c_blue = print_color('dodger_blue_3')
c_lightblue = print_color('deep_sky_blue')
