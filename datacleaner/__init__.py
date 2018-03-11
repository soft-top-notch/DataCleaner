"""Common functions."""
import os

import colored
from colored import stylize


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


p_success = print_color('green', 'bold')
p_failure = print_color('red', 'bold')
p_warning = print_color('blue')
p_info = print_color('grey')
