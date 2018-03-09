"""Common functions."""
import os


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
