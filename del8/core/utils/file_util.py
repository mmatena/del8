"""TODO: Add title."""
import pathlib


def get_file_suffix(file):
    # Pretty much get the file extension, starting with a dot.
    # Returns empty string if there is no file extension.
    return "".join(pathlib.Path(file).suffixes)
