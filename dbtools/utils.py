import re

quotes_re = re.compile(r'\'|"|\\\'|\\\"')


def pair_quotes(text, prev=None):
    for m in quotes_re.finditer(text):
        quote = m.group(0)
        if quote[0] != '\\':
            if prev:
                if prev == quote:
                    prev = None
            else:
                prev = quote
    return prev


def replace_quotes(text, quote=None):
    if text and text[0] in ('`', "'", '"') and text[-1] == text[0]:
        text = text[1:-1]
        if quote:
            text = quote + text + quote
    return text


def csv_reader(file):
    """Keeps original quotes in row."""
    for line in file:
        row = line.rstrip().split(',')
        yield _join_common_columns(row)


def _join_common_columns(row):
    result = []
    quote = None
    for value in row:
        if quote:
            result[-1] += value
            if len(value) and value[-1] == quote:
                quote = None
        else:
            result.append(value)
            if len(value) and value[0] in ('"', "'"):
                quote = value[0]
                if value[-1] == quote and (len(value) <= 2 or
                                           value[-2] != '\\'):
                    quote = None
                else:
                    result[-1] += ','
    return result
