import csv
import StringIO

import datacleaner as dc
from datacleaner import data_prep, parse_row

INPUT_DATA = {
    'd': '01/1/2010',
    'fn': 'George',
    'ln': 'Washington',
    'e': 'wizard@gmail.com',
    'a1': '123 Shilling Street',
    'a2': 'Seattle',
    'a3': 'Washington',
    'a4': '12345'
}

VALID_RESULT = {
    'dob': '01/1/2010',
    'n': 'George Washington',
    'e': 'wizard@gmail.com',
    'd': 'gmail.com',
    'a': '123 Shilling Street Seattle Washington 12345'
}


def test_data_prep_misc():
    """Validate 'x' fields are removed."""
    source = INPUT_DATA.copy()
    source['x1'] = 'unknown'
    source['x'] = 'another unknown'
    assert dc.data_prep(source) == VALID_RESULT


def test_data_prep_empty():
    """Validate empty fields are removed."""
    source = INPUT_DATA.copy()
    source['blah'] = ''
    source['other'] = 'null'
    source['empty'] = 'blank'
    assert dc.data_prep(source) == VALID_RESULT


def test_data_prep_addresses():
    """Validate address fields are combined to one 'a' field."""
    source = INPUT_DATA.copy()
    assert dc.data_prep(source)['a'] == VALID_RESULT['a']


def test_data_prep_names():
    """Validate first and last name fields are combined to one 'n' field."""
    source = INPUT_DATA.copy()
    assert dc.data_prep(source)['n'] == VALID_RESULT['n']


def test_data_prep_dob():
    """Validate date of birth 'd' field is renamed to 'dob'."""
    source = INPUT_DATA.copy()
    assert dc.data_prep(source)['dob'] == VALID_RESULT['dob']


def test_data_prep_valid_email():
    """Validate domain of email address is split out to 'd' field."""
    source = INPUT_DATA.copy()
    result = dc.data_prep(source)
    assert result.get('e') == VALID_RESULT['e']
    assert result.get('d') == VALID_RESULT['d']


def test_data_prep_double_at_email():
    """Validate domain of email address with @@ is split out to 'd' field."""
    source = INPUT_DATA.copy()
    source['e'] = 'wizard@@gmail.com'
    result = dc.data_prep(source)
    assert result.get('e') == VALID_RESULT['e']
    assert result.get('d') == VALID_RESULT['d']


def test_data_prep_invalid_email():
    """Validate deletion of 'e' field when it has invalid email address."""
    source = INPUT_DATA.copy()
    source['e'] = 'wizard@gandalf@gmail.com'
    result = dc.data_prep(source)
    assert not result.get('e')


def test_data_prep_missing_email():
    """Validate handling of missing 'e' field."""
    source = INPUT_DATA.copy()
    del source['e']
    result = dc.data_prep(source)
    assert not result.get('e')


def test_parse_row_delimiter_in_column():
    """Validate handling of a quoted column with delimiter inside."""
    line = '"1250550","1266519","0","gnarlingtonlife@gmail.com","hi, yes"'
    dialect = csv.excel
    clean, fail = dc.parse_row(line, 5, dialect)
    assert not fail


def test_get_headers_double_quoted():
    """Validate handling of double quoted header columns."""
    str_buffer = StringIO.StringIO()
    header_row = '"email","first_name","last_name"\n'
    shortened_headers = ['e', 'fn', 'ln']
    str_buffer.write(header_row)
    str_buffer.seek(0)
    found_headers = dc.get_headers(str_buffer, ',', 3)
    assert found_headers == shortened_headers


def test_get_headers_single_quoted():
    """Validate handling of single quoted header columns."""
    str_buffer = StringIO.StringIO()
    header_row = "'email','first_name','last_name'\n"
    shortened_headers = ['e', 'fn', 'ln']
    str_buffer.write(header_row)
    str_buffer.seek(0)
    found_headers = dc.get_headers(str_buffer, ',', 3)
    assert found_headers == shortened_headers
